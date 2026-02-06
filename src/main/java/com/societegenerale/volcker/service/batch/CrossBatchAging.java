package com.societegenerale.volcker.service.batch;

import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.datediff;
import static org.apache.spark.sql.functions.lag;
import static org.apache.spark.sql.functions.least;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.sum;

import com.societegenerale.volcker.ulti.SparkTemplate;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.springframework.stereotype.Service;

@Service("crossBatchAging")
public class CrossBatchAging extends SparkTemplate {

    @Override
    public void job() throws Exception {
// ==========================================
      // 1. 加载数据 (Input Loading)
      // ==========================================

      // [输入 A]: 昨日(T-1) 算好的库存快照 (Inventory State)
      // 注意：这里的 allocated_qty 是昨天剩下的
      List<Row> prevInvData = Arrays.asList(
          // 来自很久以前的交易，剩下 20 个
          RowFactory.create("D1", "C1", "2023-01-01", 20, 100.0),
          // 来自昨天的交易，剩下 50 个
          RowFactory.create("D1", "C1", "2026-02-07", 50, 101.0)
      );
      Dataset<Row> prevSnapshotDf = spark.createDataFrame(prevInvData, schema("desk", "cusip", "orig_trade_date", "qty", "price"));

      // [输入 B]: 今日(T) 的交易增量 (Incremental Trades)
      List<Row> todayTradeData = Arrays.asList(
          // 今日买入：形成新库存
          RowFactory.create("D1", "C1", "BUY", 200, 102.0),
          // 今日卖出：消耗总库存 (总量 20+50+200=270, 卖出80, 剩190)
          RowFactory.create("D1", "C1", "SELL", 80, 105.0)
      );
      // 简单 Schema: desk, cusip, side, qty, price
      Dataset<Row> todayTradesDf = spark.createDataFrame(todayTradeData, tradeSchema());

      // ==========================================
      // 2. 预处理 (Prep)
      // ==========================================

      // A. 汇总今日卖出量 (Net Sells per Desk/Cusip)
      Dataset<Row> todaySells = todayTradesDf
          .filter(col("side").equalTo("SELL"))
          .groupBy("desk", "cusip")
          .agg(sum("qty").as("total_sell_qty"));

      // B. 整理今日买入 (New Supply) -> 格式对齐 prevSnapshotDf
      // orig_trade_date 设为今日 (e.g., 2026-02-08)
      Dataset<Row> todayBuysFormatted = todayTradesDf
          .filter(col("side").equalTo("BUY"))
          .withColumn("orig_trade_date", lit("2026-02-08")) // 实际应用中取 current_date()
          .select("desk", "cusip", "orig_trade_date", "qty", "price");

      // C. 构建总供给池 (Supply Pool) = 昨日留存 U 今日买入
      // 使用 unionByName 确保字段对应
      Dataset<Row> supplyPool = prevSnapshotDf.unionByName(todayBuysFormatted);

      // ==========================================
      // 3. 核心计算 (Core Logic)
      // ==========================================

      // 逻辑：计算总供给 - 今日卖出 = 今日目标持仓
      // 然后对供给池应用 FIFO (保留最新的)

      // 关联卖出量，如果没卖出则补0
      Dataset<Row> calcDf = supplyPool
          .join(todaySells, new String[]{"desk", "cusip"}, "left_outer")
          .withColumn("total_sell_qty", coalesce(col("total_sell_qty"), lit(0)));

      // 定义窗口：按原始交易日期倒序 (最新的在上面)
      WindowSpec windowSpec = Window.partitionBy("desk", "cusip").orderBy(col("orig_trade_date").desc());

      Dataset<Row> resultDf = calcDf
          // 1. 计算该 Product 的总供给量 (Pool Size)
          .withColumn("pool_total_qty", sum("qty").over(Window.partitionBy("desk", "cusip")))

          // 2. 计算今日应保留的总持仓 (Target End Position)
          .withColumn("target_end_qty", col("pool_total_qty").minus(col("total_sell_qty")))

          // 3. FIFO 核心: 计算累加供给
          .withColumn("running_qty", sum("qty").over(windowSpec))
          .withColumn("prev_running_qty", lag("running_qty", 1, 0).over(windowSpec))

          // 4. 剪裁 (Clipping):
          // 我们只需要保留 "最新" 的 target_end_qty 那么多
          .filter(col("prev_running_qty").lt(col("target_end_qty"))) // 还没填满目标持仓的行保留
          .withColumn("allocated_qty",
              least(col("qty"), col("target_end_qty").minus(col("prev_running_qty")))
          )

          // 5. 更新账龄: 基于原始交易日 和 当前业务日期
          .withColumn("current_age_days", datediff(lit("2026-02-08"), col("orig_trade_date")))

          .select("desk", "cusip", "orig_trade_date", "allocated_qty", "current_age_days", "price");

      // ==========================================
      // 4. 结果存盘 (Save State)
      // ==========================================
      System.out.println("=== Day T Inventory Snapshot (Rolling Result) ===");
      // 这个 DataFrame 将会被保存为 Parquet/Delta，作为明天的 "prevSnapshotDf"
      resultDf.show();

        /* 预期逻辑验证:
           总供给: 20(Old) + 50(Yesterday) + 200(Today) = 270
           卖出: 80
           目标持仓: 190

           FIFO 保留逻辑 (倒序匹配):
           1. 2026-02-08 (Today) : 有200. 需要190. -> 全部满足. (保留 190, 丢弃 10)
           2. 2026-02-07 (Yest)  : 之前的已经够了 -> 丢弃.
           3. 2023-01-01 (Old)   : 之前的已经够了 -> 丢弃.

           这里有一个特例：因为今天买的太多(200)，直接把昨天和以前的库存全部挤出去了(FIFO逻辑: 卖出消耗旧的)。
           如果卖出只有 10 个?
           目标 260.
           1. Today (200) -> Keep 200. Gap 60.
           2. Yest (50)   -> Keep 50.  Gap 10.
           3. Old (20)    -> Keep 10.  Gap 0.
        */
    }

  // 辅助方法：构建 Schema
  private static org.apache.spark.sql.types.StructType schema(String... names) {
    org.apache.spark.sql.types.StructType st = new org.apache.spark.sql.types.StructType();
    for(String n : names) {
      if(n.equals("qty")) st = st.add(n, org.apache.spark.sql.types.DataTypes.IntegerType);
      else if(n.equals("price")) st = st.add(n, org.apache.spark.sql.types.DataTypes.DoubleType);
      else st = st.add(n, org.apache.spark.sql.types.DataTypes.StringType);
    }
    return st;
  }
  private static org.apache.spark.sql.types.StructType tradeSchema() {
    return schema("desk", "cusip", "side", "qty", "price");
  }
}
