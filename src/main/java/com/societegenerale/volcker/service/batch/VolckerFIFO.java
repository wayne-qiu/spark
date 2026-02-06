package com.societegenerale.volcker.service.batch;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.current_date;
import static org.apache.spark.sql.functions.datediff;
import static org.apache.spark.sql.functions.lag;
import static org.apache.spark.sql.functions.least;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.when;

import com.societegenerale.volcker.ulti.SparkTemplate;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.springframework.stereotype.Service;

@Service("volckerFIFO")
public class VolckerFIFO extends SparkTemplate {

  @Override
  public void job() throws Exception {

    List<Row> data = Arrays.asList(
        RowFactory.create("D1", "C1", "BUY", 20, 100.0,
            java.sql.Timestamp.valueOf("2026-02-01 00:00:00")),
        RowFactory.create("D1", "C1", "BUY", 100, 100.0,
            java.sql.Timestamp.valueOf("2026-02-03 00:00:00")),
        RowFactory.create("D1", "C1", "BUY", 50, 101.0,
            java.sql.Timestamp.valueOf("2026-02-04 00:00:00")),
        RowFactory.create("D1", "C1", "SELL", 80, 102.0,
            java.sql.Timestamp.valueOf("2026-02-05 00:00:00"))
    );

      // 1. 定义 Schema 并加载原始流水数据
      // 对应您提供的: D1(Desk), C1(Cusip), Side, Qty, Price, Time
      StructType schema = new StructType()
          .add("desk", DataTypes.StringType)
          .add("cusip", DataTypes.StringType)
          .add("side", DataTypes.StringType)
          .add("qty", DataTypes.IntegerType)
          .add("price", DataTypes.DoubleType)
          .add("trade_time", DataTypes.TimestampType);

      Dataset<Row> rawTrades = spark.createDataFrame(data, schema);

      // 2. 计算当前净持仓 (Net Position Calculation)
      // 逻辑：BUY为正，SELL为负。按 Desk, Cusip 分组求和。
      Dataset<Row> netPositionDf = rawTrades
          .groupBy("desk", "cusip")
          .agg(sum(
              when(col("side").equalTo("BUY"), col("qty"))
                  .otherwise(col("qty").multiply(lit(-1))))
              .as("net_position_qty"));

      // 打印中间状态：C1 的净持仓应该是 90
//      netPositionDf.show();

      // 3. 准备买入池 (Supply Pool)
      // FIFO 逻辑下，只有 BUY 单会构成库存。
      Dataset<Row> buyTrades = rawTrades.filter(col("side").equalTo("BUY"));
      buyTrades.show();

      // 4. 核心 FIFO 匹配逻辑 (Inventory Aging)

      // A. 关联：把"总目标持仓量"贴到每一笔 BUY 单上
      Dataset<Row> joinedDf = buyTrades.join(netPositionDf, new String[]{"desk", "cusip"});
      joinedDf.show();

      // B. 窗口：按时间倒序 (Newest First) -> 既然卖出的是旧的，留下的就是新的
      WindowSpec windowSpec = Window.partitionBy("desk", "cusip").orderBy(col("trade_time").desc());

      // C. 计算与剪裁
      Dataset<Row> resultDf = joinedDf
          // 仅处理多头持仓情况 (net_position_qty > 0)
          .filter(col("net_position_qty").gt(0))
          // 计算当前行及之前的累加买入量
          .withColumn("running_buy_qty", sum("qty").over(windowSpec))
          // 计算上一行的累加量 (用于判断是否溢出)
          .withColumn("prev_running_qty", lag("running_buy_qty", 1, 0).over(windowSpec))
          // 核心过滤：只要上一行的累加还没填满净持仓，这一行就还有用
          .filter(col("prev_running_qty").lt(col("net_position_qty")))
          // 核心计算：本笔交易分配量 = Min(本笔量, 剩余缺口)
          .withColumn("allocated_qty",
              least(col("qty"), col("net_position_qty").minus(col("prev_running_qty")))
          )
          // 计算账龄 (以当前时间或 Batch Date 为准，这里用 current_date)
          .withColumn("age_days", datediff(current_date(), col("trade_time")));
//          .sortWithinPartitions(col("trade_time").asc());
//          .select("desk", "cusip", "trade_time", "side", "qty", "allocated_qty", "age_days", "price");

      // 5. 结果展示
      System.out.println("=== FIFO Inventory Aging Result ===");
      System.out.println("Total Net Position: 90");
      resultDf.show();
  }
}
