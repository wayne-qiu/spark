package com.societegenerale.volcker.service.stream;

import static org.apache.spark.sql.functions.*;

import com.societegenerale.volcker.ulti.SparkTemplate;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.springframework.stereotype.Service;


@Service("streamWindow")
public class StreamWindowProcessor extends SparkTemplate {

  @Override
  public void job() throws Exception {
    System.out.println(">>> [Service] Starting Spark Job...");

    StructType tradeSchema = new StructType()
        .add("tradeId", DataTypes.StringType)
        .add("tradeTime", DataTypes.StringType)
        .add("traderId", DataTypes.StringType)
        .add("instruments", DataTypes.StringType)
        .add("side", DataTypes.StringType)
        .add("quantity", DataTypes.createDecimalType(18, 2)) // ✅ 正确
        .add("price", DataTypes.createDecimalType(18, 2));

    // 1️⃣ 读取 CSV 模拟 streaming
    Dataset<Row> rawStream = spark.readStream()
        .schema(tradeSchema)
        .option("header", "true")
        .csv("input/stream"); // 文件夹路径，Spark 会监控新增文件

    // 2️⃣ 拆分 instruments 列
    Dataset<Row> exploded = rawStream
        .withColumn("instrumentId", explode(split(col("instruments"), "\\|")))
        .withColumn("quantity", col("quantity").cast("decimal(18,2)"))
        .withColumn("price", col("price").cast("decimal(18,2)"))
        .withColumn("notional", expr("quantity * price"))
        .drop("instruments");

    // 3️⃣ 转 timestamp
    Dataset<Row> trades = exploded
        .withColumn("tradeTs", to_timestamp(col("tradeTime"), "yyyy-MM-dd HH:mm:ss"));

    // 4️⃣ 按 window 统计 rolling notional per trader
    Dataset<Row> windowAgg = trades
        .withWatermark("tradeTs", "1 hour")
        .groupBy(
            col("traderId"),
            window(col("tradeTs"), "1 hour")  // 1 小时窗口
        )
        .agg(
            sum("notional").alias("totalNotional"),
            count("*").alias("tradeCount")
        )
        .select(
            col("traderId"),
            col("window.start").alias("windowStart"),
            col("window.end").alias("windowEnd"),
            col("totalNotional"),
            col("tradeCount")
        );

    // 5️⃣ 输出到控制台
    StreamingQuery query = windowAgg.writeStream()
        .outputMode("update")   // 仅输出更新结果
        .format("console")
        .option("truncate", "false")
        .start();

    query.awaitTermination();
  }

}
