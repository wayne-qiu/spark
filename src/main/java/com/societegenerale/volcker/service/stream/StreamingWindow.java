package com.societegenerale.volcker.service.stream;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.window;

import com.societegenerale.volcker.ulti.SparkTemplate;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.springframework.stereotype.Service;


@Service("streamingWindow")
public class StreamingWindow extends SparkTemplate {

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
            java.sql.Timestamp.valueOf("2026-02-05 00:00:00")),

        // 新增 D2, C2 数据
        RowFactory.create("D2", "C2", "BUY", 200, 99.5,
            java.sql.Timestamp.valueOf("2026-02-01 10:00:00")),
        RowFactory.create("D2", "C2", "SELL", 50, 100.5,
            java.sql.Timestamp.valueOf("2026-02-02 11:00:00"))
    );

    StructType schema = new StructType()
        .add("deskId", DataTypes.StringType)
        .add("cusip", DataTypes.StringType)
        .add("side", DataTypes.StringType)
        .add("qty", DataTypes.IntegerType)
        .add("price", DataTypes.DoubleType)
        .add("tradeTime", DataTypes.TimestampType);

    Dataset<Row> df = spark.createDataFrame(data, schema);

    // =========================
    // 将 batch 转成 streaming 模拟输入
    // =========================
    Dataset<Row> streamingInput = df
        .withColumn("tradeTime", col("tradeTime").cast("timestamp"));

    // 模拟 structured streaming: 用 rate source 生成流 + join batch 数据
    Dataset<Row> stream = spark.readStream()
        .schema(schema)
        .option("maxFilesPerTrigger", 1) // 模拟微批
        .format("memory") // 注意：实际可替换为 Kafka / socket
        .load();

    // 这里为了 demo，直接用 batch df 做 streaming window 聚合
    Dataset<Row> result = df
        .withWatermark("tradeTime", "1 day") // watermark 防止状态无限增长
        .groupBy(
            col("deskId"),
            window(col("tradeTime"), "1 day")
        )
        .agg(
            sum("qty").alias("totalQty"),
            sum(expr("qty * price")).alias("notional")
        )
        .orderBy("window");

    // 输出控制台
    result.show(false);
  }
}
