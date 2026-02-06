package com.societegenerale.volcker.service.batch;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.when;

import com.societegenerale.volcker.ulti.SparkTemplate;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.springframework.stereotype.Service;


@Service("fifoInventory")
public class FifoInventory extends SparkTemplate {

    @Override
    public void job() throws Exception {

    SparkSession spark = SparkSession.builder()
        .appName("FIFO Inventory Example")
        .master("local[*]")
        .getOrCreate();

    spark.sparkContext().setLogLevel("ERROR");

    // ---------------------------
    // 1. 构造示例数据
    // ---------------------------
    List<Row> data = Arrays.asList(
        RowFactory.create("D1", "C1", "BUY", 20, 100.0, java.sql.Timestamp.valueOf("2026-02-01 00:00:00")),
        RowFactory.create("D1", "C1", "BUY", 100, 100.0, java.sql.Timestamp.valueOf("2026-02-03 00:00:00")),
        RowFactory.create("D1", "C1", "BUY", 50, 101.0, java.sql.Timestamp.valueOf("2026-02-04 00:00:00")),
        RowFactory.create("D1", "C1", "SELL", 80, 102.0, java.sql.Timestamp.valueOf("2026-02-05 00:00:00")),
        //
        RowFactory.create("D2", "C2", "BUY", 200, 99.5, java.sql.Timestamp.valueOf("2026-02-01 10:00:00")),
        RowFactory.create("D2", "C2", "SELL", 50, 100.5, java.sql.Timestamp.valueOf("2026-02-02 11:00:00")),
        RowFactory.create("D2", "C2", "BUY", 70, 101.0, java.sql.Timestamp.valueOf("2026-02-03 12:00:00")),
        RowFactory.create("D2", "C2", "SELL", 100, 102.0, java.sql.Timestamp.valueOf("2026-02-05 13:00:00"))
    );

    StructType schema = new StructType()
        .add("desk", DataTypes.StringType)
        .add("cusip", DataTypes.StringType)
        .add("side", DataTypes.StringType)
        .add("qty", DataTypes.IntegerType)
        .add("price", DataTypes.DoubleType)
        .add("trade_time", DataTypes.TimestampType);

    Dataset<Row> rawTrades = spark.createDataFrame(data, schema);

    rawTrades.show(false);

    // ---------------------------
    // 2. 定义窗口
    // ---------------------------

    // 总净持仓窗口
    WindowSpec totalWindow = Window.partitionBy("desk", "cusip");

    // 买单累计窗口 (倒序)
    WindowSpec runningSumWindow = Window.partitionBy("desk", "cusip")
        .orderBy(col("trade_time").desc());

    var buyTrades = rawTrades
        .withColumn("signedQty",
            when(col("side").equalTo("BUY"), col("qty"))
                .otherwise(col("qty").multiply(-1)))
        .withColumn("total", sum(col("signedQty")).over(totalWindow))
        .filter(col("side").equalTo("BUY"))
        .withColumn("acc", sum(col("signedQty")).over(runningSumWindow))
        .withColumn("pre", col("acc").minus(col("qty")));
    buyTrades.show();

    var finalInventory = buyTrades
        .withColumn("alc",
            when(col("total").geq(col("acc")), col("qty"))
        .otherwise(col("total").minus(col("pre"))) // 部分库存
        );

    // ---------------------------
    // 5. 输出
    // ---------------------------
    finalInventory.show(false);
  }
}
