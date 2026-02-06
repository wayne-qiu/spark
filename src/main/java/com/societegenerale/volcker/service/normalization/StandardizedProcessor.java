package com.societegenerale.volcker.service.normalization;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.split;
import static org.apache.spark.sql.functions.when;

import com.societegenerale.volcker.ulti.SparkTemplate;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.springframework.stereotype.Service;

@Service("standardized")
public class StandardizedProcessor extends SparkTemplate {

  @Override
  public void job() throws Exception {

    // Read CSV into a DataFrame
    Dataset<Row> df = spark.read()
        .option("header", "true") // set to true if CSV has header
        .option("inferSchema", "true") // automatically infer types
        .csv("input/raw_trades.csv");

    // 2. 标准化列名 & 数据
    Dataset<Row> normalizedDf = df
        .withColumnRenamed("id", "tradeId")
        .withColumnRenamed("ts", "tradeTime")
        .withColumnRenamed("usr", "traderId")
        .withColumnRenamed("instr", "instruments")
        .withColumn("instrumentId", explode(split(col("instruments"), "\\|")))
        .withColumnRenamed("type", "instrumentType")
        .withColumn("side", when(col("side").equalTo("B"), "BUY")
            .when(col("side").equalTo("S"), "SELL")
            .otherwise("UNKNOWN"))
        .withColumn("quantity", col("qty").cast(DataTypes.createDecimalType(18,2)))
        .withColumn("price", col("px").cast(DataTypes.createDecimalType(18,2)))
        .withColumn("notional", expr("quantity * price"))
        .drop("qty")
        .drop("px");

    // 4. Perform Transformations/Actions
    long count = normalizedDf.count();
    System.out.println(">>> [Service] DataFrame Content:");

    normalizedDf.show(false);

    System.out.println(">>> [Service] Total items processed: " + count);
  }
}
