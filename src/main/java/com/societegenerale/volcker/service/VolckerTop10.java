package com.societegenerale.volcker.service;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;

import com.societegenerale.volcker.entity.DeskExposure;
import com.societegenerale.volcker.entity.DeskState;
import com.societegenerale.volcker.entity.Trade;
import com.societegenerale.volcker.service.deskstate.DeskExposureStateObjectFn;
import com.societegenerale.volcker.ulti.SparkTemplate;
import java.util.List;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.streaming.GroupStateTimeout;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.springframework.stereotype.Service;

@Service("volckerTop10")
public class VolckerTop10 extends SparkTemplate {

  @Override
  public void job() throws Exception {

    List<Row> tradeData = List.of(
        RowFactory.create("T1","D1","CUSIP1","BUY",  100.0,  99.0, "2026-01-01 10:00:00", "TRADER1", "INST1", "EQUITY"),
        RowFactory.create("T2","D1","CUSIP1","SELL", 50.0, 101.0, "2026-01-01 10:01:00", "TRADER1", "INST1", "EQUITY"),
        RowFactory.create("T3","D1","CUSIP2","BUY",  200.0,  50.0, "2026-01-01 10:02:00", "TRADER2", "INST2", "EQUITY"),
        RowFactory.create("T4","D2","CUSIP1","BUY",  300.0, 100.0, "2026-01-01 10:03:00", "TRADER3", "INST1", "EQUITY"),
        RowFactory.create("T5","D2","CUSIP1","SELL", 100.0, 102.0, "2026-01-01 10:04:00", "TRADER3", "INST1", "EQUITY")
    );

    StructType schema = new StructType()
        .add("tradeId", DataTypes.StringType)
        .add("deskId", DataTypes.StringType)
        .add("cusip", DataTypes.StringType)
        .add("side", DataTypes.StringType)
        .add("quantity", DataTypes.DoubleType)
        .add("price", DataTypes.DoubleType)
        .add("tradeTime", DataTypes.StringType)
        .add("traderId", DataTypes.StringType)
        .add("instrumentId", DataTypes.StringType)
        .add("instrumentType", DataTypes.StringType);

    Dataset<Row> trades = spark.createDataFrame(tradeData, schema)
        .withColumn("quantity", col("quantity").cast(DataTypes.createDecimalType(18, 2)))
        .withColumn("price", col("price").cast(DataTypes.createDecimalType(18, 2)))
        .withColumn("tradeTime", col("tradeTime").cast(DataTypes.TimestampType));

    // coding
    //先实现 1–3（纯 groupBy）
    // 1️⃣ Desk Net Position
    Dataset<Trade> enriched = trades
        .withColumn("notional", col("quantity").multiply(col("price")))
        .withColumn("directionalNotional",
            when(col("side").equalTo("BUY"), col("notional"))
                .otherwise(col("notional").multiply(lit(-1))))
        .as(Encoders.bean(Trade.class));

    KeyValueGroupedDataset<String, Trade> grouped =
        enriched.groupByKey(
            (MapFunction<Trade, String>) Trade::getDeskId,
            Encoders.STRING()
        );

    Dataset<DeskExposure> result =
        grouped.mapGroupsWithState(
            new DeskExposureStateObjectFn(),
            Encoders.bean(DeskState.class),
            Encoders.bean(DeskExposure.class),
            GroupStateTimeout.NoTimeout()       // 状态超时策略
        );

    result.show(false);

    // Output to console
//    result.writeStream()
//        .outputMode("update")
//        .format("console")
//        .option("truncate", false)
//        .start()
//        .awaitTermination();

    //2️⃣ Desk Gross Exposure; CUSIP 级别风险暴露累计
    KeyValueGroupedDataset<String, Trade> groupByDeskId =
        enriched.groupByKey(
            (MapFunction<Trade, String>) Trade::getDeskId,
            Encoders.STRING()
        );

    //3️⃣ 1-minute Rolling Volume
    //4️⃣ Detect Directional Bias (Volcker Red Flag)
    //5️⃣ Inventory Aging
    //6️⃣ PnL Calculation (Mark-to-Market)
    //7️⃣ Concentration Risk
    //8️⃣ New Product Detection (RENTD N)
    //9️⃣ Turnover Ratio
    //🔟 Stream-State Question

    //再做 4–5（window + state）

    //再做 6–7（join + aggregation）

  }
}
