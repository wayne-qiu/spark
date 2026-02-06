package com.societegenerale.volcker.service.deskstate;

import static org.apache.spark.sql.functions.broadcast;

import com.societegenerale.volcker.entity.DeskExposure;
import com.societegenerale.volcker.entity.DeskState;
import com.societegenerale.volcker.ulti.SparkTemplate;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.GroupStateTimeout;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.springframework.stereotype.Service;

@Service("mapGroups")
public class MapGroupsProcessor extends SparkTemplate {

  @Override
  public void job() throws Exception {

    // Trader → Desk mapping
    Dataset<Row> traderDesk = spark.read()
        .option("header", true)
        .csv("input/trader_desk.csv")
        .cache();

    traderDesk.count();

    // Trade schema
    StructType tradeSchema = new StructType()
        .add("tradeId", DataTypes.StringType)
        .add("tradeTime", DataTypes.TimestampType)
        .add("traderId", DataTypes.StringType)
        .add("instrumentId", DataTypes.StringType)
        .add("instrumentType", DataTypes.StringType)
        .add("side", DataTypes.StringType)
        .add("quantity", DataTypes.createDecimalType(18, 2))
        .add("price", DataTypes.createDecimalType(18, 2))
        .add("notional", DataTypes.createDecimalType(18, 2));

    // Streaming ingestion
    Dataset<Row> tradeStream = spark.readStream()
        .schema(tradeSchema)
        .option("header", true)
        .csv("input/mapgroups"); // 文件夹路径

    // Enrich with deskId
    Dataset<Row> enriched = tradeStream.join(
        broadcast(traderDesk),
        tradeStream.col("traderId").equalTo(traderDesk.col("traderId")),
        "left"
    ).drop(traderDesk.col("traderId"));
    
    // Add watermark for event-time processing
    Dataset<Row> withWatermark = enriched.withWatermark("tradeTime", "10 minutes");

    // Group by deskId and apply stateful aggregation
    KeyValueGroupedDataset<String, Row> grouped =
        withWatermark.groupByKey(
            (MapFunction<Row, String>) r -> r.getAs("deskId"),
            Encoders.STRING()
        );
    // col("deskId") 返回 Column
    // df.groupBy(col("deskId")).count() //Dataset
    // 从 Row 切回 Trade
    // 🚩strong type
    // .as(Encoders.bean(TradeWithDesk.class)) // 必须 Serializable，必须无参构造
    // .groupByKey(t -> t.getDeskId(), Encoders.STRING())

    grouped.mapGroupsWithState(
        (desk, trade, state) -> {

          return null;
        },
        Encoders.bean(DeskState.class),      // 状态（State）的编码器
        Encoders.bean(DeskExposure.class),   // 输出结果的编码器
        GroupStateTimeout.EventTimeTimeout() // 状态超时策略 - 基于事件时间
    );
    Dataset<DeskExposure> deskExposure = grouped.mapGroupsWithState(
        new DeskExposureStateRowFn(),        // 状态更新逻辑的核心实现类
        Encoders.bean(DeskState.class),      // 状态（State）的编码器
        Encoders.bean(DeskExposure.class),   // 输出结果的编码器
        GroupStateTimeout.EventTimeTimeout() // 状态超时策略 - 基于事件时间
    );
//    deskExposure.show();

    // Output to console
    deskExposure.writeStream()
        .outputMode("update")
        .format("console")
        .option("truncate", false)
        .start()
        .awaitTermination();
  }
}
