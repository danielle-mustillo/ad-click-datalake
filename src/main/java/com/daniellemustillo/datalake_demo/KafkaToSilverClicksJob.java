package com.daniellemustillo.datalake_demo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.StructField;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;


import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.*;


@Component
public class KafkaToSilverClicksJob {
    private final SparkSession spark;
    
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrap;

    @Value("${spring.kafka.ad-click-topic}")
    private String topic;

    public KafkaToSilverClicksJob(SparkSession spark) {
        this.spark = spark;
    }

    public void run() throws Exception {
        var schema = createStructType(new StructField[]{
            createStructField("eventId", StringType, false),
            createStructField("userId", StringType, true),
            createStructField("adId", StringType, true),
            createStructField("campaignId", StringType, false),
            createStructField("country", StringType, false),
            createStructField("device", StringType, true),
            createStructField("eventTime", StringType, false),
            createStructField("cost", StringType, true)
        });

        Dataset<Row> kafka = spark.readStream()
            .format("kafka")
            .option("kafka.bootstrap.servers", bootstrap)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .load();

        Dataset<Row> parsed = kafka.selectExpr("CAST(value AS STRING) as raw_json")
            .select(from_json(col("raw_json"), schema).alias("event"))
            .select("event.*")
            .withColumn("clickTS", to_timestamp(col("eventTime")))
            .withColumn("clickDate", to_date(col("eventTime")))
            .withColumn("revenue", to_number(col("cost"), col("9.99")))
            .filter(col("eventId").isNotNull())
            .filter(col("campaignId").isNotNull())
            .filter(col("clickTS").isNotNull())
            .filter(col("clickDate").isNotNull())
            .filter(col("revenue").isNotNull());

        StreamingQuery query = parsed.writeStream()
            .format("parquet")
            .option("path", "data/silver/clicks")
            .option("checkpointLocation", "data/checkpoints/kafka-to-silver-clicks")
            .partitionBy("event_date")
            .outputMode("append")
            .start();
        
        query.awaitTermination();
    }
    
}
