package com.daniellemustillo.datalake_demo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import static org.apache.spark.sql.functions.*;

@Component
@Profile("!test")
public class KafkaToBronzeClicksJob {
    
    private final SparkSession spark;
    
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrap;

    @Value("${spring.kafka.ad-click-topic}")
    private String topic;

    public KafkaToBronzeClicksJob(SparkSession spark) {
        this.spark = spark;
    }

    public void run() throws Exception {
        Dataset<Row> kafka = spark.readStream()
            .format("kafka")
            .option("kafka.bootstrap.servers", bootstrap)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .load();

        Dataset<Row> bronze = kafka.select(
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_timestamp"),
            col("key").cast("string").alias("kafka_key"),
            col("value").cast("string").alias("raw_json")
        );

        StreamingQuery query = bronze.writeStream()
            .format("parquet")
            .option("path", "data/bronze/clicks")
            .option("checkpointLoication", "data/checkpoints/kafka-to-bronze-clicks")
            .outputMode("append")
            .start();

        query.awaitTermination();
    }


}
