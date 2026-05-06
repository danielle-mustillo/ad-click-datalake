

package com.daniellemustillo.datalake_demo;

import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import org.apache.spark.sql.types.StructField;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.*;

@Component
@Profile("!test")
public class KafkaToDataLakePipelines {

    private final SparkSession spark;
    
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrap;

    @Value("${spring.kafka.ad-click-topic}")
    private String topic;

    public KafkaToDataLakePipelines(SparkSession spark) {
        this.spark = spark;
    }

    public StreamingQuery kafkaToBronze() throws Exception {
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

        return bronze.writeStream()
            .format("parquet")
            .option("path", "data/bronze/clicks")
            .option("checkpointLocation", "data/checkpoints/kafka-to-bronze-clicks")
            .outputMode("append")
            .start();

    }
    

    public StreamingQuery kafkaToSilver() throws Exception {
        spark.sql("""
                    CREATE NAMESPACE IF NOT EXISTS local.lake
                """);

        spark.sql("""
                    CREATE TABLE IF NOT EXISTS local.lake.silver_clicks (
                        eventId STRING,
                        userId STRING,
                        adId STRING,
                        campaignId STRING,
                        country STRING,
                        device STRING,
                        eventTime STRING,
                        cost STRING,
                        clickTS TIMESTAMP,
                        clickDate DATE,
                        revenue DOUBLE 
                    )
                    USING iceberg
                    PARTITIONED BY (clickDate)
                """);

        var schema = createStructType(new StructField[] {
            createStructField("eventId", StringType, false),
            createStructField("userId", StringType, true),
            createStructField("adId", StringType, true),
            createStructField("campaignId", StringType, false),
            createStructField("country", StringType, false),
            createStructField("device", StringType, true),
            createStructField("eventTime", StringType, false),
            createStructField("cost", StringType, true)
        });
        
        // Apache Iceberg
        Dataset<Row> kafka = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrap)
                .option("subscribe", topic)
                .option("startingOffsets", "earliest")
                .load();

        // Apache Parquet
        // Dataset<Row> parsed = kafka
        //         .selectExpr("CAST(value AS STRING) as raw_json");

        Dataset<Row> parsed = kafka.selectExpr("CAST(value AS STRING) as raw_json")
            .select(from_json(col("raw_json"), schema).alias("event"))
            .select("event.*")
            .withColumn("clickTS", to_timestamp(col("eventTime")))
            .withColumn("clickDate", to_date(col("eventTime")))
            .withColumn("revenue", col("cost").cast(DoubleType))
            .filter(col("eventId").isNotNull())
            .filter(col("campaignId").isNotNull())
            .filter(col("clickTS").isNotNull())
            .filter(col("clickDate").isNotNull())
            .filter(col("revenue").isNotNull());

        // return parsed.writeStream()
        //         .format("console")
        //         .option("truncate", "false")
        //         .start();
        

        // Apache Parquet
        // return parsed.writeStream()
        //     .format("parquet")
        //     .trigger(Trigger.ProcessingTime("10 seconds")) // Silver is semi-batched
        //     .option("path", "data/silver/clicks")
        //     .option("checkpointLocation", "data/checkpoints/kafka-to-silver-clicks")
        //     .partitionBy("clickDate")
        //     .outputMode("append")
        //     .start();
        //
        // Apache Iceberg (schema)
        return parsed.writeStream()
            .queryName("kafka-to-silver-clicks")
            .trigger(Trigger.ProcessingTime("15 seconds"))
            .format("iceberg")
            .outputMode("append")
            .option("checkpointLocation", "data/checkpoints/kafka-to-silver-clicks-iceberg")
            .toTable("local.lake.silver_clicks");
        
    }
    public void silverToGold() throws Exception {
        // Apache Parquet
        // Dataset<Row> clicks = spark.read()
        //     .schema("eventId STRING, userId STRING, adId STRING, campaignId STRING, country STRING, device STRING, eventTime STRING, cost STRING, clickTS TIMESTAMP, clickDate DATE, revenue DOUBLE")
        //     .parquet("data/silver/clicks");
        //
        //

        // Apache Iceberg
        Dataset<Row> clicks = spark.table("local.lake.silver_clicks");
        spark.sql("""
                    CREATE TABLE IF NOT EXISTS local.lake.gold_campaign_five_minute_clicks (
                        campaignId STRING,
                        windowStart TIMESTAMP,
                        windowEnd TIMESTAMP,
                        clickCount BIGINT,
                        totalRevenue DOUBLE
                    )
                    USING iceberg
                """);


        System.out.println("read in silver:: ");
        clicks.show();

        // Dataset<Row> parsed = clicks
        // .selectExpr("CAST(value AS STRING) as raw_json");
        //
        // return parsed.write()
        //         .format("console")
        //         .option("truncate", "false");
        Dataset<Row> gold = clicks
            .groupBy(
                    col("campaignId"), 
                    window(col("clickTS"), "5 minutes")
            )
            .agg(
                count("*").alias("clickCount"),
                sum("revenue").alias("totalRevenue")
            )
            .select(
                col("campaignId"),
                col("window.start").alias("windowStart"),
                col("window.end").alias("windowEnd"),
                col("clickCount"),
                col("totalRevenue")
        );
        System.out.println("output in gold:: ");
        gold.show();

        // Apache Parquet
        // gold.write()
        //     .mode("overwrite")
        //     .option("checkpointLocation", "data/checkpoints/silver-to-gold-campaign-batch")
        //     .parquet("data/gold/campaign_clicks_five_mins");

        // Apache Iceberg
        gold.writeTo("local.lake.gold_campaign_five_minute_clicks")
            .overwritePartitions(); 


            //.outputMode("append")
            //.start();

    }

    public void showGold() {
        // Dataset<Row> clicks = spark.read()
        //     .parquet("data/gold/campaign_clicks_five_mins");

        Dataset<Row> clicks = spark.table("local.lake.gold_campaign_five_minute_clicks");
        clicks.orderBy(col("windowStart").desc()).show();
    }
    public void showSilver() {
        // Dataset<Row> clicks = spark.read()
        //     .parquet("data/silver/clicks");
        
        Dataset<Row> clicks = spark.table("local.lake.silver_clicks");
        clicks.orderBy(col("clickTS").desc()).show();
    }
    public void showBronze() {
        Dataset<Row> clicks = spark.read()
            .parquet("data/bronze/clicks");
        clicks.orderBy(col("offset").desc()).show();
    }
}
