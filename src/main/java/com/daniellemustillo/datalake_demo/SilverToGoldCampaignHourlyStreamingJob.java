
package com.daniellemustillo.datalake_demo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import static org.apache.spark.sql.functions.*;

@Component
@Profile("!test")
public class SilverToGoldCampaignHourlyStreamingJob {

    private final SparkSession spark;

    public SilverToGoldCampaignHourlyStreamingJob(SparkSession spark) {
        this.spark = spark;
    }

    public void run() throws Exception {
        Dataset<Row> clicks = spark.readStream()
            .schema("eventId STRING, userId STRING, adId STRING, campaignId STRING, country STRING, device STRING, eventTime STRING, cost STRING, clickTS TIMESTAMP, clickDate DATE, revenue DOUBLE")
            .parquet("data/silver/clicks");

        Dataset<Row> gold = clicks
            .withWatermark("clickTS", "10 minutes")
            .groupBy(
            col("campaignId"), 
                    window(col("clickTS"), "1 hour")
                )
            .agg(
                count("*").alias("click_count"),
                sum("revenue").alias("total_revenue")
        );

        StreamingQuery query = gold.writeStream()
            .format("parquet")
            .option("path", "data/gold/campaign_hourly_clicks")
            .option("checkpointLocation", "data/checkpoints/silver-to-gold-campaign-hourly")
            .outputMode("append")
            .start();

        query.awaitTermination();
    }
}
