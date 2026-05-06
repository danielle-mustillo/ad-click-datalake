package com.daniellemustillo.datalake_demo;

import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Configuration
public class SparkConfig {

    @Bean
    @Profile("!test")
    public SparkSession sparkSession() {
        return SparkSession.builder()
                .appName("ad-click-database")
                .master("local[*]")
                // Iceberg Spark extensions
                .config(
                        "spark.sql.extensions",
                        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")

                // Local Iceberg catalog
                .config("spark.driver.extraJavaOptions",
                        "--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED")
                .config("spark.executor.extraJavaOptions",
                        "--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED")
                .config(
                        "spark.sql.catalog.local",
                        "org.apache.iceberg.spark.SparkCatalog")
                .config(
                        "spark.sql.catalog.local.type",
                        "hadoop")
                .config(
                        "spark.sql.catalog.local.warehouse",
                        "data/warehouse")
                .config("spark.driver.host", "127.0.0.1")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "4")
                .getOrCreate();
    }
}
