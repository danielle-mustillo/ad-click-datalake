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
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate();
    }
}
