package com.daniellemustillo.datalake_demo;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
@Profile("!test")
public class SparkStreamingLifecycle implements SmartLifecycle {

    @Autowired
    private SparkSession spark;

    @Autowired
    private KafkaToDataLakePipelines streams;

    @Value("${spring.kafka.ad-click-topic}")
    private String topic;

    private final List<StreamingQuery> queries = new ArrayList<>();

    private boolean running;

    @Override
    public void start() {
        try {
            queries.add(streams.kafkaToBronze());
            queries.add(streams.kafkaToSilver());
            //queries.add(streams.silverToGold());

            running = true;

            System.out.println("Started Spark streaming queries: " + queries.size());
        } catch (Exception e) {
            running = false;
            throw new IllegalStateException("Failed to start Spark streaming queries", e);
        }
    }

    @Override
    public void stop() {
        for (StreamingQuery query : queries) {
            try {
                if (query.isActive()) {
                    query.stop();
                    System.out.println("Stopped Spark query: " + query.name());
                }
            } catch (Exception e) {
                System.err.println("Error stopping Spark query: " + query.name());
                e.printStackTrace();
            }
        }

        spark.stop();
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

}
