package com.daniellemustillo.datalake_demo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class SparkJobsController {
    @Autowired
    private KafkaToBronzeClicksJob kafkaToBronzeClicksJob;
    
    @Autowired
    private KafkaToSilverClicksJob kafkaToSilverClicksJob;

    @Autowired
    private SilverToGoldCampaignHourlyStreamingJob silverToGoldCampaignHourlyStreamingJob;

    @Value("${spring.kafka.ad-click-topic")
    private String topic;
    
    @PostMapping("/bronze")
    public void bronze() throws Exception  {
        kafkaToBronzeClicksJob.run();
    }
    
    @PostMapping("/silver")
    public void silver() throws Exception {
        kafkaToSilverClicksJob.run();
    }
    
    @PostMapping("/gold")
    public void gold() throws Exception {
        silverToGoldCampaignHourlyStreamingJob.run();
    }


}
