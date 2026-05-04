package com.daniellemustillo.datalake_demo;

import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseStatus;

@RestController
public class AdClickController {
    @Autowired
    private KafkaTemplate<String, AdClick> kafkaTemplate;

    @Autowired
    private KafkaToDataLakePipelines dataLakePipelines;

    @Value(value = "${spring.kafka.ad-click-topic}")
    private String adClicks;

    @GetMapping("/")
    public String index() {
        return new String("Greetings from Spring boot!");
    }

    @PostMapping("/click")
    @ResponseStatus(value = HttpStatus.ACCEPTED)
    public String ad_click(@RequestBody AdClick adClick) {
        // TODO move this to a service class ad-clicks

        // Overwrite the event time with the server time, just to make it easier to generate new timestamps & realism
        adClick.setEventTime(Instant.now().toString());

        CompletableFuture<SendResult<String, AdClick>> future = kafkaTemplate.send(adClicks, adClick);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                System.out.println("Sent message=[" + adClick.getEventId() + "] with offset=[" + result.getRecordMetadata().offset() + "]");
            } else {
                System.out.println("Unable to send message=[" + adClick.getEventId() + "] due to : " + ex.getMessage());
            }
        });
        return "Accepted";
    }

    @PostMapping("/aggregate")
    public void aggregate() throws Exception {
        dataLakePipelines.silverToGold();
    }

    @GetMapping("/gold")
    public void showGold() throws Exception {
        dataLakePipelines.showGold();
    }
    @GetMapping("/silver")
    public void showSilver() throws Exception {
        dataLakePipelines.showSilver();
    }
    @GetMapping("/bronze")
    public void showBronze() throws Exception {
        dataLakePipelines.showBronze();
    }

}
