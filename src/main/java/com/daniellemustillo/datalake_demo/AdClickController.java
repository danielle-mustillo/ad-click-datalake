package com.daniellemustillo.datalake_demo;

import org.springframework.web.bind.annotation.RestController;

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
    private KafkaTemplate<String, String> kafkaTemplate;

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
        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(adClicks, adClick.toString());
        // TODO tostring isn't right

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                System.out.println("Sent message=[" + adClick.getEventId() +
                        "] with offset=[" + result.getRecordMetadata().offset() + "]");
            } else {
                System.out.println("Unable to send message=[" +
                        adClick.getEventId() + "] due to : " + ex.getMessage());
            }
        });
        return "Accepted";
    }

}
