package com.daniellemustillo.datalake_demo;

import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;


@RestController
public class AdClickController {

    @GetMapping("/")
    public String index() {
        return new String("Greetings from Spring boot!");
    }
    
    @PostMapping("/click")
        public String ad_click(@RequestBody AdClick adClick) {
        // TODO write to kafka topic ad-clicks
            return entity;
        }
        
}
