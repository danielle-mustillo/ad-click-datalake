package com.daniellemustillo.datalake_demo;

import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.GetMapping;

@RestController
public class HelloController {

    @GetMapping("/")
    public String index() {
        return new String("Greetings from Spring boot!");
    }

}
