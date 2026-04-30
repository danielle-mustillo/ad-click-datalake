package com.daniellemustillo.datalake_demo;

import java.util.Arrays;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class DatalakeDemoApplication {

	public static void main(String[] args) {
		SpringApplication.run(DatalakeDemoApplication.class, args);
	}

    @Bean
    public CommandLineRunner commandLineRunner(ApplicationContext applicationContext) {
        return args -> {
            System.out.println("Lets inspect the beans provided by Spring Boot!!!");


            String[] beanNames = applicationContext.getBeanDefinitionNames();
            Arrays.sort(beanNames);
            for (String bean: beanNames) {
                System.out.println(bean);
            }
        };
    }
}


