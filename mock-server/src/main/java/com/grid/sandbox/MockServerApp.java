package com.grid.sandbox;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.boot.autoconfigure.validation.ValidationAutoConfiguration;

@SpringBootApplication(exclude = {
        ValidationAutoConfiguration.class,
        HibernateJpaAutoConfiguration.class
})
public class MockServerApp {
    public static void main(String[] args) {
        SpringApplication.run(MockServerApp.class, args);
    }
}
