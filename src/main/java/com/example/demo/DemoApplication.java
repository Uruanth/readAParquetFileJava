package com.example.demo;

import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.annotation.PreDestroy;
import java.io.IOException;

@SpringBootApplication
public class DemoApplication {

    @Autowired
    private static SparkSession session;


    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);
    }

    @PreDestroy
    public void close() {
        System.out.println("Cerrando app");
        session.stop();
    }

}
