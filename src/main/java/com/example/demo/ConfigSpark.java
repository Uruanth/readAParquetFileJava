package com.example.demo;


import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ConfigSpark {

    @Bean
    public SparkSession getSparkSession() {
        return SparkSession.builder()
                .appName("Read Parquet with Spark")
                .master("local[*]") // Utiliza el modo local con todos los núcleos disponibles
                .getOrCreate();
    }

}
