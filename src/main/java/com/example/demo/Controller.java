package com.example.demo;

import lombok.AllArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;

import java.util.Collections;
import java.util.List;

@RestController
@RequestMapping("/api")
@AllArgsConstructor
public class Controller {

    private final Spaa ser;
    private final ReadParquet readParquet;

    @GetMapping
    public String getParquet() {
        ser.get();
        return "funciono";
    }

    @GetMapping("/spark")
    public List<Entidad> getParquetRead() {

        return readParquet.get();
//        return Collections.emptyList();
    }


}
