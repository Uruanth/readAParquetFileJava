package com.example.demo;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.lang.reflect.Field;
import java.math.BigDecimal;

import org.apache.spark.sql.*;

@Slf4j
@Service
public class Spaa {

    @Autowired
    private SparkSession spark;

    public void get() {
        // Lee el archivo Parquet en un DataFrame
        String parquetPath = "E:\\Agrosuper\\docs\\materiales.parquet";
        Dataset<Row> parquetDF = spark.read().parquet(parquetPath);

        // Puedes realizar operaciones con el DataFrame, por ejemplo, mostrar el contenido
        parquetDF.show(20);
        System.out.println(parquetDF.count());


// TODO buscar datos dentro del archivo
//        Dataset<Row> res = parquetDF.filter(col("/BIC/ZMATERIAL").equalTo("3100001663"));
//        System.out.println("res.count() = " + res.count());

//        TODO obtener los datos de la primera fila
//        Row rr = parquetDF.first();
//        if (Objects.nonNull(rr)) {
//            String id = rr.getAs(0);
//            System.out.println("id = " + id);
//            System.out.println("rr " + rr.length());
//        }
//
//        IntStream.range(0, rr.length()-1)
//                .forEach(value -> {
//                    System.out.println("index: " + value + " value: " + rr.get(value));
//                    System.out.println("class: " + rr.get(value).getClass());
//                });


//        TODO Schema de datos del archivo
//        StructType schema = parquetDF.schema();

        // Imprime el nombre de las columnas y sus tipos de dato
//        for (StructField field : schema.fields()) {
//            String columnName = field.name();
//            String dataType = field.dataType().toString();
//            System.out.println("Columna: " + columnName + ", Tipo de dato: " + dataType);
//        }


        Row firstRow = parquetDF.first();

        Entidad entidad = new Entidad();
        Class<?> entidadClass = Entidad.class;

        int numColumns = firstRow.size();
        for (int i = 0; i < numColumns; i++) {
            Object columnValue = firstRow.get(i);

            // Obtén el atributo correspondiente en la clase Entidad
            Field field = entidadClass.getDeclaredFields()[i];
            field.setAccessible(true);

            // Asigna el valor al atributo utilizando reflexión
            try {
                if (field.getType() == BigDecimal.class) {
                    // Si el tipo es BigDecimal, convierte el valor a BigDecimal antes de asignarlo
                    field.set(entidad, new BigDecimal(columnValue.toString()));
                } else {
                    field.set(entidad, columnValue.toString());
                }
            } catch (IllegalAccessException e) {
                e.printStackTrace();
                // Manejar cualquier excepción si es necesario
            }
        }

        log.info(entidad.getBicZmaterial());

    }


    // Método de utilidad para capitalizar la primera letra de una cadena
    private static String capitalize(String str) {
        return str.substring(0, 1).toUpperCase() + str.substring(1);
    }

}
