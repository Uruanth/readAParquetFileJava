package com.example.demo;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.List;
import java.util.function.Function;

@Slf4j
@Service
public class ReadParquet {

    @Autowired
    private SparkSession spark;

    public List<Entidad> get() {

        String parquetPath = "E:\\Agrosuper\\docs\\materiales.parquet";
        Dataset<Row> parquetDF = spark.read().parquet(parquetPath);

        // Convierte el DataFrame en un Dataset de Entidad utilizando una función de mapeo personalizada
        Dataset<Entidad> entidadDataset = parquetDF.map(mapToEntidad, Encoders.bean(Entidad.class));
        // Muestra el contenido del Dataset de Entidad (esto imprimirá la información en la consola)
        entidadDataset.show(10);
        log.info("total: {}", entidadDataset.count());
        return entidadDataset.collectAsList()
                .subList(0, 10);

    }

    // Función de mapeo para convertir una fila del DataFrame a un objeto Entidad
    private static final MapFunction<Row, Entidad> mapToEntidad = new MapFunction<Row, Entidad>() {
        @Override
        public Entidad call(Row row) throws Exception {
            Entidad entidad = new Entidad();
            Class<?> entidadClass = Entidad.class;

            int numColumns = row.size();
            for (int i = 0; i < numColumns; i++) {
                Object columnValue = row.get(i);

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

            return entidad;
        }
    };

}
