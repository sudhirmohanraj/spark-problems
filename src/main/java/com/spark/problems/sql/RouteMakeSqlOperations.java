package com.spark.problems.sql;

import com.spark.problems.model.RouteMake;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.List;

/**
 * Created by wyh669 on 1/22/16.
 */
public class RouteMakeSqlOperations {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Calculates the popular routes");
        // To avoid invalid compression problems.
        conf.set("spark.io.compression.codec", "org.apache.spark.io.LZ4CompressionCodec");
        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = new SQLContext(sc);

        JavaRDD<RouteMake> routeMakeJavaRDD = sc.textFile("Car_Routes.txt").map(new Function<String, RouteMake>() {
            public RouteMake call(String v1) throws Exception {
                String[] parts = v1.split(",");
                RouteMake routeMake = new RouteMake();
                routeMake.setMake(parts[1]);
                routeMake.setRoute(parts[0]);
                return routeMake;
            }
        });

        DataFrame schemaRouteMake = sqlContext.createDataFrame(routeMakeJavaRDD, RouteMake.class);
        schemaRouteMake.registerTempTable("routeMake");

        DataFrame routeMakes = sqlContext.sql("select distinct make from routeMake where route='I-435'");

        List<String> routeMakesList = routeMakes.javaRDD().map(new Function<Row, String>() {
            public String call(Row row) {
                return "RouteMake: " + row.getString(0);
            }
        }).collect();

        System.out.println(routeMakesList);
    }
}
