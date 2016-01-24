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
 * Performs basic sql operations by ingesting data from a simple json file.
 */
public class BasicSQLOperationsApp {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Basic SQL Operaions.");
        // To avoid invalid compression problems.
        conf.set("spark.io.compression.codec", "org.apache.spark.io.LZ4CompressionCodec");
        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = new SQLContext(sc);

        // Basic Conversion of a simple json file onto a DataFrame.
        DataFrame dataFrame = sqlContext.read().json("Car_Routes_Sql.json");
        dataFrame.show();
        dataFrame.printSchema();
        dataFrame.select("Make").show();
        dataFrame.select(dataFrame.col("Make"),dataFrame.col("Route").contains("I-90")).show();
    }
}
