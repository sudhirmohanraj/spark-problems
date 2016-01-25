package com.spark.problems.hdfs;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * Perfroms a join on two 1*N relationships table and filters it based on a criteria by
 * retrieving data from hdfs.
 * @author mohanraj,sudhir
 */
public class IndividualCarOwnersApp {

    public static void main(String[] args){
        //Set up spark config.
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Finds out all the cars owned by a customer of last name mohan.");

        // To avoid invalid compression problems.
        conf.set("spark.io.compression.codec", "org.apache.spark.io.LZ4CompressionCodec");


        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        DataFrame jdbcDFCars = sqlContext.read().json("hdfs://localhost:9000/input/Cars.json");
        DataFrame jdbcDFOwners = sqlContext.read().json("hdfs://localhost:9000/input/Owners.json");
        DataFrame jdbcDFCarsOwners = sqlContext.read().json("hdfs://localhost:9000/input/CarsOwners.json");

        DataFrame jdbcOutput = jdbcDFCarsOwners
                .distinct()
                .join(jdbcDFCars, jdbcDFCars.col("idCars").equalTo(jdbcDFCarsOwners.col("CarId")))
                .join(jdbcDFOwners,jdbcDFOwners.col("idOwners").equalTo(jdbcDFCarsOwners.col("OwnerId")))
                .filter("LastName like 'mohan'").select("Make","Model","FirstName","LastName")
                ;

        jdbcOutput.save("hdfs://localhost:9000/output/IndividualCarOwners.json");

    }
}
