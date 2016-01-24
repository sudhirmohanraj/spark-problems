package com.spark.problems.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.util.HashMap;
import java.util.Map;

/**
 * Perfroms a join on two 1*N relationships table and filters it based on a criteria.
 * @author mohanraj,sudhir
 */
public class IndividualCarOwnersApp {

    public static void main(String[] args) {

        String URL = "jdbc:mysql://localhost:3306/Owners?user=root&password=123456";
        String DRIVER = "com.mysql.jdbc.Driver";

        // sample input under resources Cars.json
        String TABLE_NAME_CARS ="Cars";
        // sample input under resources Owners.json
        String TABLE_NAME_OWNERS = "Owners";
        // sample input under resources CarsOwners.json
        String TABLE_NAME_CARS_OWNERS = "CarsOwners";

        //set up Mysql Connection.
        Map<String, String> options = new HashMap<String, String>();
        options.put("url", URL);
        options.put("dbtable", TABLE_NAME_CARS);
        options.put("driver", DRIVER);

        Map<String, String> optionsOwners = new HashMap<String, String>();
        optionsOwners.put("url", URL);
        optionsOwners.put("dbtable", TABLE_NAME_OWNERS);
        optionsOwners.put("driver", DRIVER);


        Map<String, String> optionsCarsOwners = new HashMap<String, String>();
        optionsCarsOwners.put("url", URL);
        optionsCarsOwners.put("dbtable",TABLE_NAME_CARS_OWNERS);
        optionsCarsOwners.put("driver", DRIVER);

        //Set up spark config.
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Finds out all the cars owned by a customer of last name mohan.");

        // To avoid invalid compression problems.
        conf.set("spark.io.compression.codec", "org.apache.spark.io.LZ4CompressionCodec");


        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        //If need be ingesting data from json follow the second approach.
        DataFrame jdbcDFCars = sqlContext.load("jdbc", options);
        //DataFrame jdbcDFCars = sqlContext.read().json("src/main/resources/Cars.json");

        DataFrame jdbcDFOwners = sqlContext.load("jdbc", optionsOwners);
        //DataFrame jdbcDFOwners = sqlContext.read().json("src/main/resources/Owners.json");

        DataFrame jdbcDFCarsOwners = sqlContext.load("jdbc", optionsCarsOwners);
        //DataFrame jdbcDFCarsOwners = sqlContext.read().json("src/main/resources/CarsOwners.json");

        jdbcDFCarsOwners
                .distinct()
                .join(jdbcDFCars, jdbcDFCars.col("idCars").equalTo(jdbcDFCarsOwners.col("CarId")))
                .join(jdbcDFOwners,jdbcDFOwners.col("idOwners").equalTo(jdbcDFCarsOwners.col("OwnerId")))
                .filter("LastName like 'mohan'").select("Make","Model","FirstName","LastName")
                .show();

    }
}
