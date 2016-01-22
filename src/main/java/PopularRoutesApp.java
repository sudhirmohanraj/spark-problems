/* PopularRoutesApp.java */
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.collection.parallel.ParIterableLike;

import java.lang.System;
import java.util.Arrays;

public class PopularRoutesApp {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Calculates the popular routes");
        // To avoid invalid compression problems.
        conf.set("spark.io.compression.codec","org.apache.spark.io.LZ4CompressionCodec");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> routesData = sc.textFile("Car_Routes.txt").cache().flatMap(new FlatMapFunction<String, String>() {
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(","));
            }
        });

        routesData = routesData.filter(new Function<String, Boolean>() {
            public Boolean call(String v1) throws Exception {
                return v1.startsWith("I-");
            }
        });

        JavaPairRDD<String, Integer> pairs = routesData.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) { return new Tuple2<String, Integer>(s, 1); }
        });

        JavaPairRDD<String,Integer> pairsCount = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });

        System.out.println(pairsCount.collect());
    }
}