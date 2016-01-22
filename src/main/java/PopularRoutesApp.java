/* PopularRoutesApp.java */
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import scala.collection.parallel.ParIterableLike;

import java.lang.System;

public class PopularRoutesApp {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Calculates the popular routes");
        conf.set("spark.io.compression.codec","org.apache.spark.io.LZ4CompressionCodec");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> routesData = sc.textFile("Car_Routes.txt").cache();
        System.out.println("hello");
        System.out.println(routesData);

        //System.out.println("hello");
        //System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
    }
}