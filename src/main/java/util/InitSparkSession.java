package util;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;


public class InitSparkSession {
    public static SparkSession initSparkSession(String appName)  {

        SparkConf conf = new SparkConf()
                .setAppName(appName)
                .set("spark.sql.shuffle.partitions", "4")
                .setMaster("local[*]");

        return SparkSession
                .builder()
                .config(conf)
                .getOrCreate();

    }
}