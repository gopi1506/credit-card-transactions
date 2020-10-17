package model.reader;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;


public class CsvStreamReader {
    public static Dataset<Row> csvStreamReader(SparkSession spark, String inputPath, String delimiter, StructType schema)  {
        return spark.readStream()
                .option("header", "false")
                .option("delimiter", delimiter)
                .schema(schema)
                .csv(inputPath);

    }
}