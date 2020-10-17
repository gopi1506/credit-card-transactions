package model.writer;

import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;

public class SinkWriter {
    public static StreamingQuery consoleSink(Dataset<Row> creditCardTranDs, int triggerInterval, String timeFormat)  {
       return creditCardTranDs
                .writeStream()
                .format("console")
                .queryName("console sink")
                .outputMode("update")
                .trigger(Trigger.ProcessingTime(String.format("%s %s", triggerInterval, timeFormat)))
                .option("truncate", "false")
                .start();
    }

    public static StreamingQuery fileSink(Dataset<Row> creditCardTranDs, int triggerInterval, String timeFormat, String outputPath, String checkPointLocation)  {
        return creditCardTranDs
                .writeStream().foreachBatch(new VoidFunction2<Dataset<Row>, Long>() {
                    public void call(Dataset<Row> dataset, Long batchId) {
                        for (String c: dataset.columns()) {
                            dataset = dataset.withColumn(c, dataset.col(c).cast(DataTypes.StringType));
                        }
                        dataset.write().option("header", "true").mode("append").csv(outputPath);
                    }
                })
                .option("checkpointLocation", checkPointLocation)
                .outputMode("update")
                .queryName("csv file sink")
                .trigger(Trigger.ProcessingTime(String.format("%s %s", triggerInterval, timeFormat)))
                .start();
    }
}
