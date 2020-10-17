package model.process;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;


public class Processor {
    public static Dataset<Row> countTransactionsForWindow(Dataset<Row> creditCardTranDs, int windowDuration, int slidingInterval, String timeFormat)  {
        return creditCardTranDs
                .withWatermark("transactionTimeStamp", String.format("%s %s", windowDuration, timeFormat))
                .groupBy(
                functions.window(creditCardTranDs.col("transactionTimeStamp"),
                        String.format("%s %s", windowDuration, timeFormat),   String.format("%s %s", slidingInterval, timeFormat))).count();
    }
}