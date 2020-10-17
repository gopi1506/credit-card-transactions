import model.process.Processor;
import model.reader.CsvStreamReader;
import model.writer.SinkWriter;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.InitSparkSession;

import java.util.Objects;

/**
 * Structured streaming demo to read csv with credit card transactions
 *
 * @author Gopi
 */
public class StreamCreditTransactions {


    public static void main(String[] args) throws StreamingQueryException {

        Logger logger = LoggerFactory.getLogger("StreamCreditTransactions");

        LogManager.getLogger("org").setLevel(Level.ERROR);
        LogManager.getLogger("akka").setLevel(Level.ERROR);

        String creditCardTransactionsFilePath = args[0].trim();
        String delimiter = args[1].trim();
        int windowDuration = Integer.parseInt(args[2].trim());
        int slidingIntervalDuration = Integer.parseInt(args[3].trim());
        String timeFormat = args[4].trim();
        int triggerInterval = Integer.parseInt(args[5].trim());
        String sinkType = args[6].trim();
        String outputPath = null;
        String checkpointPath = null;

        if (sinkType.equals("file")) {
            outputPath = args[7];
            checkpointPath = args[8];
        }

        SparkSession sparkSession = InitSparkSession.initSparkSession("credit-card-transactions-csv-structured-streaming");

        StructType creditTransactionsSchema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.StringType, true),
                DataTypes.createStructField("firstName", DataTypes.StringType, true),
                DataTypes.createStructField("lastName", DataTypes.StringType, true),
                DataTypes.createStructField("transactionTimeStamp", DataTypes.TimestampType, true),
                DataTypes.createStructField("cardType", DataTypes.StringType, true),
                DataTypes.createStructField("ccName", DataTypes.StringType, true),
                DataTypes.createStructField("currencyName", DataTypes.StringType, true),
                DataTypes.createStructField("currencySymbol", DataTypes.StringType, true),
                DataTypes.createStructField("currencyCode", DataTypes.StringType, true),
                DataTypes.createStructField("forexPair", DataTypes.StringType, true)});


        try {
            logger.info("Read csv in streaming from the path: " + creditCardTransactionsFilePath);
            Dataset<Row> creditCardTransactionsDs = CsvStreamReader.csvStreamReader(sparkSession,
                    creditCardTransactionsFilePath, delimiter, creditTransactionsSchema);

            Dataset<Row> transactionsCountForEveryThreeMinutes = Processor.countTransactionsForWindow(creditCardTransactionsDs, windowDuration, slidingIntervalDuration, timeFormat);

            StreamingQuery query = null;
            if (transactionsCountForEveryThreeMinutes.isStreaming()) {
                if (sinkType.equals("console")) {
                    logger.info("Writing to console sink...");
                    query = SinkWriter.consoleSink(transactionsCountForEveryThreeMinutes, triggerInterval, timeFormat);
                } else {
                    logger.info("Writing to file sink at path: " + outputPath);
                    query = SinkWriter.fileSink(transactionsCountForEveryThreeMinutes, triggerInterval, timeFormat, outputPath, checkpointPath);
                }

            }

            Objects.requireNonNull(query).awaitTermination();

        } catch (Exception ex){
            logger.error("Application StreamCreditTransactions stopped with error: " + ex);
            throw ex;
        }
    }


}