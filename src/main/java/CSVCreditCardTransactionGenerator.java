import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.CreditCardTransactionsGenerator;

import java.io.File;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class CSVCreditCardTransactionGenerator {

    public static void main(String[] args)  {
        Logger logger = LoggerFactory.getLogger("CSVCreditCardTransactionGenerator");

        int numOfRecords = Integer.parseInt(args[0].trim());
        String delimiter = args[1].trim();
        String csvOutputPath = args[2].trim();
        int scheduleFixRate = Integer.parseInt(args[3].trim());
        ScheduledExecutorService ses = Executors.newScheduledThreadPool(1);

        try {

            //checks csv out path existence, if not thre will create the directory
            File outputDir = new File(csvOutputPath);
            if (!outputDir.exists()) {outputDir.mkdirs();}

            logger.info("Generating credit card transactions and writing to path: " + csvOutputPath);
            Runnable creditCardTransactionsGeneratorTask = () ->
                    CreditCardTransactionsGenerator.creditCardTransactionsGenerator(numOfRecords, delimiter, csvOutputPath);

            ses.scheduleAtFixedRate(creditCardTransactionsGeneratorTask, 0, scheduleFixRate, TimeUnit.SECONDS);
        } catch (Exception ex){
            logger.error("Application CSVCreditCardTransactionGenerator stopped with error: " + ex);
            throw ex;

        }
    }
}
