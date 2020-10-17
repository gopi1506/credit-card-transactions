package util;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

import static java.util.Arrays.asList;
import static net.andreinc.mockneat.unit.financial.CreditCards.creditCards;
import static net.andreinc.mockneat.unit.financial.Currencies.currencies;
import static net.andreinc.mockneat.unit.id.UUIDs.uuids;
import static net.andreinc.mockneat.unit.objects.From.from;
import static net.andreinc.mockneat.unit.text.CSVs.csvs;
import static net.andreinc.mockneat.unit.user.Names.names;

public class CreditCardTransactionsGenerator {
    public static void creditCardTransactionsGenerator(int recordsCount, String delimiter, String outputPath)  {
        DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String fileName = "credit_card_transactions_" +
                LocalDateTime.now().format(format).replaceAll(" ", "_")
                        .replaceAll(":", "_") + ".csv";

        List<String> listOfTimeStamps = asList(LocalDateTime.now().format(format), LocalDateTime.now().format(format), LocalDateTime.now().format(format));

        csvs().column(uuids())
                .column(names().first())
                .column(names().last())
                .column(from(listOfTimeStamps))
                .column(creditCards().visa().get())
                .column(creditCards().names().get())
                .column(currencies().name().get())
                .column(currencies().symbol().get())
                .column(currencies().code().get())
                .column(currencies().forexPair().get())
                .separator(delimiter)
                .write(outputPath + "/" + fileName, recordsCount);
    }
}