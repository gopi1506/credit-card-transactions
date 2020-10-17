Credit transaction

This project contains two programs.

Generates the required number of credit card transactions for a given fixed interval of time.
Reads the csv with credit card transactions in streaming fashion and calculate for given window and sliding interval durations. 

### Programs

#### CSVCreditCardTransactionGenerator:
 ```
* Keep generates csv file with required number of credit transactions for a give fixed interval.
* Arguments required for this program
          - numOfRecords => Number of records will be generated for each interval
          - delimiter => delimiter for csv
          - csvOutputPath => output path for the generated csv
          - scheduleFixRate => schedule in seconds to generate the file continuosly 
```

```
# sample file: credit_card_transactions_2020-10-17_17_13_45.csv
2bc94edc-48af-4323-b7f2-93f42ef851d8|Leeanne|Joa|2020-10-17 17:13:45|4714265938715016|China Union Pay|Marka|ман|TWD|GBP/PLN
f065a5bb-2422-4b20-9f11-5919d50a36af|Cody|Colgan|2020-10-17 17:13:45|4714265938715016|China Union Pay|Marka|ман|TWD|GBP/PLN
b990ae03-23bd-41d0-8b12-8c35da377fb7|Velia|Semmens|2020-10-17 17:13:45|4714265938715016|China Union Pay|Marka|ман|TWD|GBP/PLN
ff894733-df56-48e7-90f7-156df73b95ab|Tessa|Albery|2020-10-17 17:13:45|4714265938715016|China Union Pay|Marka|ман|TWD|GBP/PLN
7fde5064-fb48-4772-8133-079dee14e5cb|Micheline|Hoppesch|2020-10-17 17:13:45|4714265938715016|China Union Pay|Marka|ман|TWD|GBP/PLN
```

#### StreamCreditTransactions:
 ```
* Reads csv file credit transactions in streaming fashion and calucates  transactions count for each window with sliding interval.
* Arguments required for this program
          - creditCardTransactionsFilePath => Input csv file path
          - delimiter => input csv delimiter
          - windowDuration => window duration for creating window
          - slidingIntervalDuration  => sliding interval duration for triggering computation
          - timeFormat => time format can be minutes or seconds. Applicable for all the duration configurations
          - triggerInterval => trigger interval time in seconds/minutes to trigger spark query execution
          - sinkType => sink type ccan be console or file;
          - outputPath => If sink type is file, this should set to proepr output path;
          - checkpointPath => check point location, only applicable for file sink
```

```
# sample output: console sink
+------------------------------------------+-----+
|window                                    |count|
+------------------------------------------+-----+
|[2020-10-17 17:14:24, 2020-10-17 17:14:29]|10   |
|[2020-10-17 17:14:27, 2020-10-17 17:14:32]|5    |
+------------------------------------------+-----+
```
# Note
    Ensure the durtion configurations between spark streaming and csv genration matches. Otherwise result counts may be empty. 