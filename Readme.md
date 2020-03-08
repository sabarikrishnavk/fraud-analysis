 
Create a kafka stream in local
Download apache kafka

cd Downloads/kafka_2.12-2.3.0

Run the following command in different windows bin/zookeeper-server-start.sh config/zookeeper.properties

bin/kafka-server-start.sh config/server.properties

bin/kafka-console-producer.sh --broker-list localhost:9092 --topic stockstream

Paste the following json string as input to the stream in the producer window. (BTC,{"symbol":"BTC","count":1,"timestamp":"2019-11-10 01:34:00","priceData":{"total":0.0,"close":8783.09,"high":8787.21,"low":8783.09,"open":8786.94,"volume":16169.987}})

Spark program with kafka stream
Main class KafkaSparkHBaseStream accepts following mandatory parameters in arguments

hostname - localhost 
kafkahost :port -18.211.252.152:9092 
topic-name - transactions-topic-verified 
ouput folder -output/card_transactions.csv


mvn clean package 

java -cp target/FraudAnalysis-jar-with-dependencies.jar 

com.pgbde.capstone.KafkaSparkHBaseStream localhost 18.211.252.152:9092 transactions-topic-verified output/card_transactions.csv

java -cp target/FraudAnalysis-jar-with-dependencies.jar com.pgbde.capstone.KafkaSparkHBaseStream localhost 18.211.252.152:9092 transactions-topic-verified output/card_transactions.csv
