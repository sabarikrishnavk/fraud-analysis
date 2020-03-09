 
Create a kafka stream in local
Download apache kafka

cd Downloads/kafka_2.12-2.3.0

Run the following command in different windows bin/zookeeper-server-start.sh config/zookeeper.properties

bin/kafka-server-start.sh config/server.properties

bin/kafka-console-producer.sh --broker-list localhost:9092 --topic transactions-topic-verified

Paste the following json string as input to the stream in the producer window.
{
                                                                              
“card_id”:348702330256514,

“member_id”: 000037495066290,

“amount”: 9084849,

“pos_id”: 614677375609919,

“postcode”: 33946,

“transaction_dt”: “11-02-2018 00:00:00”

}


Spark program with kafka stream
Main class KafkaSparkHBaseStream accepts following mandatory parameters in arguments
 
kafkahost with port : 18.211.252.152:9092 
topic-name : transactions-topic-verified
input zipcode.csv path : /home/ec2-user/
output folder to keep the card transactions :  -output/
iteration number : to generate the groupId for kakfa consumer


mvn clean package 

java -cp target/FraudAnalysis-jar-with-dependencies.jar com.pgbde.capstone.KafkaSparkHBaseStream 18.211.252.152:9092 transactions-topic-verified "/Users/dks0410482/Desktop/workspace/Capstone/fraud-analysis/input/zipCodePosId.csv" output/ 7


To run the same in AWS
nohup spark2-submit --class com.pgbde.capstone.KafkaSparkHBaseStream --master yarn --deploy-mode client --executor-memory 4G FraudAnalysis-jar-with-dependencies.jar 18.211.252.152:9092 transactions-topic-verified /home/ec2-user/capstone/data/zipCodePosId.csv output/ 11 >> output.txt &




