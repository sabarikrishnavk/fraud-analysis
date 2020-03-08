package com.pgbde.capstone;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pgbde.capstone.bean.LookupData;
import com.pgbde.capstone.bean.TransactionData;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;


/**
 *Main program to listen to kafka queue.
 *
 * input parameters : localhost 18.211.252.152:9092 transactions-topic-verified output/card_transactions.csv
 */
public class KafkaSparkHBaseStream {

    public static String GROUP_ID ="kafkaspark-sabarivk-7";// "TransactionDatagroup";
    public static String FRAUD_STATUS = "Fraudulent";
    public static String GENUINE_STATUS = "Genuine";
    public static double THRESHOLD_DISTANCE = 10000.0;

    public static void main(String[] args) throws InterruptedException {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        if (args.length < 4 ) {

            System.out.println(" Program Input missing:  hostname, kafka hostname:port , topic name , batch interval, window interval , sliding interval (in minutes) ," +
                    "eg: localhost localhost:9092 TransactionDataData output/sma");
            System.exit(0);
        }

        String hostname = args[0];
        String broker = args[1];//"localhost:9092";
        String topic = args[2];//"TransactionDataData";// "TransactionDatastream";

        Duration batDuration = Durations.seconds(1);
        String outputPath = args[3];

        System.out.println("Executing KafkaMVAStream ");
        SparkConf sparkConf = new SparkConf().setAppName("KafkaSparkStreaming").setMaster("local[*]");
        //Create JavaStreamingContext using the spark context
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, batDuration);

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", broker);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", JsonDeserializer.class);
        kafkaParams.put("group.id", GROUP_ID);
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", false);

        Set<String> topicSet = new HashSet<String>(Arrays.asList(topic.split(",")));

            DistanceUtility disUtil= DistanceUtility.getInstance();


        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(streamingContext,
            LocationStrategies.PreferConsistent(), ConsumerStrategies.<String, String> Subscribe(topicSet, kafkaParams));
 
        //Create a D stream with TransactionData data.
        JavaDStream<TransactionData> keyMap = messages.map(new Function<ConsumerRecord<String, String>, TransactionData>() {
               @Override
               public TransactionData call(ConsumerRecord<String, String> record) throws Exception {
                   ObjectMapper mapper = new ObjectMapper();
                   TransactionData transactionData=  mapper.convertValue(record.value(), TransactionData.class);
                   applyRules(transactionData, disUtil);
                   return transactionData;
               }
           }
        );


        keyMap.foreachRDD(new VoidFunction<JavaRDD<TransactionData>>() {
            @Override
            public void call(JavaRDD<TransactionData> transactionDataJavaRDD) throws Exception {
                transactionDataJavaRDD.foreach(transactionData -> {
                    System.out.println("cardnumber "+ transactionData.getCard_id() +" :status :  "+ transactionData.getStatus());
                    //update the transaction_lookup_table with the last
                } );
            }
        });
        keyMap.print();

        streamingContext.start();
        streamingContext.awaitTermination();

    }

    /**
     * Method to apply rules on the transaction Data.
     * Update the status into the transaction data pojo
     * @param transactionData
     * @param disUtil
     */
    private static void applyRules(TransactionData transactionData, DistanceUtility disUtil) {

        //Credit score of each member: Get the credit scode from HBase "transaction_lookup_table" table
        //MemberDetails.score
        //Last used Pincode from HBase "transaction_lookup_table" table.
        // TransactionDetails:postcode,TransactionDetails:transaction_dt,TransactionDetails:ucl")
        LookupData data =  HBaseUtils.getRecord(transactionData.getCard_id());

        String transactionStatus = GENUINE_STATUS;

        if(data.getMemberId() !=null){

            String lastPincode = data.getPostCode();
            Integer memberscore = data.getScore();
            if (memberscore < 200){
                transactionStatus = FRAUD_STATUS;
            }else {
                double distance = disUtil.getDistanceViaZipCode(lastPincode, transactionData.getPostcode());

                if (distance > THRESHOLD_DISTANCE) {
                    transactionStatus = FRAUD_STATUS;
                }
            }
        }
        transactionData.setStatus(transactionStatus);
        if(GENUINE_STATUS.equals(transactionStatus)) {
            //Populate the last updated transaction Data into
            HBaseUtils.addTransaction(HBaseUtils.LOOKUP_TABLE, transactionData);
        }
        System.out.println("Processed : "+transactionData.toString());
    }
}
