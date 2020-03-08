package com.pgbde.capstone;

import java.io.IOException;

import com.pgbde.capstone.bean.LookupData;
import com.pgbde.capstone.bean.TransactionData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;



public class HBaseUtils {
    public final static String LOOKUP_TABLE = "transaction_lookup_table";
    private final static String MEMBER_DETAILS = "MemberDetails";
    private final static String TRANSACTION_DETAILS = "TransactionDetails";
    private final static String CARD_DETAILS = "CardDetails";

    private static Connection connection = null;
    private static Table lookupTable =null;

    public static void main(String[] args) throws Exception {
        init();
//      write to HBase
        addRecord(lookupTable,"659990931314251", MEMBER_DETAILS, "score", "297");
//        addRecord(lookupTable,"659990931314251", CARD_DETAILS, "member_id", "928036864799687");
//        addRecord(lookupTable,"659990931314251", TRANSACTION_DETAILS, "postcode", "48060");
//        addRecord(lookupTable,"659990931314251", TRANSACTION_DETAILS, "transaction_dt", "31-10-2017 23:10:04");
//        addRecord(lookupTable,"659990931314251", TRANSACTION_DETAILS, "ucl", "1.3567");

//      read from HBase
        LookupData data = getRecord( "659990931314251");
        System.out.println(data.toString());
        close();

    }

    private static void init(){

        Configuration conf = HBaseConfiguration.create();
        conf.addResource(new Path("file:///~/Downloads/hbase-1.6.0/conf/hbase-site.xml"));

        try {
            connection = ConnectionFactory.createConnection(conf);
            lookupTable = connection.getTable(TableName.valueOf(LOOKUP_TABLE));

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }


    private static void close() {
        try {
            lookupTable.close();
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static LookupData getRecord(String cardId) {
        init();
        LookupData data = new LookupData();
        try {
            Get g = new Get(Bytes.toBytes(cardId));
            Result result = lookupTable.get(g);
            byte[] score = result.getValue(Bytes.toBytes(MEMBER_DETAILS), Bytes.toBytes("score"));

            if (score != null) {
                data.setScore(Integer.parseInt(Bytes.toString(score)));
            }

            byte[] memberId = result.getValue(Bytes.toBytes(CARD_DETAILS), Bytes.toBytes("member_id"));
            byte[] postcode = result.getValue(Bytes.toBytes(TRANSACTION_DETAILS), Bytes.toBytes("postcode"));
            byte[] transactionDate = result.getValue(Bytes.toBytes(TRANSACTION_DETAILS), Bytes.toBytes("transaction_dt"));
            byte[] ucl = result.getValue(Bytes.toBytes(TRANSACTION_DETAILS), Bytes.toBytes("ucl"));

            data.setCardId(cardId);
            data.setMemberId(Bytes.toString(memberId));
            data.setPostCode(Bytes.toString(postcode));
            data.setTransactionDate(Bytes.toString(transactionDate));
            data.setUcl(Bytes.toString(ucl));

        } catch (Exception e) {
            e.printStackTrace();
        }
        close();
        return data;
    }
    // put one row
    public static void addRecord( Table destinationTable,  String rowKey,
                                 String family, String qualifier, String value)  {
        init();
        try {
            Put put = new Put(Bytes.toBytes(rowKey));
                put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier),
                    Bytes.toBytes(value));
            destinationTable.put(put);
//          System.out.println("insert " + rowKey + " to table.");
            destinationTable.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        close();
    }
    public static void addTransaction(String tableName, TransactionData data){

        init();
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            addRecord( table, data.getCard_id(), TRANSACTION_DETAILS, "postcode", data.getPostcode());
            addRecord( table, data.getCard_id(), TRANSACTION_DETAILS, "transaction_dt", data.getTransaction_dt());
        } catch (IOException e) {
            e.printStackTrace();
        }
        close();
    }
}
