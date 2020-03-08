package com.pgbde.capstone.bean;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;
import java.util.Date;

/**
 * POJO Class to map kafka stream objects.
 *
 * {
 *
 * “card_id”:348702330256514,
 *
 * “member_id”: 000037495066290,
 *
 * “amount”: 9084849,
 *
 * “pos_id”: 614677375609919,
 *
 * “postcode”: 33946,
 *
 * “transaction_dt”: “11-02-2018 00:00:00”
 *
 * }
 */
public class TransactionData implements Serializable {

    private int count =1;
    private String card_id;
    private String member_id;
    private Long amount;
    private String pos_id;
    private String postcode;
    private String transaction_dt;



    private String status;

    @Override
    public String toString() {
        ObjectMapper mapper = new ObjectMapper();
        String s = this.card_id +":" + this.amount;
        try {
            s = mapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return s;
    }
    public TransactionData() {
    }

    public String getCard_id() {
        return card_id;
    }

    public String getMember_id() {
        return member_id;
    }

    public Long getAmount() {
        return amount;
    }

    public String getPos_id() {
        return pos_id;
    }

    public String getPostcode() {
        return postcode;
    }

    public String getTransaction_dt() {
        return transaction_dt;
    }

    public void setCard_id(String card_id) {
        this.card_id = card_id;
    }

    public void setMember_id(String member_id) {
        this.member_id = member_id;
    }

    public void setAmount(Long amount) {
        this.amount = amount;
    }

    public void setPos_id(String pos_id) {
        this.pos_id = pos_id;
    }

    public void setPostcode(String postcode) {
        this.postcode = postcode;
    }

    public void setTransaction_dt(String transaction_dt) {
        this.transaction_dt = transaction_dt;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}
