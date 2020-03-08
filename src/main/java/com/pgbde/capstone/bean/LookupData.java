package com.pgbde.capstone.bean;

import java.io.Serializable;

public class LookupData implements Serializable {

    @Override
    public String toString() {
        return "LookupData{" +
                "cardId='" + cardId + '\'' +
                ", memberId='" + memberId + '\'' +
                ", score=" + score +
                ", postCode='" + postCode + '\'' +
                ", ucl='" + ucl + '\'' +
                ", transactionDate='" + transactionDate + '\'' +
                '}';
    }

    private String cardId;
    private String memberId;
    private Integer score;
    private String postCode;
    private String ucl;

    public String getCardId() {
        return cardId;
    }

    public void setCardId(String cardId) {
        this.cardId = cardId;
    }

    public String getMemberId() {
        return memberId;
    }

    public void setMemberId(String memberId) {
        this.memberId = memberId;
    }

    public Integer getScore() {
        return score;
    }

    public void setScore(Integer score) {
        this.score = score;
    }

    public String getPostCode() {
        return postCode;
    }

    public void setPostCode(String postCode) {
        this.postCode = postCode;
    }

    public String getUcl() {
        return ucl;
    }

    public void setUcl(String ucl) {
        this.ucl = ucl;
    }

    public String getTransactionDate() {
        return transactionDate;
    }

    public void setTransactionDate(String transactionDate) {
        this.transactionDate = transactionDate;
    }

    private String transactionDate;

}
