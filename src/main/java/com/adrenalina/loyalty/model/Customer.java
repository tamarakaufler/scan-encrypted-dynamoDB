package com.adrenalina.loyalty.model;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.datamodeling.encryption.DoNotTouch;

import java.util.Date;


@DynamoDBTable(tableName="Customer")
public class Customer {

    private Date date;

    public Customer() {}

    public Customer(Long customerId, String card, Date date) {
        this.setCard(card);
        this.setCustomerId(customerId);
        this.namespace = namespace;
        this.date = date;
    }

    @DoNotTouch
    @Override
    @DynamoDBHashKey(attributeName="customerId")
    public Long getCustomerId() {
        return super.getCustomerId();
    }

    //Encrypted by default
    @Override
    @DynamoDBAttribute(attributeName="card")
    public String getCard() {
        return super.getCard();
    }

    @DoNotTouch
    @DynamoDBAttribute(attributeName="date")
    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    @Override
    public boolean equals(Object obj) {

        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        final Customer other = (Customer) obj;
        return Objects.equal(this.getCard(), other.getCard())
                && Objects.equal(this.getCustomerId(), other.getCustomerId())
                && Objects.equal(this.date, other.date);

    }

    @Override
    public int hashCode() {

        return Objects.hashCode(
                this.getCard(),
                this.getCustomerId(),
                this.date);

    }

    @Override
    public String toString() {

        return MoreObjects.toStringHelper(this)
                .add("card", "xxxxxxxxxxx")
                .add("customerId", getCustomerId())
                .add("date", date)
                .toString();
    }

}

