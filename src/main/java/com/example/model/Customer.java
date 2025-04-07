package com.example.model;

public class Customer {
    private long cCustkey;
    private String cMktsegment;

    public Customer(long cCustkey, String cMktsegment) {
        this.cCustkey = cCustkey;
        this.cMktsegment = cMktsegment;
    }

    public long getCCustkey() {
        return cCustkey;
    }

    public String getCMktsegment() {
        return cMktsegment;
    }

    public static Customer fromString(String data) {
        String[] fields = data.split("\\|");
        return new Customer(
            Long.parseLong(fields[0]),
            fields[6]  // c_mktsegment field
        );
    }
} 