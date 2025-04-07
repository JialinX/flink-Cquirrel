package com.example.model;

import java.time.LocalDate;

public class Order {
    private long oOrderkey;
    private long oCustkey;
    private LocalDate oOrderdate;

    public Order(long oOrderkey, long oCustkey, LocalDate oOrderdate) {
        this.oOrderkey = oOrderkey;
        this.oCustkey = oCustkey;
        this.oOrderdate = oOrderdate;
    }

    public long getOOrderkey() {
        return oOrderkey;
    }

    public long getOCustkey() {
        return oCustkey;
    }

    public LocalDate getOOrderdate() {
        return oOrderdate;
    }

    public static Order fromString(String data) {
        String[] fields = data.split("\\|");
        return new Order(
            Long.parseLong(fields[0]),
            Long.parseLong(fields[1]),
            LocalDate.parse(fields[4])  // o_orderdate field
        );
    }
} 