package com.example.model;

public class LineItem {
    private long lOrderkey;
    private String lShipmode;
    private double lExtendedprice;
    private double lDiscount;

    public LineItem(long lOrderkey, String lShipmode, double lExtendedprice, double lDiscount) {
        this.lOrderkey = lOrderkey;
        this.lShipmode = lShipmode;
        this.lExtendedprice = lExtendedprice;
        this.lDiscount = lDiscount;
    }

    public long getLOrderkey() {
        return lOrderkey;
    }

    public String getLShipmode() {
        return lShipmode;
    }

    public double getLExtendedprice() {
        return lExtendedprice;
    }

    public double getLDiscount() {
        return lDiscount;
    }

    public static LineItem fromString(String data) {
        String[] fields = data.split("\\|");
        return new LineItem(
            Long.parseLong(fields[0]),
            fields[14],  // l_shipmode field
            Double.parseDouble(fields[5]),  // l_extendedprice field
            Double.parseDouble(fields[6])   // l_discount field
        );
    }
} 