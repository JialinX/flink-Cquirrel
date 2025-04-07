package com.example.model;

public class DataRecord {
    private String type;  // "+" or "-"
    private String tableType;  // "LI", "OR", "CU"
    private String data;  // 实际数据

    public DataRecord(String type, String tableType, String data) {
        this.type = type;
        this.tableType = tableType;
        this.data = data;
    }

    public String getType() {
        return type;
    }

    public String getTableType() {
        return tableType;
    }

    public String getData() {
        return data;
    }

    public static DataRecord fromString(String line) {
        String type = line.substring(0, 1);
        String tableType = line.substring(1, 3);
        String data = line.substring(3);
        return new DataRecord(type, tableType, data);
    }
} 