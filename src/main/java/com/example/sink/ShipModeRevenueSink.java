package com.example.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.HashMap;
import java.util.Map;

/**
 * Custom Sink function to integrate results from all partitions and get final results for 3 ship modes
 */
public class ShipModeRevenueSink extends RichSinkFunction<Tuple2<String, Double>> {
    
    private Map<String, Double> shipModeRevenues;
    
    @Override
    public void open(Configuration parameters) throws Exception {
        shipModeRevenues = new HashMap<>();
    }
    
    @Override
    public void invoke(Tuple2<String, Double> value, Context context) throws Exception {
        String shipMode = value.f0;
        Double revenue = value.f1;
        
        // Update or add shipping mode revenue
        shipModeRevenues.put(shipMode, revenue);
    }
    
    @Override
    public void close() throws Exception {
        // Print final results when closing
        System.out.println("\nFinal Shipping Mode Revenue Results:");
        System.out.println("----------------------------------------");
        shipModeRevenues.forEach((mode, rev) -> 
            System.out.printf("Shipping Mode: %s, Total Revenue: %.2f%n", mode, rev));
        System.out.println("----------------------------------------\n");
        
        // Clean up resources
        shipModeRevenues.clear();
    }
} 