package com.example.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.HashMap;
import java.util.Map;

/**
 * 自定义的 Sink 函数，用于整合所有分区的结果，最终得到3个 shipmode 的最终结果
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
        
        // 更新或添加运输方式的收入
        shipModeRevenues.put(shipMode, revenue);
    }
    
    @Override
    public void close() throws Exception {
        // 在关闭时打印最终结果
        System.out.println("\n最终运输方式收入结果：");
        System.out.println("----------------------------------------");
        shipModeRevenues.forEach((mode, rev) -> 
            System.out.printf("运输方式: %s, 总收入: %.2f%n", mode, rev));
        System.out.println("----------------------------------------\n");
        
        // 清理资源
        shipModeRevenues.clear();
    }
} 