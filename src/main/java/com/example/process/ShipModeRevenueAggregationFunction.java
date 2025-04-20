package com.example.process;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;

import java.util.HashMap;
import java.util.Map;

/**
 * 聚合处理函数，用于计算每个 l_shipmode 的 SUM(l_extendedprice * (1 - l_discount)) 作为收入（revenue）
 */
public class ShipModeRevenueAggregationFunction extends ProcessFunction<Tuple4<String, Double, Double, String>, Tuple2<String, Double>> {
    
    // 使用 MapState 存储每个 shipmode 的收入
    private MapState<String, Double> shipModeRevenueMap;
    
    @Override
    public void open(Configuration parameters) throws Exception {
        // 初始化 MapState，key 是 shipmode，value 是收入
        shipModeRevenueMap = getRuntimeContext().getMapState(
            new MapStateDescriptor<>("shipmode-revenue-map", String.class, Double.class));
    }
    
    @Override
    public void processElement(Tuple4<String, Double, Double, String> value, Context context, Collector<Tuple2<String, Double>> collector) throws Exception {
        // 获取 shipmode、extendedPrice 和 discount
        String shipMode = value.f0;
        double extendedPrice = value.f1;
        double discount = value.f2;
        
        // 计算收入：extendedPrice * (1 - discount)
        double revenue = extendedPrice * (1 - discount);
        
        // 获取当前 shipmode 的收入
        Double currentRevenue = shipModeRevenueMap.get(shipMode);
        
        // 如果当前 shipmode 的收入为 null，则初始化为 0
        if (currentRevenue == null) {
            currentRevenue = 0.0;
        }
        
        // 累加收入
        currentRevenue += revenue;
        
        // 更新 shipmode 的收入
        shipModeRevenueMap.put(shipMode, currentRevenue);
        
        // 输出当前 shipmode 的总收入
        collector.collect(new Tuple2<>(shipMode, currentRevenue));
        
        // 打印日志
        // System.out.println("ShipMode: " + shipMode + ", 当前收入: " + revenue + ", 总收入: " + currentRevenue);
    }
} 