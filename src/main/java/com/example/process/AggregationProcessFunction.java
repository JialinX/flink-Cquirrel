package com.example.process;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple4;
import java.util.HashMap;
import java.util.Map;

public class AggregationProcessFunction extends ProcessFunction<Tuple4<Long, String, Double, Double>, Map<String, Double>> {
    private MapState<String, Double> revenueState; // shipmode -> revenue

    @Override
    public void open(Configuration parameters) throws Exception {
        revenueState = getRuntimeContext().getMapState(
            new MapStateDescriptor<>("revenue-state", String.class, Double.class));
    }

    @Override
    public void processElement(Tuple4<Long, String, Double, Double> item, Context context, Collector<Map<String, Double>> out) throws Exception {
        String shipmode = item.f1;  // shipmode
        double revenue = item.f2 * (1 - item.f3);  // extendedprice * (1 - discount)
        
        Double currentRevenue = revenueState.get(shipmode);
        if (currentRevenue == null) {
            currentRevenue = 0.0;
        }
        currentRevenue += revenue;
        revenueState.put(shipmode, currentRevenue);
        
        // 输出当前所有shipmode的revenue
        Map<String, Double> result = new HashMap<>();
        for (Map.Entry<String, Double> entry : revenueState.entries()) {
            result.put(entry.getKey(), entry.getValue());
        }
        out.collect(result);
    }
} 