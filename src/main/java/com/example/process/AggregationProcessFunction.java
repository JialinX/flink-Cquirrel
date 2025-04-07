package com.example.process;

import com.example.model.LineItem;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import java.util.HashMap;
import java.util.Map;

public class AggregationProcessFunction extends ProcessFunction<LineItem, Map<String, Double>> {
    private MapState<String, Double> revenueState; // shipmode -> revenue

    @Override
    public void open(Configuration parameters) throws Exception {
        revenueState = getRuntimeContext().getMapState(
            new MapStateDescriptor<>("revenue-state", String.class, Double.class));
    }

    @Override
    public void processElement(LineItem lineItem, Context context, Collector<Map<String, Double>> out) throws Exception {
        String shipmode = lineItem.getLShipmode();
        double revenue = lineItem.getLExtendedprice() * (1 - lineItem.getLDiscount());
        
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