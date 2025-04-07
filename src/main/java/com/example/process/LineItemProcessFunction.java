package com.example.process;

import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import java.util.Set;
import java.util.HashSet;
import java.util.List;

public class LineItemProcessFunction extends KeyedProcessFunction<Long, Tuple4<Long, String, Double, Double>, Tuple4<Long, String, Double, Double>> {
    // 存储 orderkey -> lineitem 的映射
    private MapState<Long, Set<Tuple4<Long, String, Double, Double>>> lineItemState;
    // 存储 orderkey 的集合
    private MapState<Long, Boolean> orderKeyState;
    
    private static final List<String> VALID_SHIPMODES = List.of("RAIL", "AIR", "TRUCK");

    @Override
    public void open(Configuration parameters) throws Exception {
        // 初始化 lineItemState
        MapStateDescriptor<Long, Set<Tuple4<Long, String, Double, Double>>> lineItemDescriptor = 
            new MapStateDescriptor<>("lineitem-state", 
                TypeInformation.of(Long.class),
                TypeInformation.of(new TypeHint<Set<Tuple4<Long, String, Double, Double>>>() {}));
        lineItemState = getRuntimeContext().getMapState(lineItemDescriptor);

        // 初始化 orderKeyState
        MapStateDescriptor<Long, Boolean> orderKeyDescriptor = 
            new MapStateDescriptor<>("orderkey-state", 
                TypeInformation.of(Long.class),
                TypeInformation.of(Boolean.class));
        orderKeyState = getRuntimeContext().getMapState(orderKeyDescriptor);
    }

    @Override
    public void processElement(Tuple4<Long, String, Double, Double> item, Context context, Collector<Tuple4<Long, String, Double, Double>> out) throws Exception {
        Long orderKey = item.f0;
        String shipmode = item.f1;
        
        if (shipmode.isEmpty()) {
            // 这是一个来自 orderKey 的查询
            orderKeyState.put(orderKey, true);
            
            Set<Tuple4<Long, String, Double, Double>> items = lineItemState.get(orderKey);
            if (items != null) {
                for (Tuple4<Long, String, Double, Double> storedItem : items) {
                    out.collect(storedItem);
                }
            }
        } else if (VALID_SHIPMODES.contains(shipmode)) {
            // 这是一个实际的 LineItem 数据
            Set<Tuple4<Long, String, Double, Double>> items = lineItemState.get(orderKey);
            if (items == null) {
                items = new HashSet<>();
            }
            items.add(item);
            lineItemState.put(orderKey, items);

            if (orderKeyState.get(orderKey) != null) {
                out.collect(item);
            }
        }
    }

    public void processOrderKey(Long orderKey, Collector<Tuple4<Long, String, Double, Double>> out) throws Exception {
        Set<Tuple4<Long, String, Double, Double>> items = lineItemState.get(orderKey);
        if (items != null) {
            for (Tuple4<Long, String, Double, Double> item : items) {
                out.collect(item);
            }
        }
    }
} 