package com.example.process;

import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import com.example.model.Order;
import java.util.ArrayList;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class OrderProcessFunction extends KeyedCoProcessFunction<Long, Tuple2<Long, String>, Tuple2<Order, String>, Tuple2<Long, String>> {
    // 使用 MapState 存储 custkey 到 orderkey 列表的映射
    private MapState<Long, List<Long>> custKeyToOrderKeysMap;
    // 使用 ValueState<Set<Long>> 存储所有见过的 custkey
    private ValueState<Set<Long>> custKeySet;
    // 定义日期格式
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    // 定义日期范围
    private static final LocalDate START_DATE = LocalDate.parse("1995-01-01", DATE_FORMATTER);
    private static final LocalDate END_DATE = LocalDate.parse("1996-01-01", DATE_FORMATTER);

    @Override
    public void open(Configuration parameters) throws Exception {
        // 初始化 MapState，key 是 custkey，value 是 orderkey 列表
        custKeyToOrderKeysMap = getRuntimeContext().getMapState(
            new MapStateDescriptor<>("custkey-to-orderkeys-map", Long.class, (Class<List<Long>>) (Class<?>) List.class));
        
        // 初始化 ValueState 用于存储所有见过的 custkey
        custKeySet = getRuntimeContext().getState(
            new ValueStateDescriptor<>("custkey-set", (Class<Set<Long>>) (Class<?>) Set.class));
    }

    @Override
    public void processElement1(Tuple2<Long, String> tuple, Context context, Collector<Tuple2<Long, String>> collector) throws Exception {
        Long custKey = tuple.f0;
        String type = tuple.f1;
        
        // 获取当前的 custkey 集合
        Set<Long> currentCustKeys = custKeySet.value();
        if (currentCustKeys == null) {
            currentCustKeys = new HashSet<>();
        }
        
        // 检查 custkey 是否已存在，如果不存在则添加
        if (currentCustKeys.add(custKey)) {
            custKeySet.update(currentCustKeys);
        }
        
        // 如果 custkey 在 custKeyToOrderKeysMap 中，则流出对应的所有 orderkey
        List<Long> orderKeys = custKeyToOrderKeysMap.get(custKey);
        if (orderKeys != null && !orderKeys.isEmpty()) {
            for (Long orderKey : orderKeys) {
                collector.collect(new Tuple2<>(orderKey, type));
            }
        }
    }

    @Override
    public void processElement2(Tuple2<Order, String> orderTuple, Context context, Collector<Tuple2<Long, String>> collector) throws Exception {
        Order order = orderTuple.f0;
        String type = orderTuple.f1;
        
        // 获取订单日期
        LocalDate orderDate = order.getOOrderdate();
        
        // 检查订单日期是否在指定范围内
        if (orderDate.isAfter(START_DATE.minusDays(1)) && orderDate.isBefore(END_DATE)) {
            // 第一步：更新 custkey -> orderkey 映射
            List<Long> orderKeys = custKeyToOrderKeysMap.get(order.getOCustkey());
            if (orderKeys == null) {
                orderKeys = new ArrayList<>();
                orderKeys.add(order.getOOrderkey());
                custKeyToOrderKeysMap.put(order.getOCustkey(), orderKeys);
            } else {
                orderKeys.add(order.getOOrderkey());
                custKeyToOrderKeysMap.put(order.getOCustkey(), orderKeys);
            }
            
            // 第二步：检查 custkey 是否在 custKeySet 中
            Set<Long> currentCustKeys = custKeySet.value();
            if (currentCustKeys != null && currentCustKeys.contains(order.getOCustkey())) {
                collector.collect(new Tuple2<>(order.getOOrderkey(), type));
            }
        }
    }
} 