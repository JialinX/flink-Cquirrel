package com.example.process;

import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import com.example.model.Order;
import java.util.ArrayList;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class OrderProcessFunction extends KeyedCoProcessFunction<Long, Long, Order, Long> {
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
    public void processElement1(Long custKey, Context context, Collector<Long> collector) throws Exception {
        // 处理来自 filteredCustomerKeys 的数据流
        // System.out.println("OrderProcessFunction收到客户数据: custkey=" + custKey+ "    processElement1");
        
        // 获取当前的 custkey 集合
        Set<Long> currentCustKeys = custKeySet.value();
        if (currentCustKeys == null) {
            currentCustKeys = new HashSet<>();
        }
        
        // 检查 custkey 是否已存在，如果不存在则添加
        if (currentCustKeys.add(custKey)) {
            // System.out.println("新客户ID " + custKey + " 加入 custkey 集合" + "    processElement1");
            custKeySet.update(currentCustKeys);
        }
        
        // 如果 custkey 在 custKeyToOrderKeysMap 中，则流出对应的所有 orderkey
        List<Long> orderKeys = custKeyToOrderKeysMap.get(custKey);
        if (orderKeys != null && !orderKeys.isEmpty()) {
            System.out.println("找到客户 " + custKey + " 的所有订单: " + orderKeys + "    processElement1");
            for (Long orderKey : orderKeys) {
                collector.collect(orderKey);
                System.out.println("已流出订单数据: orderKey=" + orderKey);
            }
        } else {
            // System.out.println("客户 " + custKey + " 在 custKeyToOrderKeysMap 中不存在或订单列表为空" + "    processElement1");
        }
    }

    @Override
    public void processElement2(Order order, Context context, Collector<Long> collector) throws Exception {
        // 处理来自订单数据流的数据
        // System.out.println("OrderProcessFunction收到订单数据: orderKey=" + order.getOOrderkey() + ", custKey=" + order.getOCustkey() + ", orderDate=" + order.getOOrderdate());
        
        // 获取订单日期
        LocalDate orderDate = order.getOOrderdate();
        
        // 检查订单日期是否在指定范围内
        if (orderDate.isAfter(START_DATE.minusDays(1)) && orderDate.isBefore(END_DATE)) {
            // System.out.println("订单日期在指定范围内: " + order.getOOrderdate());
            
            // 第一步：更新 custkey -> orderkey 映射
            List<Long> orderKeys = custKeyToOrderKeysMap.get(order.getOCustkey());
            if (orderKeys == null) {
                // 如果 custkey 不存在，创建新的映射
                // System.out.println("客户 " + order.getOCustkey() + " 不在映射中，创建新的映射");
                orderKeys = new ArrayList<>();
                orderKeys.add(order.getOOrderkey());
                custKeyToOrderKeysMap.put(order.getOCustkey(), orderKeys);
                // System.out.println("新创建的订单列表: " + orderKeys);
            } else {
                // 如果 custkey 已存在，更新 orderkey 列表
                // System.out.println("更新客户 " + order.getOCustkey() + " 的订单列表");
                orderKeys.add(order.getOOrderkey());
                custKeyToOrderKeysMap.put(order.getOCustkey(), orderKeys);
                // System.out.println("更新后的订单列表: " + orderKeys);
            }
            
            // 第二步：检查 custkey 是否在 custKeySet 中
            Set<Long> currentCustKeys = custKeySet.value();
            if (currentCustKeys != null && currentCustKeys.contains(order.getOCustkey())) {
                // 如果 custkey 在集合中，流出这个 orderkey
                // System.out.println("客户 " + order.getOCustkey() + " 在 custKeySet 中，流出订单: " + order.getOOrderkey());
                collector.collect(order.getOOrderkey());
                System.out.println("已流出订单数据: orderKey=" + order.getOOrderkey() + ", custKey=" + order.getOCustkey());
            } else {
                // System.out.println("客户 " + order.getOCustkey() + " 不在 custKeySet 中，不流出订单");
            }
        } else {
            // 订单日期不在指定范围内，忽略
            // System.out.println("订单日期不在指定范围内，忽略: " + order.getOOrderdate());
        }
    }
} 