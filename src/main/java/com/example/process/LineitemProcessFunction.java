package com.example.process;

import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import com.example.model.LineItem;
import java.util.ArrayList;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class LineitemProcessFunction extends KeyedCoProcessFunction<Long, Long, LineItem, Tuple4<String, Double, Double, String>> {
    // 使用 MapState 存储 orderkey 到 (l_shipmode, l_extendedprice, l_discount) 的映射
    private MapState<Long, List<Tuple3<String, Double, Double>>> orderKeyToLineitemInfoMap;
    
    // 使用 ValueState<Set<Long>> 存储所有见过的 orderkey
    private ValueState<Set<Long>> orderKeySet;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 初始化 MapState，key 是 orderkey，value 是 Tuple3 列表
        orderKeyToLineitemInfoMap = getRuntimeContext().getMapState(
            new MapStateDescriptor<>("orderkey-to-lineiteminfo-map", Long.class, (Class<List<Tuple3<String, Double, Double>>>) (Class<?>) List.class));
        
        // 初始化 ValueState 用于存储所有见过的 orderkey
        orderKeySet = getRuntimeContext().getState(
            new ValueStateDescriptor<>("orderkey-set", (Class<Set<Long>>) (Class<?>) Set.class));
    }

    @Override
    public void processElement1(Long orderKey, Context context, Collector<Tuple4<String, Double, Double, String>> collector) throws Exception {
        // 处理来自 orderKeys 的数据流
        // System.out.println("LineitemProcessFunction收到订单数据: orderKey=" + orderKey);
        
        // 获取当前的 orderkey 集合
        Set<Long> currentOrderKeys = orderKeySet.value();
        if (currentOrderKeys == null) {
            currentOrderKeys = new HashSet<>();
        }
        
        // 检查 orderkey 是否已存在，如果不存在则添加
        if (currentOrderKeys.add(orderKey)) {
            // System.out.println("新订单ID " + orderKey + " 加入 orderkey 集合");
            orderKeySet.update(currentOrderKeys);
        }
        
        // 获取当前 orderkey 对应的 lineitem 信息列表
        List<Tuple3<String, Double, Double>> lineitemInfos = orderKeyToLineitemInfoMap.get(orderKey);
        
        if (lineitemInfos != null && !lineitemInfos.isEmpty()) {
            // 如果 orderkey 在映射中且列表不为空，输出所有的 lineitem
            // System.out.println("找到订单 " + orderKey + " 的所有订单项信息: " + lineitemInfos.size() + " 条");
            for (Tuple3<String, Double, Double> info : lineitemInfos) {
                // System.out.println("流出数据: 订单项信息: shipMode=" + info.f0 + ", extendedPrice=" + info.f1 + ", discount=" + info.f2);
                collector.collect(new Tuple4<>(info.f0, info.f1, info.f2, "+"));
            }
        } else {
            // 如果这个 orderkey 对应的列表为空，什么也不做
            // System.out.println("订单 " + orderKey + " 的订单项信息列表为空，不执行任何操作");
        }
    }

    @Override
    public void processElement2(LineItem lineitem, Context context, Collector<Tuple4<String, Double, Double, String>> collector) throws Exception {
        // 获取需要的字段
        Long orderKey = lineitem.getLOrderkey();
        String shipMode = lineitem.getLShipmode();
        double extendedPrice = lineitem.getLExtendedprice();
        double discount = lineitem.getLDiscount();

         // 处理来自 lineitem 数据流的数据
        // System.out.println("LineitemProcessFunction收到订单项数据: orderKey=" + orderKey + "shipMode" + shipMode + "extendedPrice" + extendedPrice + "discount" + discount);
        
        // 判断 l_shipmode 是否在 ('RAIL', 'AIR', 'TRUCK') 中
        if (!shipMode.equals("RAIL") && !shipMode.equals("AIR") && !shipMode.equals("TRUCK")) {
            // System.out.println("忽略不符合要求的 shipmode: " + shipMode);
            return;
        }
        
        // 第一步：将数据存入 orderKeyToLineitemInfoMap
        // 创建 Tuple3 对象
        Tuple3<String, Double, Double> info = new Tuple3<>(shipMode, extendedPrice, discount);
        
        // 获取当前 orderkey 对应的 lineitem 信息列表
        List<Tuple3<String, Double, Double>> lineitemInfos = orderKeyToLineitemInfoMap.get(orderKey);
        
        if (lineitemInfos == null) {
            // 如果这个 orderkey 不在映射中，创建一个新的空列表并加入映射
            // System.out.println("新订单ID " + orderKey + " 加入映射，初始订单项信息列表为空");
            lineitemInfos = new ArrayList<>();
            lineitemInfos.add(info);
            orderKeyToLineitemInfoMap.put(orderKey, lineitemInfos);
            // System.out.println("新创建的订单项信息列表: " + lineitemInfos.size() + " 条");
        } else {
            // 如果这个 orderkey 在映射中，将新的 lineitem 信息加入列表
            // System.out.println("更新订单 " + orderKey + " 的订单项信息列表");
            lineitemInfos.add(info);
            orderKeyToLineitemInfoMap.put(orderKey, lineitemInfos);
            // System.out.println("更新后的订单项信息列表: " + lineitemInfos.size() + " 条");
        }
        
        // 第二步：判断 orderkey 是否在 orderKeySet 中
        Set<Long> currentOrderKeys = orderKeySet.value();
        if (currentOrderKeys != null && currentOrderKeys.contains(orderKey)) {
            // 如果 orderkey 在集合中，流出数据
            // System.out.println("订单 " + orderKey + " 在 orderKeySet 中，流出数据: shipmode=" + shipMode + ", extendedPrice=" + extendedPrice + ", discount=" + discount);
            collector.collect(new Tuple4<>(info.f0, info.f1, info.f2, "+"));
        } else {
            // System.out.println("订单 " + orderKey + " 不在 orderKeySet 中，不流出数据");
        }
    }
} 