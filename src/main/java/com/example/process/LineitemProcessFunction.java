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
import org.apache.flink.api.java.tuple.Tuple2;
import com.example.model.LineItem;
import java.util.ArrayList;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class LineitemProcessFunction extends KeyedCoProcessFunction<Long, Tuple2<Long, String>, Tuple2<LineItem, String>, Tuple4<String, Double, Double, String>> {
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
    public void processElement1(Tuple2<Long, String> tuple, Context context, Collector<Tuple4<String, Double, Double, String>> collector) throws Exception {
        Long orderKey = tuple.f0;
        String type = tuple.f1;
        
        // 获取当前的 orderkey 集合
        Set<Long> currentOrderKeys = orderKeySet.value();
        if (currentOrderKeys == null) {
            currentOrderKeys = new HashSet<>();
        }
        
        // 检查 orderkey 是否已存在，如果不存在则添加
        if (currentOrderKeys.add(orderKey)) {
            orderKeySet.update(currentOrderKeys);
        }
        
        // 获取当前 orderkey 对应的 lineitem 信息列表
        List<Tuple3<String, Double, Double>> lineitemInfos = orderKeyToLineitemInfoMap.get(orderKey);
        
        if (lineitemInfos != null && !lineitemInfos.isEmpty()) {
            for (Tuple3<String, Double, Double> info : lineitemInfos) {
                collector.collect(new Tuple4<>(info.f0, info.f1, info.f2, type));
            }
        }
    }

    @Override
    public void processElement2(Tuple2<LineItem, String> lineitemTuple, Context context, Collector<Tuple4<String, Double, Double, String>> collector) throws Exception {
        LineItem lineitem = lineitemTuple.f0;
        String type = lineitemTuple.f1;
        
        // 获取需要的字段
        Long orderKey = lineitem.getLOrderkey();
        String shipMode = lineitem.getLShipmode();
        double extendedPrice = lineitem.getLExtendedprice();
        double discount = lineitem.getLDiscount();
        
        // 判断 l_shipmode 是否在 ('RAIL', 'AIR', 'TRUCK') 中
        if (!shipMode.equals("RAIL") && !shipMode.equals("AIR") && !shipMode.equals("TRUCK")) {
            return;
        }
        
        // 第一步：将数据存入 orderKeyToLineitemInfoMap
        // 创建 Tuple3 对象
        Tuple3<String, Double, Double> info = new Tuple3<>(shipMode, extendedPrice, discount);
        
        // 获取当前 orderkey 对应的 lineitem 信息列表
        List<Tuple3<String, Double, Double>> lineitemInfos = orderKeyToLineitemInfoMap.get(orderKey);
        
        if (lineitemInfos == null) {
            lineitemInfos = new ArrayList<>();
            lineitemInfos.add(info);
            orderKeyToLineitemInfoMap.put(orderKey, lineitemInfos);
        } else {
            lineitemInfos.add(info);
            orderKeyToLineitemInfoMap.put(orderKey, lineitemInfos);
        }
        
        // 第二步：判断 orderkey 是否在 orderKeySet 中
        Set<Long> currentOrderKeys = orderKeySet.value();
        if (currentOrderKeys != null && currentOrderKeys.contains(orderKey)) {
            collector.collect(new Tuple4<>(info.f0, info.f1, info.f2, type));
        }
    }
} 