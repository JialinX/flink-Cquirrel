package com.example.process;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import java.util.HashMap;
import java.util.Map;

public class OrderProcessFunction extends ProcessFunction<Long, Long> {
    // 使用HashMap存储custkey到orderkey的映射
    private final Map<Long, Long> custKeyToOrderKeyMap = new HashMap<>();

    @Override
    public void processElement(Long custKey, Context context, Collector<Long> collector) throws Exception {
        // 打印流入的数据
        System.out.println("OrderProcessFunction收到数据: custKey=" + custKey);
        System.out.println("当前映射大小: " + custKeyToOrderKeyMap.size());

        if (custKeyToOrderKeyMap.containsKey(custKey)) {
            // 如果custkey在映射中存在，输出对应的orderkey
            Long orderKey = custKeyToOrderKeyMap.get(custKey);
            System.out.println("找到已存在的映射: custKey=" + custKey + ", orderKey=" + orderKey);
            if (orderKey != null) {
                collector.collect(orderKey);
            }
        } else {
            // 如果custkey不在映射中，将其添加到映射中，orderkey设为null
            custKeyToOrderKeyMap.put(custKey, null);
            // 打印插入信息
            System.out.println("新客户ID " + custKey + " 已被插入映射中，对应的订单ID为null");
            System.out.println("更新后的映射大小: " + custKeyToOrderKeyMap.size());
        }
    }
} 