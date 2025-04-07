package com.example.process;

import com.example.model.Order;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.time.LocalDate;

public class OrderProcessFunction extends KeyedProcessFunction<Long, Order, Long> {
    // 存储 custkey -> orderkey 的映射
    private MapState<Long, Set<Long>> orderState;
    // 存储 custkey 的集合
    private MapState<Long, Boolean> customerKeyState;
    
    private static final LocalDate START_DATE = LocalDate.parse("1995-01-01");
    private static final LocalDate END_DATE = LocalDate.parse("1996-12-31");

    @Override
    public void open(Configuration parameters) throws Exception {
        // 初始化 orderState
        MapStateDescriptor<Long, Set<Long>> orderDescriptor = 
            new MapStateDescriptor<>("order-state", 
                TypeInformation.of(Long.class),
                TypeInformation.of(new TypeHint<Set<Long>>() {}));
        orderState = getRuntimeContext().getMapState(orderDescriptor);

        // 初始化 customerKeyState
        MapStateDescriptor<Long, Boolean> customerKeyDescriptor = 
            new MapStateDescriptor<>("customerkey-state", 
                TypeInformation.of(Long.class),
                TypeInformation.of(Boolean.class));
        customerKeyState = getRuntimeContext().getMapState(customerKeyDescriptor);
    }

    @Override
    public void processElement(Order order, Context context, Collector<Long> out) throws Exception {
        LocalDate orderDate = order.getOOrderdate();
        
        if (orderDate.isAfter(START_DATE) && orderDate.isBefore(END_DATE)) {
            Long customerKey = order.getOCustkey();
            Long orderKey = order.getOOrderkey();

            // 存储 order 数据
            Set<Long> orderKeys = orderState.get(customerKey);
            if (orderKeys == null) {
                orderKeys = new HashSet<>();
            }
            orderKeys.add(orderKey);
            orderState.put(customerKey, orderKeys);

            // 检查是否有匹配的 custkey
            if (customerKeyState.get(customerKey) != null) {
                out.collect(orderKey);
            }
        }
    }

    public void processCustomerKey(Long customerKey, Context context, Collector<Long> out) throws Exception {
        customerKeyState.put(customerKey, true);
        
        Set<Long> orderKeys = orderState.get(customerKey);
        if (orderKeys != null) {
            for (Long orderKey : orderKeys) {
                out.collect(orderKey);
            }
        }
    }
} 