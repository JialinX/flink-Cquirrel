package com.example.process;

import com.example.model.Order;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

public class OrderProcessFunction extends ProcessFunction<Order, Long> {
    private MapState<Long, List<Long>> orderState; // custkey -> list of orderkeys
    private static final LocalDate START_DATE = LocalDate.parse("1995-01-01");
    private static final LocalDate END_DATE = LocalDate.parse("1996-01-01");

    @Override
    public void open(Configuration parameters) throws Exception {
        MapStateDescriptor<Long, List<Long>> descriptor = 
            new MapStateDescriptor<>("order-state", 
                Types.LONG,
                Types.LIST(Types.LONG));
        orderState = getRuntimeContext().getMapState(descriptor);
    }

    @Override
    public void processElement(Order order, Context context, Collector<Long> out) throws Exception {
        if (order.getOOrderdate().isAfter(START_DATE) && 
            order.getOOrderdate().isBefore(END_DATE)) {
            
            List<Long> orderKeys = orderState.get(order.getOCustkey());
            if (orderKeys == null) {
                orderKeys = new ArrayList<>();
            }
            orderKeys.add(order.getOOrderkey());
            orderState.put(order.getOCustkey(), orderKeys);
            
            out.collect(order.getOOrderkey());
        }
    }

    public void processCustomerKey(Long custKey, Collector<Long> out) throws Exception {
        List<Long> orderKeys = orderState.get(custKey);
        if (orderKeys != null) {
            for (Long orderKey : orderKeys) {
                out.collect(orderKey);
            }
        }
    }
} 