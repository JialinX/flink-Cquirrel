package com.example.process;

import com.example.model.Customer;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;

public class CustomerProcessFunction extends ProcessFunction<Customer, Long> {
    private ValueState<Boolean> processedState;

    @Override
    public void open(Configuration parameters) throws Exception {
        processedState = getRuntimeContext().getState(
            new ValueStateDescriptor<>("processed-state", Boolean.class));
    }

    @Override
    public void processElement(Customer customer, Context context, Collector<Long> out) throws Exception {
        System.out.println("CustomerProcessFunction处理数据: custkey=" + customer.getCCustkey() 
            + ", mktsegment=" + customer.getCMktsegment());
            
        if ("AUTOMOBILE".equals(customer.getCMktsegment())) {
            System.out.println("发现AUTOMOBILE客户，输出custkey: " + customer.getCCustkey());
            out.collect(customer.getCCustkey());
        }
    }
} 