package com.example.process;

import com.example.model.Customer;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class CustomerProcessFunction extends KeyedProcessFunction<Long, Customer, Long> {
    @Override
    public void processElement(Customer customer, Context context, Collector<Long> out) throws Exception {
        // System.out.println("CustomerProcessFunction处理数据: custkey=" + customer.getCCustkey() 
            // + ", mktsegment=" + customer.getCMktsegment());
            
        if ("AUTOMOBILE".equals(customer.getCMktsegment())) {
            // System.out.println("发现AUTOMOBILE客户，输出custkey: " + customer.getCCustkey());
            out.collect(customer.getCCustkey());
        }
    }
} 