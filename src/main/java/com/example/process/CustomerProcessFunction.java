package com.example.process;

import com.example.model.Customer;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple2;

public class CustomerProcessFunction extends KeyedProcessFunction<Long, Tuple2<Customer, String>, Tuple2<Long, String>> {
    @Override
    public void processElement(Tuple2<Customer, String> customerTuple, Context context, Collector<Tuple2<Long, String>> out) throws Exception {
        Customer customer = customerTuple.f0;
        String type = customerTuple.f1;
            
        if ("AUTOMOBILE".equals(customer.getCMktsegment())) {
            // System.out.println("发现AUTOMOBILE客户，输出custkey: " + customer.getCCustkey());
            out.collect(new Tuple2<>(customer.getCCustkey(), type));
        }
    }
} 