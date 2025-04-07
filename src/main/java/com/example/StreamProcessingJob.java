package com.example;

import com.example.model.*;
import com.example.process.*;
import com.example.serialization.LocalDateSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.ExecutionConfig;
import java.time.LocalDate;
import java.util.Map;
import java.util.HashMap;

public class StreamProcessingJob {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 注册LocalDate序列化器
        env.getConfig().registerTypeWithKryoSerializer(LocalDate.class, LocalDateSerializer.class);

        // 读取输入数据流
        DataStream<String> inputStream = env.readTextFile("input_data_all.csv");

        // 解析数据记录
        DataStream<DataRecord> dataRecords = inputStream
            .map(DataRecord::fromString);

        // 处理Customer数据
        DataStream<Long> customerKeys = dataRecords
            .filter(record -> record.getType().equals("+") && record.getTableType().equals("CU"))
            .map(record -> Customer.fromString(record.getData()))
            .keyBy(Customer::getCCustkey)
            .process(new CustomerProcessFunction());

        // 处理Order数据
        DataStream<Long> orderKeys = dataRecords
            .filter(record -> record.getType().equals("+") && record.getTableType().equals("OR"))
            .map(record -> Order.fromString(record.getData()))
            .keyBy(Order::getOCustkey)
            .process(new OrderProcessFunction());

        // 合并来自Customer和Order的orderKeys
        DataStream<Long> allOrderKeys = customerKeys.union(orderKeys);

        // 处理LineItem数据
        DataStream<LineItem> lineItems = dataRecords
            .filter(record -> record.getType().equals("+") && record.getTableType().equals("LI"))
            .map(record -> LineItem.fromString(record.getData()))
            .keyBy(LineItem::getLOrderkey)
            .process(new LineItemProcessFunction());

        // 处理来自Order的LineItem查询
        DataStream<LineItem> queriedLineItems = allOrderKeys
            .keyBy(orderKey -> orderKey)
            .process(new OrderKeyProcessFunction());

        // 合并所有LineItem数据
        DataStream<LineItem> allLineItems = lineItems.union(queriedLineItems);

        // 聚合计算
        DataStream<Map<String, Double>> revenueByShipmode = allLineItems
            .keyBy(LineItem::getLShipmode)
            .process(new AggregationProcessFunction());

        // 添加一个全局统计的 ProcessFunction
        DataStream<Map<String, Double>> totalRevenueByShipmode = revenueByShipmode
            .keyBy(map -> 1L)  // 使用常量key确保所有数据进入同一个key group
            .process(new ProcessFunction<Map<String, Double>, Map<String, Double>>() {
                private Map<String, Double> totals = new HashMap<>();

                @Override
                public void processElement(Map<String, Double> value, Context ctx, Collector<Map<String, Double>> out) throws Exception {
                    // 更新每个运输方式的总和
                    value.forEach((shipmode, revenue) -> 
                        totals.merge(shipmode, revenue, Double::sum));
                    
                    // 输出当前的总和
                    out.collect(new HashMap<>(totals));
                }
            });

        // 输出结果
        totalRevenueByShipmode.print();

        env.execute("Stream Processing Job");
    }
} 