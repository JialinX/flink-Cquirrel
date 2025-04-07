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
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
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
            .map(DataRecord::fromString)
            .returns(TypeInformation.of(DataRecord.class));

        // 处理Customer数据
        DataStream<Long> customerKeys = dataRecords
            .filter(record -> record.getType().equals("+") && record.getTableType().equals("CU"))
            .map(record -> Customer.fromString(record.getData()))
            .returns(TypeInformation.of(Customer.class))
            .keyBy(Customer::getCCustkey)
            .process(new CustomerProcessFunction())
            .returns(Types.LONG);

        // 处理Order数据
        DataStream<Order> orderStream = dataRecords
            .filter(record -> record.getType().equals("+") && record.getTableType().equals("OR"))
            .map(record -> Order.fromString(record.getData()))
            .returns(TypeInformation.of(Order.class));

        DataStream<Long> orderKeys = orderStream
            .keyBy(Order::getOCustkey)
            .process(new OrderProcessFunction())
            .returns(Types.LONG);

        // 合并来自Customer和Order的orderKeys
        DataStream<Long> allOrderKeys = customerKeys.union(orderKeys);

        // 处理LineItem数据
        DataStream<LineItem> lineItemStream = dataRecords
            .filter(record -> record.getType().equals("+") && record.getTableType().equals("LI"))
            .map(record -> LineItem.fromString(record.getData()))
            .returns(TypeInformation.of(LineItem.class));

        // 将LineItem转换为Tuple4格式
        TypeInformation<Tuple4<Long, String, Double, Double>> tupleTypeInfo = 
            new TupleTypeInfo<>(Types.LONG, Types.STRING, Types.DOUBLE, Types.DOUBLE);

        DataStream<Tuple4<Long, String, Double, Double>> lineItems = lineItemStream
            .map(item -> new Tuple4<>(
                item.getLOrderkey(),
                item.getLShipmode(),
                item.getLExtendedprice(),
                item.getLDiscount()
            ))
            .returns(tupleTypeInfo)
            .keyBy(item -> item.f0)  // orderkey
            .process(new LineItemProcessFunction())
            .returns(tupleTypeInfo);

        // 处理来自Order的LineItem查询
        DataStream<Tuple4<Long, String, Double, Double>> queriedLineItems = allOrderKeys
            .map(key -> new Tuple4<>(key, "", 0.0, 0.0))
            .returns(tupleTypeInfo)
            .keyBy(item -> item.f0)  // orderkey
            .process(new LineItemProcessFunction())
            .returns(tupleTypeInfo);

        // 合并所有LineItem数据
        DataStream<Tuple4<Long, String, Double, Double>> allLineItems = lineItems.union(queriedLineItems);

        // 聚合计算
        MapTypeInfo<String, Double> mapTypeInfo = new MapTypeInfo<>(Types.STRING, Types.DOUBLE);

        DataStream<Map<String, Double>> revenueByShipmode = allLineItems
            .keyBy(item -> item.f1)  // shipmode
            .process(new AggregationProcessFunction())
            .returns(mapTypeInfo);

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
            })
            .returns(mapTypeInfo);

        // 输出结果
        totalRevenueByShipmode.print();

        env.execute("Stream Processing Job");
    }
} 