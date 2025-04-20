package com.example;

import com.example.model.Customer;
import com.example.model.DataRecord;
import com.example.model.Order;
import com.example.model.LineItem;
import com.example.serialization.LocalDateSerializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.example.process.CustomerProcessFunction;
import com.example.process.OrderProcessFunction;
import com.example.process.LineitemProcessFunction;
import com.example.process.ShipModeRevenueAggregationFunction;
import com.example.sink.ShipModeRevenueSink;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple2;

import java.time.Duration;
import java.time.LocalDate;

public class StreamProcessingJob {
    public static void main(String[] args) throws Exception {
        // 设置流执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 注册LocalDate序列化器
        System.out.println("正在注册LocalDate序列化器...");
        env.getConfig().registerTypeWithKryoSerializer(LocalDate.class, LocalDateSerializer.class);
        System.out.println("LocalDate序列化器注册完成");

        // 创建文件源
        System.out.println("正在创建文件源...");
        FileSource<String> source = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(), new org.apache.flink.core.fs.Path("input_data_all.csv"))
                .build();
        System.out.println("文件源创建完成");

        // 创建数据流
        System.out.println("正在创建数据流...");
        DataStream<String> inputStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "File Source");
        System.out.println("数据流创建完成");

        // 解析数据记录
        DataStream<DataRecord> dataRecords = inputStream
                .map(line -> {
                    DataRecord record = DataRecord.fromString(line);
                    return record;
                });

        // 过滤出客户数据并转换为Customer对象
        DataStream<Customer> customers = dataRecords
                .filter(record -> {
                    boolean isCustomer = record.getType().equals("+") && record.getTableType().equals("CU");
                    return isCustomer;
                })
                .map(record -> {
                    Customer customer = Customer.fromString(record.getData());
                    return customer;
                });

        // 过滤出订单数据并转换为Order对象
        DataStream<Order> orders = dataRecords
                .filter(record -> {
                    boolean isOrder = record.getType().equals("+") && record.getTableType().equals("OR");
                    return isOrder;
                })
                .map(record -> {
                    Order order = Order.fromString(record.getData());
                    return order;
                });

        // 过滤出订单项数据并转换为LineItem对象
        DataStream<LineItem> lineitems = dataRecords
                .filter(record -> {
                    boolean isLineitem = record.getType().equals("+") && record.getTableType().equals("LI");
                    return isLineitem;
                })
                .map(record -> {
                    LineItem lineitem = LineItem.fromString(record.getData());
                    return lineitem;
                });

        // 使用CustomerProcessFunction处理Customer对象
        DataStream<Tuple2<Long, String>> filteredCustomerKeys = customers
                .keyBy(Customer::getCCustkey)
                .process(new CustomerProcessFunction());

        // 使用OrderProcessFunction处理两个数据流
        DataStream<Tuple2<Long, String>> orderKeys = filteredCustomerKeys
                .connect(orders)
                .keyBy(
                    tuple -> tuple.f0,
                    order -> order.getOCustkey()
                )
                .process(new OrderProcessFunction());

        // 使用LineitemProcessFunction处理两个数据流
        DataStream<Tuple4<String, Double, Double, String>> lineitemResults = orderKeys
                .connect(lineitems)
                .keyBy(
                    tuple -> tuple.f0,
                    lineitem -> lineitem.getLOrderkey()
                )
                .process(new LineitemProcessFunction());

        // 使用ShipModeRevenueAggregationFunction处理lineitemResults数据流
        DataStream<Tuple2<String, Double>> shipModeRevenueResults = lineitemResults
                .keyBy(value -> value.f0) // 按shipMode分组
                .process(new ShipModeRevenueAggregationFunction())
                .keyBy(value -> value.f0) // 再次按shipMode分组
                .reduce((value1, value2) -> {
                    // 合并相同shipMode的结果，取最新的总收入
                    return new Tuple2<>(value1.f0, value2.f1);
                });
        
        // 使用自定义的 ShipModeRevenueSink 整合所有分区的结果
        shipModeRevenueResults.addSink(new ShipModeRevenueSink()).setParallelism(1);
        
        // 打印中间结果，用于调试
        // System.out.println("运输方式收入中间结果：");
        // shipModeRevenueResults.print();

        // 执行任务
        System.out.println("开始执行任务...");
        env.execute("Stream Processing Job");
        System.out.println("任务执行完成");
    }
} 