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
import org.apache.flink.api.java.tuple.Tuple3;
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
        DataStream<Long> filteredCustomerKeys = customers
                .keyBy(Customer::getCCustkey)
                .process(new CustomerProcessFunction());

        // 使用OrderProcessFunction处理两个数据流
        DataStream<Long> orderKeys = filteredCustomerKeys
                .connect(orders)
                .keyBy(
                    custKey -> custKey,
                    order -> order.getOCustkey()
                )
                .process(new OrderProcessFunction());

        // 使用LineitemProcessFunction处理两个数据流
        DataStream<Tuple3<String, Double, Double>> lineitemResults = orderKeys
                .connect(lineitems)
                .keyBy(
                    orderKey -> orderKey,
                    lineitem -> lineitem.getLOrderkey()
                )
                .process(new LineitemProcessFunction());

        // 使用ShipModeRevenueAggregationFunction处理lineitemResults数据流
        DataStream<Tuple2<String, Double>> shipModeRevenueResults = lineitemResults
                .keyBy(value -> value.f0) // 按shipMode分组
                .process(new ShipModeRevenueAggregationFunction())
                ;
        
        System.out.println("运输方式最终收入结果：");
        shipModeRevenueResults.print();

        // 执行任务
        System.out.println("开始执行任务...");
        env.execute("Stream Processing Job");
        System.out.println("任务执行完成");
    }
} 