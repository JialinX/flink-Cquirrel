package com.example;

import com.example.model.Customer;
import com.example.model.DataRecord;
import com.example.serialization.LocalDateSerializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import com.example.process.CustomerProcessFunction;
import com.example.process.OrderProcessFunction;

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

        // 打印原始数据并确保数据流被消费
        System.out.println("开始处理数据流...");
        // inputStream.map(line -> {
        //     System.out.println("原始数据: " + line);
        //     return line;
        // }).print();  // 添加print()确保数据流被消费

        // 解析数据记录
        DataStream<DataRecord> dataRecords = inputStream
                .map(line -> {
                    DataRecord record = DataRecord.fromString(line);
                    // System.out.println("解析后的数据记录: type=" + record.getType() + ", tableType=" + record.getTableType());
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

        // 使用CustomerProcessFunction处理Customer对象
        DataStream<Long> filteredCustomerKeys = customers
                .keyBy(Customer::getCCustkey)
                .process(new CustomerProcessFunction());

        // 使用OrderProcessFunction处理过滤后的客户ID
        DataStream<Long> orderKeys = filteredCustomerKeys
                .keyBy(key -> key)
                .process(new OrderProcessFunction());

        // 打印结果
        orderKeys.print();

        // 执行任务
        System.out.println("开始执行任务...");
        env.execute("Stream Processing Job");
        System.out.println("任务执行完成");
    }
} 