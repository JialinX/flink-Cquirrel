package com.example;

import com.example.model.*;
import com.example.process.*;
import com.example.serialization.LocalDateSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import java.time.LocalDate;

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

        // 输出结果
        customerKeys.print();

        env.execute("Stream Processing Job");
    }
} 