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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.functions.MapFunction;

import java.time.Duration;
import java.time.LocalDate;

public class StreamProcessingJob {
    public static void main(String[] args) throws Exception {
        // Set up streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Register LocalDate serializer
        System.out.println("Registering LocalDate serializer...");
        env.getConfig().registerTypeWithKryoSerializer(LocalDate.class, LocalDateSerializer.class);
        System.out.println("LocalDate serializer registration completed");

        // Create file source
        System.out.println("Creating file source...");
        FileSource<String> source = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(), new org.apache.flink.core.fs.Path("file://Users/jialinxie/Desktop/flink/input_data_all.csv"))
                .build();
        System.out.println("File source creation completed");

        // Create data stream
        System.out.println("Creating data stream...");
        DataStream<String> inputStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "File Source");
        System.out.println("Data stream creation completed");

        // Parse data records
        DataStream<DataRecord> dataRecords = inputStream
                .map(line -> {
                    DataRecord record = DataRecord.fromString(line);
                    return record;
                });

        // Filter customer data and convert to Customer objects
        DataStream<Tuple2<Customer, String>> customers = dataRecords
                .filter(record -> record.getTableType().equals("CU"))
                .map(new MapFunction<DataRecord, Tuple2<Customer, String>>() {
                    @Override
                    public Tuple2<Customer, String> map(DataRecord record) throws Exception {
                        Customer customer = Customer.fromString(record.getData());
                        return new Tuple2<>(customer, record.getType());
                    }
                });

        // Filter order data and convert to Order objects
        DataStream<Tuple2<Order, String>> orders = dataRecords
                .filter(record -> record.getTableType().equals("OR"))
                .map(new MapFunction<DataRecord, Tuple2<Order, String>>() {
                    @Override
                    public Tuple2<Order, String> map(DataRecord record) throws Exception {
                        Order order = Order.fromString(record.getData());
                        return new Tuple2<>(order, record.getType());
                    }
                });

        // Filter line item data and convert to LineItem objects
        DataStream<Tuple2<LineItem, String>> lineitems = dataRecords
                .filter(record -> record.getTableType().equals("LI"))
                .map(new MapFunction<DataRecord, Tuple2<LineItem, String>>() {
                    @Override
                    public Tuple2<LineItem, String> map(DataRecord record) throws Exception {
                        LineItem lineitem = LineItem.fromString(record.getData());
                        return new Tuple2<>(lineitem, record.getType());
                    }
                });

        // Process Customer objects using CustomerProcessFunction
        DataStream<Tuple2<Long, String>> filteredCustomerKeys = customers
                .keyBy(tuple -> tuple.f0.getCCustkey())
                .process(new CustomerProcessFunction());

        // Process two data streams using OrderProcessFunction
        DataStream<Tuple2<Long, String>> orderKeys = filteredCustomerKeys
                .connect(orders)
                .keyBy(
                    tuple -> tuple.f0,
                    order -> order.f0.getOCustkey()
                )
                .process(new OrderProcessFunction());

        // Process two data streams using LineitemProcessFunction
        DataStream<Tuple4<String, Double, Double, String>> lineitemResults = orderKeys
                .connect(lineitems)
                .keyBy(
                    tuple -> tuple.f0,
                    lineitem -> lineitem.f0.getLOrderkey()
                )
                .process(new LineitemProcessFunction());

        // Process lineitemResults using ShipModeRevenueAggregationFunction
        DataStream<Tuple2<String, Double>> shipModeRevenueResults = lineitemResults
                .keyBy(value -> value.f0) // Group by shipMode
                .process(new ShipModeRevenueAggregationFunction())
                .keyBy(value -> value.f0) // Group by shipMode again
                .reduce((value1, value2) -> {
                    // Merge results with same shipMode, take the latest total revenue
                    return new Tuple2<>(value1.f0, value2.f1);
                });
        
        // Use custom ShipModeRevenueSink to integrate results from all partitions
        shipModeRevenueResults.addSink(new ShipModeRevenueSink()).setParallelism(1);
        
        // Print intermediate results for debugging
        // System.out.println("Transport mode revenue intermediate results:");
        // shipModeRevenueResults.print();

        // Execute the job
        System.out.println("Starting job execution...");
        env.execute("Stream Processing Job");
        System.out.println("Job execution completed");
    }
} 