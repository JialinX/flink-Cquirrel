package com.example.process;

import com.example.model.LineItem;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import java.util.ArrayList;
import java.util.List;

public class OrderKeyProcessFunction extends ProcessFunction<Long, LineItem> {
    private MapState<Long, List<LineItem>> lineItemState;
    private static final List<String> VALID_SHIPMODES = List.of("RAIL", "AIR", "TRUCK");

    @Override
    public void open(Configuration parameters) throws Exception {
        MapStateDescriptor<Long, List<LineItem>> descriptor = 
            new MapStateDescriptor<>("lineitem-state", 
                Types.LONG,
                Types.LIST(Types.GENERIC(LineItem.class)));
        lineItemState = getRuntimeContext().getMapState(descriptor);
    }

    @Override
    public void processElement(Long orderKey, Context context, Collector<LineItem> out) throws Exception {
        List<LineItem> items = lineItemState.get(orderKey);
        if (items != null) {
            for (LineItem item : items) {
                if (VALID_SHIPMODES.contains(item.getLShipmode())) {
                    out.collect(item);
                }
            }
        }
    }
} 