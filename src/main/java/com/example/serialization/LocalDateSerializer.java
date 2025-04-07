package com.example.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.time.LocalDate;

public class LocalDateSerializer extends Serializer<LocalDate> {
    @Override
    public void write(Kryo kryo, Output output, LocalDate date) {
        output.writeInt(date.getYear());
        output.writeInt(date.getMonthValue());
        output.writeInt(date.getDayOfMonth());
    }

    @Override
    public LocalDate read(Kryo kryo, Input input, Class<LocalDate> type) {
        int year = input.readInt();
        int month = input.readInt();
        int day = input.readInt();
        return LocalDate.of(year, month, day);
    }
} 