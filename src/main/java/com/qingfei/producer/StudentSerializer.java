package com.qingfei.producer;

import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

class Student {


}

public class StudentSerializer implements Serializer<Student> {

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public byte[] serialize(String s, Student student) {
        return new byte[0];
    }

    @Override
    public void close() {

    }
}
