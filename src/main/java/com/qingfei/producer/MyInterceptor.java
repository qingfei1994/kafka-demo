package com.qingfei.producer;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

public class MyInterceptor implements ProducerInterceptor {
    private long success = 0;
    private long fail = 0;
    @Override
    public ProducerRecord onSend(ProducerRecord producerRecord) {
        String modifiedValue = "prefix1-" + producerRecord.value();
        return new ProducerRecord(producerRecord.topic(),producerRecord.partition(),modifiedValue);
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        if(e==null) {
            success++;
        } else {
            fail++;
        }
    }

    @Override
    public void close() {
        double successRatio = (double) success /(success+fail);
        System.out.println(String.format("success = %d, fail = %d", success,fail));
    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
