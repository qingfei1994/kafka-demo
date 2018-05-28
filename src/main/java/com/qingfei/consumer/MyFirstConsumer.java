package com.qingfei.consumer;

import kafka.Kafka;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created by ASUS on 12/24/2017.
 */
public class MyFirstConsumer {
    public static void main(String[] args) {
        Properties kafkaProp = new Properties();
        kafkaProp.put("bootstrap.servers", "192.168.0.110:9092");
        kafkaProp.put("group.id", "1");
        kafkaProp.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProp.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String,byte[]> consumer = new KafkaConsumer<String, byte[]>(kafkaProp);
        consumer.subscribe(Arrays.asList("haiqingchen"));
        try {
            while (true) {
                ConsumerRecords<String,byte[]> consumerRecords = consumer.poll(100);
                for (ConsumerRecord<String,byte[]> record:consumerRecords) {
                    System.out.println("MyFirstConsumer's consumption message:partition"+record.partition()+",offset:"+record.offset()+",key="+record.key()+",value="+record.value());

                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }
}
