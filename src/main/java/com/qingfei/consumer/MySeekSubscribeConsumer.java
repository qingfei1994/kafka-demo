package com.qingfei.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Properties;
import java.util.Set;

/**
 * Created by ASUS on 12/29/2017.
 */
public class MySeekSubscribeConsumer {
    public static void main(String[] args) {
        Properties prop = new Properties();
        prop.put("bootstrap.servers","localhost:9092");
        prop.put("group.id", "group1");
        prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String,byte[]> consumer = new KafkaConsumer<String, byte[]>(prop);
        consumer.subscribe(Arrays.asList("seek_test"));
        //consumer.poll(1000);
        Set<TopicPartition> assignment = consumer.assignment();
        for(TopicPartition tp:assignment) {
            consumer.seek(tp,0);
        }
        while(true) {
            ConsumerRecords<String, byte[]> records = consumer.poll(1000);
            for (ConsumerRecord record : records) {
                System.out.println("My seek to Begin Consumer consume message partition:" + record.partition() + ",offset:" + record.offset() + "key:" + record.key() + ",value:" + record.value()+"timestamp:" + record.timestamp() + "timestampType:" + record.timestampType());
            }
        }
    }
}
