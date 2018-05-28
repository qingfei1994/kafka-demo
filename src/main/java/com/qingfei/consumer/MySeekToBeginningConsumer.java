package com.qingfei.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created by ASUS on 12/29/2017.
 */
public class MySeekToBeginningConsumer {
    public static void main(String[] args) {
        Properties prop = new Properties();
        prop.put("bootstrap.servers","192.168.0.110:9092");
        prop.put("group.id", "group1");
        prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //第一个参数是topic name,第二个参数是partition
        TopicPartition seekToBegin = new TopicPartition("haiqingchen",1);
        KafkaConsumer<String,byte[]> consumer = new KafkaConsumer<String, byte[]>(prop);
        consumer.assign(Arrays.asList(seekToBegin));
        consumer.seekToBeginning(Arrays.asList(seekToBegin));
        ConsumerRecords<String,byte[]> records=consumer.poll(1000);
        for (ConsumerRecord record:records) {
            System.out.println("My seek to Begin Consumer consume message partition:"+record.partition()+",offset:"+record.offset()+"key:"+record.key()+",value:"+record.value());
        }
    }
}
