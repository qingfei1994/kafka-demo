package com.qingfei.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.FileOutputStream;
import java.util.Arrays;
import java.util.Properties;

/**
 * Created by ASUS on 1/7/2018.
 */
public class MyImageConsumer {
    public static void main(String[] args) {
        Properties prop = new Properties();
        prop.put("bootstrap.servers","192.168.0.110:9092");
        prop.put("group.id","group1");
        prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        KafkaConsumer<String,byte[]> consumer = new KafkaConsumer<String, byte[]>(prop);
        consumer.subscribe(Arrays.asList("haiqingchen"));
        try {
            while (true) {
                ConsumerRecords<String, byte[]> records = consumer.poll(1000);
                for (ConsumerRecord<String, byte[]> record : records) {
                    System.out.println("partition=" + record.partition() + ",offset=" + record.offset() + ",key=" + record.key() + ",value=" + record.value());
                    String filename = record.key();
                    byte[] message = record.value();
                    FileOutputStream fos = new FileOutputStream("/usr/local/kafka_consumer/" + filename, false);
                    fos.write(message);
                    fos.flush();
                    fos.close();
                }
            }
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }
}
