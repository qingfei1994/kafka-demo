package com.qingfei.producer;

import kafka.Kafka;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

/**
 * Created by ASUS on 1/7/2018.
 */
public class MyImageProducer {
    public static void main(String[] args) {
        Properties prop = new Properties();
        prop.put("bootstrap.servers","192.168.0.110:9092");
        prop.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        //use ByteArraySerializer
        prop.put("value.serializer","org.apache.kafka.common.serialization.ByteArraySerializer");
        KafkaProducer<String,byte[]> producer = new KafkaProducer<String, byte[]>(prop);
        try{
            File file = new File("/usr/local/kafka_producer/kafka_logo.jpg");
            FileInputStream fis = new FileInputStream(file);
            byte[] buffer = new byte[fis.available()];
            fis.read(buffer);
            ProducerRecord<String,byte[]> record = new ProducerRecord<String, byte[]>("haiqingchen",file.getName(),buffer);
            producer.send(record);
            producer.close();
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
}
