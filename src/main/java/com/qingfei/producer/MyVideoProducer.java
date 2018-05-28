package com.qingfei.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

/**
 * Created by ASUS on 1/7/2018.
 */
public class MyVideoProducer {
    public static void main(String[] args) {
        Properties prop = new Properties();
        prop.put("bootstrap.servers","192.168.0.110:9092");
        prop.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        //use ByteArraySerializer
        prop.put("value.serializer","org.apache.kafka.common.serialization.ByteArraySerializer");
        prop.put("max.request.size","5335302");
        KafkaProducer<String,byte[]> producer = new KafkaProducer<String, byte[]>(prop);
        try{
            File file = new File("/usr/local/kafka_producer/test.mp4");
            FileInputStream fis = new FileInputStream(file);
            byte[] buffer = new byte[fis.available()];
            fis.read(buffer);
            ProducerRecord<String,byte[]> record = new ProducerRecord<String, byte[]>("haiqingchen",file.getName(),buffer);
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e!=null) {
                        e.printStackTrace();
                    } else {
                        System.out.println("partition="+recordMetadata.partition()+",offset="+recordMetadata.offset());
                    }
                }
            });
            producer.close();
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
}
