package com.qingfei.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

/**
 * Created by ASUS on 12/23/2017.
 */
public class MyPartitionProducer {

    public static void main(String[] args) {
        Properties kafkaProperty = new Properties();
        kafkaProperty.put("bootstrap.servers","192.168.0.110:9092");
        kafkaProperty.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        kafkaProperty.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        kafkaProperty.put("partitioner.class","com.qingfei.producer.MyPartitioner");
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(kafkaProperty);
        //ProducerRecord<String,String> record = new ProducerRecord<String, String>("hello",null,"hello kafka partition");
        ProducerRecord<String,String> record = new ProducerRecord<String, String>("hello","1","hello kafka partition");
        producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e!=null) {
                        System.out.println("error occur during sending message");
                        e.printStackTrace();
                    } else {
                        System.out.println("message offset:" + recordMetadata.offset() + ",message partition:" + recordMetadata.partition());
                    }
            }
        });
        producer.close();

    }
}
