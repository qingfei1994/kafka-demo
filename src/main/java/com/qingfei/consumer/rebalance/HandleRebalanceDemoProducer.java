package com.qingfei.consumer.rebalance;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

/**
 * Created by ASUS on 5/20/2018.
 */
public class HandleRebalanceDemoProducer {
    private static final String TOPIC="rebalance";
    public static void main(String[] args) {
        KafkaProducer<String,String> producer = new KafkaProducer<>(getKafkaProperty());

        for(int i=0;i<10000;i++) {
            ProducerRecord<String,String> record = new ProducerRecord<>(TOPIC,"hello"+i);
            producer.send(record, (RecordMetadata recordMetadata, Exception e)-> {
                    if(e!=null) {
                        e.printStackTrace();
                    } else {
                        System.out.println("send record to partition: "+recordMetadata.partition()+", offset :"+recordMetadata.offset());
                    }
            });
        }
        producer.close();
    }

    public static Properties getKafkaProperty() {
        Properties kafkaProperty = new Properties();
        kafkaProperty.put("bootstrap.servers","192.168.0.110:9092,192.168.0.110:9093,192.168.0.110:9094");
        kafkaProperty.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        kafkaProperty.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        return kafkaProperty;
    }
}
