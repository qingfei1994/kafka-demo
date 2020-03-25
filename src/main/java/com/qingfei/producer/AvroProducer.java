package com.qingfei.producer;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

//  java -cp ~/IdeaProjects/kafka-demo/target/kafka-demo-1.0-SNAPSHOT.jar:/Users/chenhaiqing/kafka/libs/*:/Users/chenhaiqing/.m2/repository/org/apache/avro/avro/1.9.1/avro-1.9.1.jar com.qingfei.producer.AvroProducer
public class AvroProducer {
    public static void main(String[] args) {
        String topic = "avro";
        String broker = "localhost:9092";
        Properties kafkaProperties = new Properties();
        kafkaProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,broker);
        kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,AvroSerializer.class.getName());

        KafkaProducer producer = new KafkaProducer(kafkaProperties);
        for(int i=0;i<100;i++) {
            User user = new User();
            user.setName("name" + i);
            user.setAge(i+1);
            ProducerRecord<String,User> record = new ProducerRecord<>(topic,user);
            producer.send(record);
        }
    }
}
