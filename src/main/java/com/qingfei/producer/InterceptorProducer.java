package com.qingfei.producer;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
//  java -cp ~/IdeaProjects/kafka-demo/target/kafka-demo-1.0-SNAPSHOT.jar:/Users/chenhaiqing/kafka/libs/* com.qingfei.producer.InterceptorProducer
public class InterceptorProducer {
    public static void main(String[] args) {
        String topic = "interceptor";
        String broker = "localhost:9092";
        Properties kafkaProperties = new Properties();
        kafkaProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,broker);
        kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,org.apache.kafka.common.serialization.StringSerializer.class.getName());
        kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,org.apache.kafka.common.serialization.StringSerializer.class.getName());
        kafkaProperties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,com.qingfei.producer.MyInterceptor.class.getName());

        KafkaProducer producer = new KafkaProducer(kafkaProperties);
        for(int i=0;i<100;i++) {
            ProducerRecord<String,String> record = new ProducerRecord<>(topic,"key" + i,"value" + i);
            producer.send(record);
        }
    }
}
