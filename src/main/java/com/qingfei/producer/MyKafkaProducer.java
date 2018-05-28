package com.qingfei.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;

/** 同步发送信息
 * Created by ASUS on 12/9/2017.
 */
public class MyKafkaProducer {
    public static void main(String[] args) throws Exception{
        Properties kafkaProperty = new Properties();
        //配置kafka集群地址
        //kafkaProperty.put("bootstrap.servers","192.168.0.110:9092,192.168.0.110:9093,192.168.0.110:9094");
        kafkaProperty.put("bootstrap.servers","192.168.0.110:9092");
        //kafkaProperty.put("zk.connect","192.168.0.110:2181");

        //kafkaProperty.put("metadata.broker.list","192.168.0.110:9092");
        //key用于在partition之间均匀分布
        kafkaProperty.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        kafkaProperty.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        //用property创建一个Producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(kafkaProperty);
        ProducerRecord<String,String> record = new ProducerRecord<String, String>("hello","msg","hello chenhaiqing");
        //如果发送失败，则会抛出异常，否则就会成功输出所需信息
        Future<RecordMetadata> future= producer.send(record);
        RecordMetadata recordMetadata = future.get();
        System.out.println("The message offset:" + recordMetadata.offset() + ",partition:" + recordMetadata.partition());

        producer.close();

    }
}
