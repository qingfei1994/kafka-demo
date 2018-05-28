package com.qingfei.consumer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

/** 异步发送信息
 * Created by ASUS on 12/9/2017.
 */
public class MySimulateProducer {
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
        try {
            for (int i=0;i<10;i++) {
                ProducerRecord<String, String> record = new ProducerRecord<String, String>("haiqingchen", "msg"+i, "hello jenny"+i);
                producer.send(record, new Callback() {
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null) {
                            e.printStackTrace();
                        } else {
                            System.out.println("The message offset is" + recordMetadata.offset() + ",partition is" + recordMetadata.partition());

                        }
                    }
                });
            }
            producer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
