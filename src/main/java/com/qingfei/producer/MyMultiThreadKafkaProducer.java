package com.qingfei.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

/** 异步发送信息
 * Created by ASUS on 12/9/2017.
 */
public class MyMultiThreadKafkaProducer implements Runnable {
    private int messageCount;
    private String topic;
    private KafkaProducer<String,String> producer;
    public MyMultiThreadKafkaProducer(String topic,int messageCount){
        this.topic = topic;
        this.messageCount= messageCount;
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
        producer = new KafkaProducer<String, String>(kafkaProperty);
    }


    public void run() {
        int messageNo =0;
        while(messageNo<messageCount) {
            ProducerRecord<String,String> record = new ProducerRecord<String, String>(topic,"MessageNo"+messageNo,"MessageNo"+messageNo);
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e!=null) {
                        e.printStackTrace();
                    } else {
                        System.out.println("The message offset is"+recordMetadata.offset() +",partition is"+recordMetadata.partition());

                    }
                }
            });
            messageNo++;
        }
        try {
            Thread.sleep(6000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) throws Exception {
        MyMultiThreadKafkaProducer producer = new MyMultiThreadKafkaProducer("hello",10);
        for (int i=0;i<=3;i++) {
            new Thread(producer).start();
        }
    }
}
