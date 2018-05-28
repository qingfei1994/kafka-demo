package com.qingfei.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.List;
import java.util.Properties;

/**
 * Created by ASUS on 12/27/2017.
 */
public class MultithreadConsumer implements Runnable {
    KafkaConsumer<String,String> consumer;
    List<String> topics;
    int id;
    String groupId;
    public MultithreadConsumer(int id,String groupId,List<String> topics){
        this.id= id;
        this.groupId=groupId;
        this.topics=topics;
        Properties prop = new Properties();
        prop.put("bootstrap.servers","192.168.0.110:9092");
        prop.put("group.id", groupId);
        prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer=new KafkaConsumer<String, String>(prop);
        consumer.subscribe(topics);
    }

    public void run() {
        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for(ConsumerRecord record:records) {
                System.out.println("Consumer-"+id+"'s consumption message partition: "+record.partition()+",offset:"+record.offset()+",key:"+record.key()+",value:"+record.value());
            }
        }
    }
}
