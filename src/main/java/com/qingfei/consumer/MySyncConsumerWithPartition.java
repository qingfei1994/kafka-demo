package com.qingfei.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**Commit offset in partition level
 * Created by ASUS on 12/24/2017.
 */
public class MySyncConsumerWithPartition {
    public static void main(String[] args) {
        Properties kafkaProp = new Properties();
        kafkaProp.put("bootstrap.servers", "192.168.0.110:9092");
        kafkaProp.put("group.id", "group1");
        kafkaProp.put("enable.auto.commit","false");
        kafkaProp.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProp.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String,byte[]> consumer = new KafkaConsumer<String, byte[]>(kafkaProp);
        consumer.subscribe(Arrays.asList("haiqingchen"));
        try {
            while (true) {
                ConsumerRecords<String,byte[]> consumerRecords = consumer.poll(100);
                //遍历每个partition
                for (TopicPartition partition:consumerRecords.partitions()) {
                    //获取指定partition的消息记录
                    List<ConsumerRecord<String,byte[]>> records = consumerRecords.records(partition);
                    //遍历指定partition中的消息记录
                    for(ConsumerRecord record:records){
                        System.out.println("consumer consume message:partition"+record.partition()+",offset="+record.offset()+",key="+record.key()+",value="+record.value());
                    }
                    //获取partition的最后一个offset
                    long lastOffset = records.get(records.size()-1).offset();
                    consumer.commitSync(Collections.singletonMap(partition,new OffsetAndMetadata(lastOffset)));
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }
}
