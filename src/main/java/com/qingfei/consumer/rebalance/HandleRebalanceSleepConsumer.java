package com.qingfei.consumer.rebalance;


import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * Created by ASUS on 5/20/2018.
 */
public class HandleRebalanceSleepConsumer {

    private static KafkaConsumer<String,byte[]> kafkaConsumer;
    private static Map<TopicPartition,OffsetAndMetadata> currentOffset = new HashMap<>();
    private static class HandleRebalanceListener implements ConsumerRebalanceListener {
        public void onPartitionsRevoked(Collection<TopicPartition> collection) {
            System.out.println("Losing partitions in rebalance.Committing current offset:"+ currentOffset);
            kafkaConsumer.commitSync(currentOffset);
        }

        public void onPartitionsAssigned(Collection<TopicPartition> collection) {

        }
    }
    public static void main(String[] args) throws Exception{
        Properties kafkaProp = getKafkaProperty();
        kafkaConsumer= new KafkaConsumer<>(kafkaProp);
        kafkaConsumer.subscribe(Arrays.asList("rebalance"),new HandleRebalanceListener());
        while(true) {
            ConsumerRecords<String,byte[]> records = kafkaConsumer.poll(100);
            for(ConsumerRecord<String,byte[]> record:records) {
                System.out.printf("topic=%s,partition=%s,offset=%d,key=%s,value=%s \n", record.topic(), record.partition(), record.offset(), record.key(), record.value());
                currentOffset.put(new TopicPartition(record.topic(),record.partition()),new OffsetAndMetadata(record.offset()+1,"no metadata"));
                Thread.sleep(1000000);
            }
            kafkaConsumer.commitAsync(currentOffset,null);
        }
    }

    public static Properties getKafkaProperty() {
        Properties kafkaProp = new Properties();
        kafkaProp.setProperty("bootstrap.servers","192.168.0.110:9092,192.168.0.110:9093,192.168.0.110:9094");
        kafkaProp.setProperty("group.id","1");
        kafkaProp.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProp.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        return kafkaProp;
    }
}
