package com.qingfei.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

/**At Least Once
 * Created by ASUS on 12/24/2017.
 */
public class MyAsyncConsumer {
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
                for (ConsumerRecord<String,byte[]> record:consumerRecords) {
                    System.out.println("MyFirstConsumer's consumption message:partition"+record.partition()+",offset:"+record.offset()+",key="+record.key()+",value="+record.value());

                }
                consumer.commitAsync(new OffsetCommitCallback() {
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
                        if (e!=null) {
                            System.out.println("error occur when committing offset");
                        }
                    }
                });
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }
}
