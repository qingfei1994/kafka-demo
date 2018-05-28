package com.qingfei.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;

/** ͬ��������Ϣ
 * Created by ASUS on 12/9/2017.
 */
public class MyKafkaProducer {
    public static void main(String[] args) throws Exception{
        Properties kafkaProperty = new Properties();
        //����kafka��Ⱥ��ַ
        //kafkaProperty.put("bootstrap.servers","192.168.0.110:9092,192.168.0.110:9093,192.168.0.110:9094");
        kafkaProperty.put("bootstrap.servers","192.168.0.110:9092");
        //kafkaProperty.put("zk.connect","192.168.0.110:2181");

        //kafkaProperty.put("metadata.broker.list","192.168.0.110:9092");
        //key������partition֮����ȷֲ�
        kafkaProperty.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        kafkaProperty.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        //��property����һ��Producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(kafkaProperty);
        ProducerRecord<String,String> record = new ProducerRecord<String, String>("hello","msg","hello chenhaiqing");
        //�������ʧ�ܣ�����׳��쳣������ͻ�ɹ����������Ϣ
        Future<RecordMetadata> future= producer.send(record);
        RecordMetadata recordMetadata = future.get();
        System.out.println("The message offset:" + recordMetadata.offset() + ",partition:" + recordMetadata.partition());

        producer.close();

    }
}
