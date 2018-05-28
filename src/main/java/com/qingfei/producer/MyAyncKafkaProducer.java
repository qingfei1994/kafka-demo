package com.qingfei.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;

/** �첽������Ϣ
 * Created by ASUS on 12/9/2017.
 */
public class MyAyncKafkaProducer {
    public static void main(String[] args) throws Exception{
        Properties kafkaProperty = getProperties();
        //��property����һ��Producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(kafkaProperty);
        ProducerRecord<String,String> record = new ProducerRecord<String, String>("hello","msg","hello jennychen");
        producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e!=null) {
                        e.printStackTrace();
                    } else {
                        System.out.println("The message offset is"+recordMetadata.offset() +",partition is"+recordMetadata.partition());

                    }
            }
        });
        producer.close();
    }
    public static Properties getProperties(){
        Properties kafkaProperty = new Properties();
        //����kafka��Ⱥ��ַ
        //kafkaProperty.put("bootstrap.servers","192.168.0.110:9092,192.168.0.110:9093,192.168.0.110:9094");
        kafkaProperty.put("bootstrap.servers","192.168.0.110:9092");
        //kafkaProperty.put("zk.connect","192.168.0.110:2181");

        //kafkaProperty.put("metadata.broker.list","192.168.0.110:9092");
        //key������partition֮����ȷֲ�
        kafkaProperty.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        kafkaProperty.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        return kafkaProperty;
    }

}
