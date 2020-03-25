package com.qingfei.producer;

import kafka.producer.Producer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Properties;


/**Test ack properties
 * Created by ASUS on 5/9/2018.
 */
public class TestAckProducer  {
    private final static Logger logger = LoggerFactory.getLogger(TestAckProducer.class);
    public static void main(String[] args) {
        String ack="0";
        if(args.length==1) {
            ack = args[0];
        }
        Properties kafkaProperties = getProperties(ack);
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(kafkaProperties);
        long taskStartTime = System.currentTimeMillis();
        for (int i=0;i<10000;i++) {
            final ProducerRecord<String,String> record = new ProducerRecord<String, String>("hello","msg","hello"+i);
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        //logger.info("send record "+record.value());
                        System.out.println("send record " + record.value());
                    } else {
                        e.printStackTrace();
                    }

                }
            });
        }
        producer.close();
        long taskEndTime = System.currentTimeMillis();
        System.out.println("spend "+ (taskEndTime-taskStartTime)+"ms to process");
    }
    public static Properties getProperties(String ack){
        Properties kafkaProp = new Properties();
        kafkaProp.put("bootstrap.servers","192.168.0.110:9092");
        kafkaProp.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        kafkaProp.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        kafkaProp.put("acks",ack);
        return kafkaProp;
    }
}
