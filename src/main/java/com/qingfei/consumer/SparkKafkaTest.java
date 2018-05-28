/*
package com.qingfei.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.*;

import java.lang.Iterable;
import java.util.*;
import java.util.Iterator;

*/
/**
 * Created by ASUS on 1/14/2018.
 *//*

public class SparkKafkaTest {
    public static void main(String[] args) {
        String brokers="192.168.0.110:9092";
        String topics="haiqingchen";
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("SparkafkaTest");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));
        jssc.sparkContext().setLogLevel("WARN");
        Set<String> topicSet = new HashSet<String>(Arrays.asList(topics));
        Map<String,Object> kafkaParams = new HashMap<String,Object>();
        kafkaParams.put("bootstrap.servers",brokers);
        kafkaParams.put("group.id","group2");
        kafkaParams.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        JavaInputDStream<ConsumerRecord<Object,Object>> lines = KafkaUtils.createDirectStream(jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topicSet,kafkaParams));
        JavaDStream<String> flatResult = lines.flatMap(new FlatMapFunction<ConsumerRecord<Object, Object>, String>() {
            public Iterable<String> call(ConsumerRecord<Object, Object> t) throws Exception {
                String[] splits = (t.value().toString()).split(",");
                return Arrays.asList(splits).i
            }
        });
        flatResult.print();
     }
}
*/
