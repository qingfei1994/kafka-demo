package com.qingfei.topic;

import org.apache.kafka.clients.admin.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaAdminClientDemo {
    public static void main(String[] args) {
        Properties kafkaProperties = new Properties();
        kafkaProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        kafkaProperties.put("create.topic.policy.class.name",TopicPolicy.class.getClass());
        AdminClient client = AdminClient.create(kafkaProperties);
        NewTopic topic = new NewTopic("topic-admin",2,(short) 1);
        Map<String,String> configMap = new HashMap<>();
        configMap.put("cleanup.policy","compact");
        topic.configs(configMap);
        CreateTopicsResult result = client.createTopics(Collections.singleton(topic));
        try {
            result.all().get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } finally {
            client.close();
        }
    }
}
