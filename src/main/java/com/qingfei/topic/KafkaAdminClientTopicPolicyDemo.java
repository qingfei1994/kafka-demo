package com.qingfei.topic;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaAdminClientTopicPolicyDemo {
    public static void main(String[] args) {
        Properties kafkaProperties = new Properties();
        kafkaProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        AdminClient client = AdminClient.create(kafkaProperties);
        NewTopic topic = new NewTopic("topic-policy",2,(short) 1);
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
