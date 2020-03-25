package com.qingfei.topic;

import org.apache.kafka.clients.admin.*;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class KafkaAdminClientListTopicDemo {
    public static void main(String[] args) {
        Properties kafkaProperties = new Properties();
        kafkaProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        AdminClient client = AdminClient.create(kafkaProperties);

        ListTopicsResult result = client.listTopics();
        try {
            Set<String> topics = result.names().get();
            for(String topic:topics) {
                System.out.println("topic:" + topic);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } finally {
            client.close();
        }
    }
}
