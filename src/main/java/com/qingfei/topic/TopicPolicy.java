package com.qingfei.topic;

import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.server.policy.CreateTopicPolicy;

import java.util.Map;

public class TopicPolicy implements CreateTopicPolicy {

    @Override
    public void validate(RequestMetadata requestMetadata) throws PolicyViolationException {
        if(requestMetadata.replicationFactor()!=null && requestMetadata.replicationFactor()<2) {
            throw new PolicyViolationException("replication factor should be at least 2");
        }
    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
