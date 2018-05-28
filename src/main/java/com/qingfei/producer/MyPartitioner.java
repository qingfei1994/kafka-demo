package com.qingfei.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.record.InvalidRecordException;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;

/**
 * Created by ASUS on 12/23/2017.
 */
public class MyPartitioner implements Partitioner {
    public MyPartitioner(){

    }
    public void configure(Map<String, ?> var1) {

    }
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster)
    {
        System.out.println("I am a custom partitioner....");
        List<PartitionInfo> partitions = cluster.availablePartitionsForTopic(topic);
        int numPartitions = partitions.size();
        System.out.println("numPartitions:"+numPartitions);
        //不允许record的key值为null
        if (keyBytes==null) {
            throw new InvalidRecordException("key can not be null");
        }

        if (key.toString().equals("1")) {
            return 0;
        }
        return (Utils.abs(Utils.murmur2(keyBytes))%(numPartitions));
    }
    public void close(){

    }
}
