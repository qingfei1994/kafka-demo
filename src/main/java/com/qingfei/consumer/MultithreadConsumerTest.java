package com.qingfei.consumer;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by ASUS on 12/29/2017.
 */
public class MultithreadConsumerTest {
    public static void main(String[] args) {
        int numOfConsumer=3;
        String groupId="group2";
        List<String> topics = Arrays.asList("haiqingchen");
        ExecutorService executor = Executors.newFixedThreadPool(numOfConsumer);
        for (int i=0;i<numOfConsumer;i++) {
            MultithreadConsumer consumer = new MultithreadConsumer(i,groupId,topics);
            executor.submit(consumer);
        }
    }
}
