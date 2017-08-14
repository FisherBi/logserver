package com.greenback.multipleconsumers;

import java.util.Arrays;
import java.util.List;

/**
 * Created by fisbii on 17-7-31.
 */
public class MultipleConsumersMain {
    private static final List<String> topicList = Arrays.asList("iha_info", "iha_error");
    public static void main(String[] args) {

        String brokers = "localhost:9092";
        String groupId = "group01";
        int numberOfConsumer = 3;

        for(String topic : topicList){
            // Start group of Notification Consumers
            NotificationConsumerGroup consumerGroup =
                    new NotificationConsumerGroup(brokers, groupId, topic, numberOfConsumer);

            consumerGroup.execute();
        }
    }
}
