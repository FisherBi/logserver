package com.greenback.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by fisbii on 17-7-25.
 */
public class LogConsumer {
    private static final SimpleDateFormat sdf = new SimpleDateFormat("YYYY-MM-dd");
    private static final String ihaLogPath = "/data/home/fisbii/ihalog";
    private static final List<String> topicList = Arrays.asList("iha_info","iha_error");

    public static void main(String[] args) {

        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfig);
        TestConsumerRebalanceListener rebalanceListener = new TestConsumerRebalanceListener();
        consumer.subscribe(topicList, rebalanceListener);

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            printLogToFile(records);

            consumer.commitSync();
        }

    }

    private static void printLogToFile(ConsumerRecords<String, String> records) {
        Date now = new Date();
        String date = sdf.format(now);
        File file = new File(ihaLogPath);
        createDirs(file);
        for(String topic : topicList){
            try {
                String logFileName = ihaLogPath + File.separator + topic + "_" + date + ".log";
                final PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(logFileName, true)));
                try {
                    for (ConsumerRecord<String, String> record : records.records(topic)) {
                        System.out.printf("Received Message topic =%s, partition =%s, offset = %d, key = %s, value = %s\n", record.topic(), record.partition(), record.offset(), record.key(), record.value());
                        writer.write(record.value());
                    }
                } finally {
                    writer.flush();
                    writer.close();
                }
            } catch (final Exception ex) {
                System.out.println("error to write file!");
            }
        }
    }

    private static void createDirs(File f) {
        if (f != null && !f.exists()) {
            if (!f.mkdirs()) {
                System.out.println("Can't make directory:" + f.getAbsolutePath());
            }
        }
    }

    private static class  TestConsumerRebalanceListener implements ConsumerRebalanceListener {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            System.out.println("Called onPartitionsRevoked with partitions:" + partitions);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            System.out.println("Called onPartitionsAssigned with partitions:" + partitions);
        }
    }

}
