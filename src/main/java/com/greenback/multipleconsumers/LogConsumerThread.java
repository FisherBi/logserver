package com.greenback.multipleconsumers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;

/**
 * Created by fisbii on 17-7-31.
 */
public class LogConsumerThread implements Runnable {
    private static final SimpleDateFormat sdf = new SimpleDateFormat("YYYY-MM-dd");
    private static final String ihaLogPath = "/data/home/fisbii/ihalog";
    private static final List<String> topicList = Arrays.asList("iha_info","iha_error");
    private final KafkaConsumer<String, String> consumer;
    private final String topic;

    public LogConsumerThread(String brokers, String groupId, String topic){
        Properties prop = createConsumerConfig(brokers, groupId);
        this.consumer = new KafkaConsumer<>(prop);
        this.topic = topic;
        this.consumer.subscribe(Arrays.asList(this.topic));
    }

    private static Properties createConsumerConfig(String brokers, String groupId) {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("group.id", groupId);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }

    @Override
    public void run() {
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
}
