package com.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoGroup {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoGroup.class);

        //Consumer config
        String bootstrapServer = "127.0.0.1:9092";
        String grp_id = "mygrp-id-1 ";
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,grp_id);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        //subscribe to topic
        consumer.subscribe(Arrays.asList("second_topic"));

        //poll for neew data
        while(true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for(ConsumerRecord record:records){
                logger.info("Key - " + record.key() + " :: value - "+ record.value()+"");
                logger.info("Partition -" + record.key() + " :: Offset - "+ record.value()+"");
            }
        }

    }

}
