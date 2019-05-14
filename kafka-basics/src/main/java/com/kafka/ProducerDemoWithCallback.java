package com.kafka;


import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    public static void main(String[] args) {
        System.out.println("Producer with call backk :::: ______________________________");
        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);
        //create producer properties
        String bootstrapServers = "127.0.0.1:9092";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer .class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);


        //send data
        for (int i =0 ;i <10; i++){

            final ProducerRecord<String, String > record = new ProducerRecord<String, String>("first_topic", "hello World"+i);
            kafkaProducer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e == null) {
                        System.out.println("record is sent..........");
                        logger.info("Recieved new record \n" +" Topic:: "+ recordMetadata.topic() +"\n Partition:: "+recordMetadata.partition()
                                +"\n offset:: "+ recordMetadata.offset() + "\n Timestamp:: " + recordMetadata.timestamp());
                    }
                    else{

                    }

                }
            });
        }

        //flush data
        kafkaProducer.flush();
        kafkaProducer.close();
    }

}
