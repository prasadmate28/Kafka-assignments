package com.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class);
    public TwitterProducer(){

    }

    public static void main(String[] args) {
        System.out.println("hello");
        new TwitterProducer().run();
    }

    public void run() {
        System.out.println("hello run");
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
//        BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(1000);

        Client client = createTwitterClient(msgQueue);
        //create twitter client
        client.connect();



        //create a kafka producer
        KafkaProducer<String, String> producer = createKafkaProducer();



        //add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            logger.info("Topping application");
            logger.info("shutting down client from twitter..");
            client.stop();
            logger.info("closing producer....");
            producer.close();
            logger.info("done....!");
        }));

        //loop  to send tweets to kafka
        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5L,TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if(msg!=null){
                logger.info(""+msg);
                producer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e!=null){
                            logger.error("Something went wrong");
                        }else{
                             logger.info("topic posted");
                        }
                    }
                });
            }
        }
        logger.info("End of client");

    }

    private KafkaProducer<String,String> createKafkaProducer() {
        //create producer properties
        String bootstrapServers = "127.0.0.1:9092";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

    return producer;
    }

    String consumerKey = "9IpNsmkrgbNABCFqcp1aYTSar";
    String consumerSecret = "8WPTJmDYTBa2A2RQzGMPG0hZ1w8NcaGjV3Va2hfJDHvPkviZnO";
    String token = "840952710711169024-W3wl3eDhpE7t7JDuWBDViqLmelL43PX";
    String secret = "Nq3DXwyPoHUAif2vSPuIwv7CI7XlNmFEEQMTUbdeZC8GS";
    List<String> terms = Lists.newArrayList("kafka",  "distributed","systems");

    public Client createTwitterClient(BlockingQueue<String> msgQueue){

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        //List<Long> followings = Lists.newArrayList(1234L, 566788L);

        //hosebirdEndpoint.followings(followings);
        hosebirdEndpoint.trackTerms(terms);
    // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret );

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));
                //.eventMessageQueue(eventQueue);                          // optional: use this if you want to process client events

        Client hosebirdClient = builder.build();

        return hosebirdClient;

    }
}