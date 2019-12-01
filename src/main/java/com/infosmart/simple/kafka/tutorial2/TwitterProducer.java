package com.infosmart.simple.kafka.tutorial2;

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
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
//import java.util.logging.Logger;

public class TwitterProducer
{
    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    String consumerKey = "qTwN829defcpf9XhorXUl0gUA";
    String consumerSecret = "UntbO0F95TZjzmXIHi6CqVYROwu9vMxCJ71Wc7rDHQAHpscK37";
    String token = "18499132-1RLEoApKubg9rwxAJee2IpgO5g86IQLLC2EDcdxPm";
    String secret = "VfewfyfR3s2OF9MomJ1hcUHzPa4hlTp4VCICF59u4B3Mq";

    List<String> terms = Lists.newArrayList("bitcoin");


    public TwitterProducer() {}

    public static void main(String[] args) throws InterruptedException {
        new TwitterProducer().run();
    }

    public void run() throws InterruptedException
    {
        int capacity = 1000;

        //Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(capacity);

        // Create a Twiiter client
        Client client = createTwitterClient(msgQueue);

        // Attempts to establish a connection.
        client.connect();


        // create a kafka producer
        KafkaProducer<String, String> producer = createKafkaProducer();


        // Add a shutdown hook
        Runtime.getRuntime().addShutdownHook(
                new Thread( () -> {
                    logger.info("stopping application...");
                    logger.info("shutting down client from twitter");
                    client.stop();

                    logger.info("closing producer");
                    producer.close();

                    logger.info("Exit!");
                })
        );


        // loop to send tweets to kafka
        while (!client.isDone())
        {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }

            if ( msg != null)
            {
                logger.info(msg);

                String topic = "twitter_tweets";
                String key = null;

                // Send message to Kafka via producer
                producer.send(new ProducerRecord<String, String>(topic, key, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null) {
                            logger.error("Error Occurred from producer");
                        }
                    }
                });
            }
        }

        logger.info("End of application");
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue)
    {
        // Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth)
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        // Optional: set up some followings and track terms
        // Note: follow terms only
//        List<Long> followings = Lists.newArrayList(1234L, 566788L);
//        hosebirdEndpoint.followings(followings);

        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);


        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

// optional: use this if you want to process client events
//                .eventMessageQueue(eventQueue);

        Client hosebirdClient = builder.build();

        return hosebirdClient;

//        // Attempts to establish a connection.
//        hosebirdClient.connect();
    }


    // Create Kafka Producer
    public KafkaProducer<String, String> createKafkaProducer()
    {
        String bootstrapServers = "127.0.0.1:9092";

        // create Producer proprerties
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create the Producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);
        return (producer);
    }


}