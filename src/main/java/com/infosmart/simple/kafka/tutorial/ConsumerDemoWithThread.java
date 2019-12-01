package com.infosmart.simple.kafka.tutorial;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread
{
    public static void main(String[] args) throws InterruptedException {
        new ConsumerDemoWithThread().run();
    }

    private ConsumerDemoWithThread() {}

    private void run() throws InterruptedException {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-sixth-application";
        String autoOffsetReset = "earliest";
        String topic = "first_topic";

        CountDownLatch latch = new CountDownLatch(1);

        logger.info("Creating the consumer thread");

        Runnable myConsumerThread = new ConsumerRunnable(
                bootstrapServers,
                groupId,
                topic,
                latch);

        Thread myThread = new Thread(myConsumerThread);
        myThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("Caught shutdown hook");
            ((ConsumerRunnable) myConsumerThread).shutdown();

            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }
        ));

        try {
            latch.await();
        }
        catch (InterruptedException e) {
            e.printStackTrace();
            logger.error("Application got interrupted", e);
        }
        finally {
            logger.info("Application is closing");
        }
    }

    public static class ConsumerRunnable implements Runnable
    {
        private  CountDownLatch latch;
        private  KafkaConsumer<String, String> consumer;
        private  Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());

        public ConsumerRunnable(String bootstrapServers,
                              String groupId,
                              String topic,
                              CountDownLatch latch)
        {
            this.latch = latch;

            // Create Consumer configs
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


            // Create Consumer
            consumer = new KafkaConsumer<String, String>(properties);

            // Subscribe Consumer to our topic(s)
            consumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run()
        {
            try {
                // Poll for new data
                while (true)
                {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key: " + record.key() + ", Value: " + record.value());
                        logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                    }
                }
            }
            //
            catch (WakeupException e) {
                logger.info("Received shutdown signal!");
            }
            //
            finally {
                // close consumer
                consumer.close();

                // tell our main code we're done with the consumer
                latch.countDown();

            }

        }

        public void shutdown()
        {
            // the wakeup() method is a special method to interrupt consumer.poll()
            // it will throw the exception WakeupException
            consumer.wakeup();
        }


    }   //class-ConsumerRunnable

}
