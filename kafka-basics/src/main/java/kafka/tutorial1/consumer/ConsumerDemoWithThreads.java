package kafka.tutorial1.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThreads {

    public static void main(String[] args) {
        new ConsumerDemoWithThreads().run();
    }

    public void run() {
        final Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThreads.class.getName());

        Properties properties = new Properties();

        String server = "127.0.0.1:9092";
        String groupId = "My-sixth-Application";
        String topic = "first_topic";
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //latch for dealing with multiple threads
        CountDownLatch countDownLatch = new CountDownLatch(1);

        //create the consumer runnable
        logger.info("Creating the consumer thread");
        Runnable consumerThreadRunnable = new ConsumerThread(countDownLatch, properties, topic);

        //start the thread
        Thread myThread = new Thread(consumerThreadRunnable);
        myThread.start();

        //add the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            ((ConsumerThread) consumerThreadRunnable).shutDown();
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }));

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted", e);
        } finally {
            logger.info("Application is closing");
        }

    }

    public class ConsumerThread implements Runnable {

        private CountDownLatch latch;
        private KafkaConsumer<String, String> kafkaConsumer;

        private final Logger logger = LoggerFactory.getLogger(ConsumerThread.class.getName());

        public ConsumerThread(CountDownLatch countDownLatch, Properties properties, String topic) {
            this.latch = countDownLatch;
            kafkaConsumer = new KafkaConsumer<>(properties);
            kafkaConsumer.subscribe(Collections.singleton(topic));
        }

        @Override
        public void run() {

            try {
                while (true) {
                    ConsumerRecords<String, String> records =
                            kafkaConsumer.poll(Duration.ofMillis(100)); //new in Kafka 2.0.0

                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key: " + record.key() + "\n" +
                                "Topic: " + record.topic() + "\n" + "Value: " + record.value() + "\n" +
                                "Partition: " + record.partition() + "\n" +
                                "Offset: " + record.offset() + "\n" +
                                "timestamp: " + record.timestamp());

                    }
                }
            } catch (WakeupException e) {
                logger.info("Received ShutDown Signal!");
            } finally {
                kafkaConsumer.close();
                //tell out main code we are done with consume
                latch.countDown();
            }


        }

        public void shutDown() {
            //the wakeup() method is a special method to interrupt consumer.poll()
            //it will throw the exception WakeUpException
            kafkaConsumer.wakeup();
        }
    }

}
