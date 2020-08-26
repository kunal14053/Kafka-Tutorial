package com.start.kafka.tutorial1.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoAssignSeek {

    public static void main(String[] args) {


        final Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class.getName());

        Properties properties = new Properties();
        String server = "127.0.0.1:9092";
        String topic = "first_topic";
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create a consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);

        //assign and seek are mostly used to reply data or fetch a specific message

        //assign
        TopicPartition partitioToReadFrom = new TopicPartition(topic,0);
        kafkaConsumer.assign(Collections.singleton(partitioToReadFrom));

        long offset = 15L;

        //seek
        kafkaConsumer.seek(partitioToReadFrom, offset);


        int numberOfMsgsToRead = 5 ;
        boolean keepOnReading = true;
        int numberOfMsgsReadSoFar = 0;

        // poll for new data
        while(keepOnReading){
           ConsumerRecords<String, String> records =
                   kafkaConsumer.poll(Duration.ofMillis(100)); //new in Kafka 2.0.0

            for(ConsumerRecord<String, String> record : records){
                numberOfMsgsReadSoFar++;
                logger.info("Key: "+ record.key()+"\n"+
                "Topic: "+ record.topic()+"\n"+ "Value: "+record.value()+"\n"+
                "Partition: "+ record.partition()+"\n"+
                "Offset: "+record.offset()+"\n"+
                "timestamp: "+record.timestamp());
                if(numberOfMsgsReadSoFar >= numberOfMsgsToRead) {
                    keepOnReading = false;
                    break; //exiting the for loop
                }
            }
        }

        logger.info("Exiting the application");

    }

}
