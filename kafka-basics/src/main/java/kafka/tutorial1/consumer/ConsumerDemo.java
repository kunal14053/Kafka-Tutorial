package kafka.tutorial1.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {

    public static void main(String[] args) {
        System.out.printf("Hello World");

        final Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());

        Properties properties = new Properties();

        String server = "127.0.0.1:9092";
        String groupId = "My-forth-Application";
        String topic = "first_topic";
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create a consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);

        // subscribe consumer to our topic(s)
        kafkaConsumer.subscribe(Collections.singleton(topic));
        //kafkaConsumer.subscribe(Arrays.asList("",""));

        // poll for new data
        while(true){
           ConsumerRecords<String, String> records =
                   kafkaConsumer.poll(Duration.ofMillis(100)); //new in Kafka 2.0.0

            for(ConsumerRecord<String, String> record : records){
                logger.info("Key: "+ record.key()+"\n"+
                "Topic: "+ record.topic()+"\n"+ "Value: "+record.value()+"\n"+
                "Partition: "+ record.partition()+"\n"+
                "Offset: "+record.offset()+"\n"+
                "timestamp: "+record.timestamp());

            }
        }



    }

}
