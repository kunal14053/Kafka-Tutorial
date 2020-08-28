package kafka.tutorial1.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class.getName());

        System.out.println("Hello World");

        //Producer Property
        Properties properties = new Properties();
        String server = "127.0.0.1:9092";
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //send Data Async
        for(int i=0; i<10; i++) {
            String topic = "first_topic";
            String value = "Hello World:" + Integer.toString(i);
            String key = "Key_"+ Integer.toString(i);

            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic,key,value);

            logger.info("Key: "+key); //log the key

            //send data async
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //execute every time a record is send or an exception is thrown
                    if (e == null) {
                        //record is sent successfully
                        logger.info("Received new metadata. \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "TimeStamp: " + recordMetadata.timestamp() + "\n" +
                                "Offset: " + recordMetadata.offset());

                    } else {
                        logger.error("Error while producing", e);
                    }
                }
            }).get(); //block the send to make it sync //not to do in production

        }
        //flush data
        producer.flush();
        //flush and close producer
        producer.close();
    }
}
