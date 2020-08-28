package kafka.tutorial3;

import com.google.gson.Gson;
import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ElasticSearchConsumer {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

    public static RestHighLevelClient createClient() {

        String hostName="kafka-course-9076664169.us-east-1.bonsaisearch.net";
        String userName="zdegn6zi3p";
        String password="vavnoqye2l";

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,new UsernamePasswordCredentials(userName, password));

        RestClientBuilder builder = RestClient.builder(new HttpHost(hostName, 443, "https")).setHttpClientConfigCallback(
                new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                }
        );

        RestHighLevelClient client = new RestHighLevelClient(builder);

        return client;
    }

    public static KafkaConsumer<String, String> createKafkaConsumer(String topic){

        Properties properties = new Properties();

        String server = "127.0.0.1:9092";
        String groupId = "kafka-demo-elasticsearch";

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //disable auto commit of offset
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"100");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);

        kafkaConsumer.subscribe(Collections.singleton(topic));

        return kafkaConsumer;
    }


    public static void main(String[] args) throws IOException {

        RestHighLevelClient client = createClient();

        String topic = "twitter_tweet";
        KafkaConsumer<String, String> kafkaConsumer =  createKafkaConsumer(topic);


        while(true){
            ConsumerRecords<String, String> records =
                    kafkaConsumer.poll(Duration.ofMillis(100)); //new in Kafka 2.0.0

            Integer recordCounts = records.count();

            logger.info("Received "+ recordCounts +" records");

            BulkRequest bulkRequest = new BulkRequest();

            for(ConsumerRecord<String, String> record : records){

                try {
                    //Kafka Generic ID
                    //String id = record.topic() + "_" + record.partition() + "_" +  record.offset();

                    //twitter feed specific id
                    String id = extractIDFromTweets(record.value());

                    //where we insert data into ElasticSearch
                    IndexRequest indexRequest = new IndexRequest(
                            "twitter",
                            "tweets",
                            id //this is to make out consumer idempotent
                    ).source(record.value(), XContentType.JSON);

                    bulkRequest.add(indexRequest); //we add to out bulk request

                    // IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);

                    //logger.info("Index Response ID: "+ indexResponse.getId());

                    /* try {
                        Thread.sleep(10); //introduce a small delay
                     } catch (InterruptedException e) {
                           e.printStackTrace();
                     }*/
                } catch (NullPointerException e){
                    logger.warn("Skipping bad data: " + record.value());
                }

            }

            if(recordCounts>0) {

                BulkResponse bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);

                logger.info("Committing the offset");
                kafkaConsumer.commitSync();
                logger.info("Offset have been committed");

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        //close the ElasticSearch Client
        //client.close();
    }

    private static String extractIDFromTweets(String tweetJson) {
        JsonParser jsonParser = new JsonParser();
        return jsonParser.parse(tweetJson).getAsJsonObject().get("id_str").getAsString();
    }

}
