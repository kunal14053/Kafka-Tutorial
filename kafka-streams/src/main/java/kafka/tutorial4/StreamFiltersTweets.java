package kafka.tutorial4;


import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class StreamFiltersTweets {


    public static void main(String[] args) {
        //create properties
        Properties properties = new Properties();
        String server = "127.0.0.1:9092";
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        //create a topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        //input topic
        KStream<String,String> inputTopic =  streamsBuilder.stream("twitter_tweet");
        KStream<String,String> filteredStream = inputTopic.filter((k, jsonTweets)-> extractFollowersCountFromTweets(jsonTweets)>1000);
        filteredStream.to("important_tweets");

        //build a topology
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build()
                ,properties);

        //start a stream application
        kafkaStreams.start();

    }


    private static Integer extractFollowersCountFromTweets(String tweetJson) {
        JsonParser jsonParser = new JsonParser();
        try {
            return jsonParser.parse(tweetJson).getAsJsonObject().get("user").getAsJsonObject().get("followers_counts").getAsInt();
        } catch (NullPointerException e){
            return 0;
        }
    }

}
