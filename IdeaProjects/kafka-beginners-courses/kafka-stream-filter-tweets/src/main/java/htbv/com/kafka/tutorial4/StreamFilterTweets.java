package htbv.com.kafka.tutorial4;


import com.fasterxml.jackson.core.JsonParseException;
import com.google.gson.JsonParser;

import com.fasterxml.jackson.core.util.JsonParserDelegate;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;
import java.util.stream.Stream;

public class StreamFilterTweets {

    public static void main(String[] args) {
        //create properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-stream");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.StringSerde.class.getName());

        //create a topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        //input topic
        KStream<String, String> inputTopic = streamsBuilder.stream("twitter_topics");
        KStream<String, String> filteredStream = inputTopic.filter(
                //filter for tweets which has a user of over 10000 followers
                (k, jsonTweet) -> extractUserFollowerInTweet(jsonTweet) > 10000
        );
        filteredStream.to("important_tweets");

        //build the topology
        KafkaStreams kafkaStreams = new KafkaStreams(
                streamsBuilder.build(),
                properties
        );
        //start our streams application
        kafkaStreams.start();
    }

    private static JsonParser jsonParser = new JsonParser();

    private static Integer extractUserFollowerInTweet(String tweetJson) {
        //gson library
        try {
            return jsonParser.parse(tweetJson)
                    .getAsJsonObject()
                    .get("User")
                    .getAsJsonObject()
                    .get("followers_count")
                    .getAsInt();
        } catch (NullPointerException e) {
            return 0;
        }
    }

}
