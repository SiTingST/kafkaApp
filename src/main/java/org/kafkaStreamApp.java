package org;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import javax.net.ssl.HttpsURLConnection;

import java.util.Properties;

public class kafkaStreamApp {

    private static final String SOURCE_NODE = "SOURCE_NODE";
    private static final String PROCESSOR_NODE = "PROCESSOR_NODE";

    private static final String TOPIC = "test";

    public static void main(String[] args) throws IOException {
        String content = readFromHttp("https://www.w3schools.com/howto/howto_make_a_website.asp");
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-app");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        Topology topology = builder.build();
        builder.stream(TOPIC).to("this is a message from kafka");

        topology.addSource(SOURCE_NODE, TOPIC);
        topology.addProcessor(PROCESSOR_NODE, kafkaStreamProducer::new, SOURCE_NODE);

        KafkaStreams streams = new KafkaStreams(topology, properties);
        streams.start();
    }

    private static String readFromHttp(String urlLink) throws IOException {
        String inputLine;
        String result = null;

        URL url = new URL(urlLink);
        HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(conn.getInputStream()));

        inputLine = bufferedReader.readLine();
        while (inputLine != null) {
            result += inputLine;
            inputLine = bufferedReader.readLine();
        }
        bufferedReader.close();

        return result;
    }
}