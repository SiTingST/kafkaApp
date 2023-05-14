package main;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import javax.net.ssl.HttpsURLConnection;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class kafkaStreamApp {

    private static final String SOURCE_NODE = "SOURCE_NODE";
    private static final String PROCESSOR_NODE = "PROCESSOR_NODE";

    private static final String TOPIC = "test";

    private static final String url = "https://api.adviceslip.com/advice";

    public static void main(String[] args)  {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-app");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        StreamsBuilder builder = new StreamsBuilder();

        Topology topology = builder.build();
        topology.addSource(SOURCE_NODE, TOPIC);
        topology.addProcessor(PROCESSOR_NODE, kafkaStreamProcessor::new, SOURCE_NODE);

        KafkaStreams streams = new KafkaStreams(topology, properties);
        streams.start();

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i=0; i<5; i++){
            readFromHttp(url)
                    .thenApply(advice -> {
                        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, "key", advice);
                        System.out.println("record:" +  record);
                        producer.send(record);
                    return null;
                    });
        }

// task 2
//        for (int i=0; i<100; i++) {
//            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, "key1", String.valueOf(random.nextInt(10)));
//            ProducerRecord<String, String> record2 = new ProducerRecord<>(TOPIC, "key2", String.valueOf(random.nextInt(10)));
//            producer.send(record);
//            producer.send(record2);
//        };
        System.out.println("Kafka Streams application is running");
    }

    private static CompletableFuture<String> readFromHttp(String urlLink) {
        ExecutorService executor = Executors.newFixedThreadPool(5);
        StringBuilder stringBuilder = new StringBuilder();

        CompletableFuture<String> httpResponse = CompletableFuture.supplyAsync(() -> {
            try {
                String inputLine;

                URL url = new URL(urlLink);
                HttpsURLConnection connection = (HttpsURLConnection) url.openConnection();
                connection.setRequestProperty("accept", "application/json");
                connection.setRequestMethod("GET");

                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(connection.getInputStream()));

                while( (inputLine = bufferedReader.readLine()) != null) {
                    stringBuilder.append(inputLine);
                }

                bufferedReader.close();
                return stringBuilder.toString();

            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        }, executor);
        return httpResponse;
    }
}