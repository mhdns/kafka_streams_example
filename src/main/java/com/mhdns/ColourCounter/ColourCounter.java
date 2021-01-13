package com.mhdns.ColourCounter;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Locale;
import java.util.Properties;

public class ColourCounter {
    public static void main(String[] args) {
        System.out.println("hello world!");

        // write configs
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "application-name");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create streams builder
        StreamsBuilder builder = new StreamsBuilder();

        // Get data from input stream fav-colour-input
        KStream<String, String> inputStream = builder.stream("fav-colour-input");

        // set key values for new stream
        KStream<String, String> interStream = inputStream
                .filter((key, value) -> value.contains(","))
                .selectKey((key, value) -> value.split(",")[0].toLowerCase())
                .mapValues((key, value) -> value.split(",")[1].toLowerCase());

        // Send the values to a new stream user-colour-inter
        interStream.to("user-colour-inter");

        // Create a KTable to get data from intermediate stream user-colour-inter
        KTable<String, String> interTable = builder.table("user-colour-inter");

        // Generate colour count table to count number of users
        KTable<String, Long> finalTable = interTable
                .groupBy((user, colour) -> new KeyValue<>(colour, colour))
                .count();

        // Send table to sink stream color-count-output and specify the serdes
        finalTable.toStream().to("color-count-output", Produced.with(Serdes.String(), Serdes.Long()));

        // Create Stream
        KafkaStreams stream = new KafkaStreams(builder.build(), config);

        // Start Stream
        stream.start();

        // Print topology
        System.out.println(stream.toString());

        // Shutdown gracefully
         Runtime.getRuntime().addShutdownHook(new Thread(stream::close));

    }
}
