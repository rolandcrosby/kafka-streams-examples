package com.cockroachlabs;

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class CRDBCDCConsumer {
    public static void main(final String[] args) {
        final String bootstrapServers = args.length >= 1 ? args[0] : "localhost:9092";
        final String topic = args.length >= 2 ? args[1] : "avro_episodes";
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "crdb-cdc-consumer");
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "crdb-cdc-consumer-client");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        streamsConfiguration.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, ClusterLogicalTimestampExtractor.class);
        streamsConfiguration.put("schema.registry.url", "http://localhost:8081");
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<GenericRecord, GenericRecord> quotesStream = builder.stream(topic);
        quotesStream.foreach((key, value) -> System.out.println(key + "=>" + value));
        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
        streams.cleanUp();
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
