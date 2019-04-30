package com.cockroachlabs;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections.map.SingletonMap;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Properties;

public class ExactlyOnce {
    public static void main(final String[] args) {
        final String bootstrapServers, schemaRegistry, topic;
        if (args.length < 3) {
            System.out.println("Connecting to Kafka and schema registry on localhost.");
            System.out.println("Arguments: kafkaurl schemaregistry topic");
            bootstrapServers = "localhost:9092";
            schemaRegistry = "http://localhost:8081";
            topic = "tls_sasl_avro_rides";
        } else {
            bootstrapServers = args[0];
            schemaRegistry = args[1];
            topic = args[2];
        }
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "crdb-cdc-consumer");
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "crdb-cdc-consumer-client");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        streamsConfiguration.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, ClusterLogicalTimestampExtractor.class);
        streamsConfiguration.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        streamsConfiguration.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistry);

        final StreamsBuilder builder = new StreamsBuilder();

        final String stateStorePrefix = "latest_timestamp_";
        final String exactlyOncePrefix = "exactly_once_";
        final String timestampStateStoreName = stateStorePrefix + topic;

        final Serde<GenericRecord> stateKeySerde = new GenericAvroSerde();
        stateKeySerde.configure(new SingletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistry), true);
        final StoreBuilder<KeyValueStore<GenericRecord, String>> tsStoreBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(timestampStateStoreName),
                stateKeySerde,
                Serdes.String()
        );
        builder.addStateStore(tsStoreBuilder);

        final KStream<GenericRecord, GenericRecord> inStream = builder.stream(topic);
        inStream.flatTransform(() -> new CRDBExactlyOnceTransformer(timestampStateStoreName), timestampStateStoreName)
                .to(exactlyOncePrefix + topic);

        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
        streams.cleanUp();
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

}
