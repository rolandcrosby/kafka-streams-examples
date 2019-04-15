package com.cockroachlabs;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

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
        streamsConfiguration.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        streamsConfiguration.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<GenericRecord, GenericRecord> inStream = builder.stream(topic);

        final Serde<GenericRecord> stateSerde = new GenericAvroSerde();
        final StoreBuilder<KeyValueStore<GenericRecord, String>> tsStoreBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("latest-timestamp-store"),
                stateSerde,
                Serdes.String()
        );
        builder.addStateStore(tsStoreBuilder);

        inStream.transformValues(new ValueTransformerWithKeySupplier<GenericRecord, GenericRecord, GenericRecord>() {
            @Override
            public ValueTransformerWithKey<GenericRecord, GenericRecord, GenericRecord> get() {
                return new ValueTransformerWithKey<GenericRecord, GenericRecord, GenericRecord>() {
                    private KeyValueStore<GenericRecord, String> state;

                    @Override
                    public void init(final ProcessorContext processorContext) {
                        this.state = (KeyValueStore<GenericRecord, String>) processorContext.getStateStore("latest-timestamp-store");
                    }

                    @Override
                    public GenericRecord transform(final GenericRecord key, final GenericRecord value) {
                        Utf8 ts = (Utf8) value.get("updated");
                        if (ts == null) {
                            ts = (Utf8) value.get("resolved");
                            if (ts == null) {
                                throw new RuntimeException("timestamp not found in input message");
                            }
                        }
                        final String tsString = ts.toString();
                        final String lastTS = this.state.get(key);
                        if (lastTS == null || tsString.compareTo(lastTS) > 0) {
                            this.state.put(key, tsString);
                            return value;
                        }
                        return null;
                    }

                    @Override
                    public void close() {

                    }
                };
            }
        }, "latest-timestamp-store")
                .to("polished_" + topic);

        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
        streams.cleanUp();
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
