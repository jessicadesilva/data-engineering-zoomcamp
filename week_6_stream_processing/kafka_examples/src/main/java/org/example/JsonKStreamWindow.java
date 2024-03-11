package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import org.example.customserdes.CustomSerdes;
import org.example.data.Ride;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.WindowedSerdes;

import java.util.Map;
import java.util.HashMap;
import java.util.Properties;
import java.util.Optional;
import java.time.Duration;
import java.time.temporal.ChronoUnit;

public class JsonKStreamWindow {

    private Properties props = new Properties();

    public JsonKStreamWindow(Optional<Properties> properties) {
        this.props = properties.orElseGet(() -> {
            String userName = System.getenv("CLUSTER_API_KEY");
            String passWord = System.getenv("CLUSTER_API_SECRET");
            String bootstrapServer = System.getenv("BOOTSTRAP_SERVER");
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            props.put("security.protocol", "SASL_SSL");
            props.put("sasl.jaas.config", String.format("org.apache.kafka.common.security.plain.PlainLoginModule required username='%s' password='%s';", userName, passWord));
            props.put("sasl.mechanism", "PLAIN");
            props.put("client.dns.lookup", "use_all_dns_ips");
            props.put("session.timeout.ms", "45000");
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka_tutorial.kstream.count.plocation.v1");
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
            return props;
        });
    }

    public Topology createTopology() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        var ridesStream = streamsBuilder.stream("rides", Consumed.with(Serdes.String(), CustomSerdes.getSerde(Ride.class)));
        var puLocationCount = ridesStream.groupByKey().windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofSeconds(10), Duration.ofSeconds(5))).count().toStream();
        var windowSerde = WindowedSerdes.timeWindowedSerdeFrom(String.class, 10*1000);
        puLocationCount.to("rides-pulocation-window-count", Produced.with(windowSerde, Serdes.Long()));
        return streamsBuilder.build();
    }

    public void countPLocation() {
        var topology = createTopology();

        var kStreams = new KafkaStreams(topology, props);
        kStreams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(kStreams::close));
    }

    private Serde<Ride> getJsonSerde() {
        Map<String, Object> serdeProps = new HashMap<>();
        serdeProps.put("json.value.type", Ride.class);
        final Serializer<Ride> mySerializer = new KafkaJsonSerializer();
        mySerializer.configure(serdeProps, false);

        final Deserializer<Ride> myDeserializer = new KafkaJsonDeserializer<>();
        myDeserializer.configure(serdeProps, false);
        return Serdes.serdeFrom(mySerializer, myDeserializer);
        }


    public static void main(String[] args) {
        var object = new JsonKStreamWindow(Optional.empty());
        object.countPLocation();
    }
}