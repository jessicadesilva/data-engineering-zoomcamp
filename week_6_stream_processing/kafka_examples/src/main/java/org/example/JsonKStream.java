package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import org.example.data.Ride;

import java.util.Map;
import java.util.HashMap;
import java.util.Properties;

public class JsonKStream {

    private Properties props = new Properties();

    public JsonKStream() {
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
    }

    public void countPLocation() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        // returns a kafka stream
        var ridesStream = streamsBuilder.stream("rides", Consumed.with(Serdes.String(), getJsonSerde()));
        var puLocationCount = ridesStream.groupByKey().count().toStream();
        puLocationCount.to("rides-pulocation-count", Produced.with(Serdes.String(), Serdes.Long()));

        var kStreams = new KafkaStreams(streamsBuilder.build(), props);
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
        var object = new JsonKStream();
        object.countPLocation();
    }
}