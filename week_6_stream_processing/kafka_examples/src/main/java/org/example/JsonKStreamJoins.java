package org.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.*;
import org.example.customserdes.CustomSerdes;
import org.example.data.PickupLocation;
import org.example.data.Ride;
import org.example.data.VendorInfo;

import java.time.Duration;
import java.util.Optional;
import java.util.Properties;

public class JsonKStreamJoins {

    private Properties props = new Properties();

    public static final String INPUT_RIDE_TOPIC = "rides";
    public static final String INPUT_RIDE_LOCATION_TOPIC = "rides_location";
    public static final String OUTPUT_TOPIC = "vendor_info";

    public JsonKStreamJoins(Optional<Properties> properties) {
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
            // update application id
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka_tutorial.kstream.joined.rides.pickuplocation.v1");
            props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

            return props;
        });
    }

    public Topology createTopology() {
        // need streambuilder and data from input topic(s)
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, Ride> rides = streamsBuilder.stream(INPUT_RIDE_TOPIC, Consumed.with(Serdes.String(), CustomSerdes.getSerde(Ride.class)));
        KStream<String, PickupLocation> pickupLocations = streamsBuilder.stream(INPUT_RIDE_LOCATION_TOPIC, Consumed.with(Serdes.String(), CustomSerdes.getSerde(PickupLocation.class)));
        
        var pickupLocationsKeyedOnPUId = pickupLocations.selectKey((key, value) -> String.valueOf(value.PULocationID));
        var joined = rides.join(pickupLocationsKeyedOnPUId, (ValueJoiner<Ride, PickupLocation, Optional<VendorInfo>>) (ride, pickupLocation) -> {
            // time elapsed between calls
            var period = Duration.between(ride.tpep_dropoff_datetime, pickupLocation.tpep_pickup_datetime);
            // Optional is wrapper around null
            if(period.abs().toMinutes() > 10) return Optional.empty();
            else return Optional.of(new VendorInfo(ride.VendorID, pickupLocation.PULocationID, pickupLocation.tpep_pickup_datetime, ride.tpep_dropoff_datetime));
        },
            JoinWindows.ofTimeDifferenceAndGrace(Duration.ofMinutes(20), Duration.ofMinutes(5)),
        StreamJoined.with(Serdes.String(), CustomSerdes.getSerde(Ride.class), CustomSerdes.getSerde(PickupLocation.class)));

        joined.filter(((key, value) -> value.isPresent())).mapValues(Optional::get).to(OUTPUT_TOPIC, Produced.with(Serdes.String(), CustomSerdes.getSerde(VendorInfo.class)));

        // returns the topology
        return streamsBuilder.build();
    }

    public void joinRidesPickupLocation() throws InterruptedException {

        var topology = createTopology();
        var kStreams = new KafkaStreams(topology, props);

        kStreams.setUncaughtExceptionHandler(exception -> {
            System.out.println(exception.getMessage());
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
        });
        kStreams.start();
        while (kStreams.state() != KafkaStreams.State.RUNNING) {
            System.out.println(kStreams.state());
            Thread.sleep(1000);
        }
        System.out.println(kStreams.state());
        Runtime.getRuntime().addShutdownHook(new Thread(kStreams::close));
        
    }

    public static void main(String[] args) throws InterruptedException{
        var object = new JsonKStreamJoins(Optional.empty());
        object.joinRidesPickupLocation();
    }
}