package org.example;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import org.example.data.Ride;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;


import org.apache.kafka.clients.producer.*;
import org.apache.kafka.streams.StreamsConfig;

public class JsonProducer {

    Properties props = new Properties();

    public JsonProducer(){
        // coming froming Confluent Cloud Configuration snippet
        String userName = System.getenv("CLUSTER_API_KEY");
        String passWord = System.getenv("CLUSTER_API_SECRET");
        String bootstrapServer = System.getenv("BOOTSTRAP_SERVER");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.jaas.config", String.format("org.apache.kafka.common.security.plain.PlainLoginModule required username='%s' password='%s';", userName, passWord));
        props.put("sasl.mechanism", "PLAIN");
        props.put("session.timeout.ms", "45000");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaJsonSerializer");
    }

    public List<Ride> getRides() throws IOException, CsvException {
        var ridesStream = this.getClass().getResource("/rides.csv");
        var reader = new CSVReader(new FileReader(ridesStream.getFile()));
        reader.skip(1);
        return reader.readAll().stream().map(arr -> new Ride(arr))
                .collect(Collectors.toList());

    }

   public void publishRides(List<Ride> rides) throws ExecutionException, InterruptedException {
        KafkaProducer<String, Ride> kafkaProducer = new KafkaProducer<String, Ride>(props);
        for(Ride ride: rides) {
            ride.tpep_pickup_datetime = LocalDateTime.now().minusMinutes(20);
            ride.tpep_dropoff_datetime = LocalDateTime.now();
            var record = kafkaProducer.send(new ProducerRecord<>("rides", String.valueOf(ride.PULocationID), ride), (metadata, exception) -> {
                if(exception != null) {
                    System.out.println(exception.getMessage());
                }
            });
            Thread.sleep(500);
	    }
    }
	public static void main(String[] args) throws IOException, CsvException, ExecutionException, InterruptedException {
        var producer = new JsonProducer();
        var rides = producer.getRides();
        producer.publishRides(rides);
    }
}