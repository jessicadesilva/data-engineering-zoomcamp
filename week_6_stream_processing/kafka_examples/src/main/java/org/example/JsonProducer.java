package org.example;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import org.example.data.Ride;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
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
        Scanner myObj = new Scanner(System.in);  // Create a Scanner object
        System.out.println("Enter API key");
        String userName = myObj.nextLine();  // Read user input
        System.out.println("Enter API secret");
        String passWord = myObj.nextLine(); // Read user input
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "pkc-12576z.us-west2.gcp.confluent.cloud:9092");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.jaas.config", String.format("org.apache.kafka.common.security.plain.PlainLoginModule required username='%s' password='%s';", userName, passWord));
        props.put("sasl.mechanism", "PLAIN");
        props.put("client.dns.lookup", "use_all_dns_ips");
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
		    kafkaProducer.send(new ProducerRecord<>("rides", String.valueOf(ride.DOLocationID), ride), (metadata, exception) -> {
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