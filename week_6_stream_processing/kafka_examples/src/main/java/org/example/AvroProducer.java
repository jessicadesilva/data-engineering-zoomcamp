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
import schemaregistry.RideRecord;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

public class AvroProducer {

    Properties props = new Properties();

    public AvroProducer(){
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
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());

        String schemaRegUrlConfig = System.getenv("SCHEMA_REGISTRY_URL");
        String schemaRegUserName = System.getenv("SCHEMA_REGISTRY_KEY");
        String schemaRegPassWord = System.getenv("SCHEMA_REGISTRY_SECRET");
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegUrlConfig);
        props.put("basic.auth.credentials.source", "USER_INFO");
        props.put("basic.auth.user.info",schemaRegUserName+":"+schemaRegPassWord);
        
    }

    public List<RideRecord> getRides() throws IOException, CsvException {
        var ridesStream = this.getClass().getResource("/rides.csv");
        var reader = new CSVReader(new FileReader(ridesStream.getFile()));
        reader.skip(1);
        return reader.readAll().stream().map(row -> 
            RideRecord.newBuilder().setVendorId(row[0])
            .setTripDistance(Double.parseDouble(row[4]))
            .setPassengerCount(Integer.parseInt(row[3]))
            .setPuLocationId(Long.parseLong(row[7]))
            .build()
        ).collect(Collectors.toList());

    }

   public void publishRides(List<RideRecord> rides) throws ExecutionException, InterruptedException {
        KafkaProducer<String, RideRecord> kafkaProducer = new KafkaProducer<String, RideRecord>(props);
        for(RideRecord ride: rides) {
            var record = kafkaProducer.send(new ProducerRecord<>("rides_avro", String.valueOf(ride.getVendorId()), ride), (metadata, exception) -> {
                if(exception != null) {
                    System.out.println(exception.getMessage());
                }
            });
            Thread.sleep(500);
	    }
    }
	public static void main(String[] args) throws IOException, CsvException, ExecutionException, InterruptedException {
        var producer = new AvroProducer();
        var rideRecords = producer.getRides();
        producer.publishRides(rideRecords);
    }
}