package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.example.data.Ride;

import java.time.temporal.ChronoUnit;
import java.time.*;
import java.util.List;
import java.util.Properties;

import io.confluent.kafka.serializers.KafkaJsonDeserializerConfig;

public class JsonConsumer {
    // make private
	private Properties props = new Properties();
    private KafkaConsumer<String, Ride> consumer;

	public JsonConsumer(){
		// read in environment variables
		String userName = System.getenv("CLUSTER_API_KEY");
		String passWord = System.getenv("CLUSTER_API_SECRET");
        String bootstrapServer = System.getenv("BOOTSTRAP_SERVER");
        System.out.println(userName);
        System.out.println(passWord);
		
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.jaas.config", String.format("org.apache.kafka.common.security.plain.PlainLoginModule required username='%s' password='%s';", userName, passWord));        props.put("sasl.mechanism", "PLAIN");
        props.put("client.dns.lookup", "use_all_dns_ips");
        props.put("session.timeout.ms", "45000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaJsonDeserializer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka_tutorial_example.jsonconsumer.v2");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(KafkaJsonDeserializerConfig.JSON_VALUE_TYPE, Ride.class);
        consumer = new KafkaConsumer<String, Ride>(props);
        consumer.subscribe(List.of("rides"));
    }

    public void consumeFromKafka() {
        System.out.println("Consuming form kafka started");
        while(true){
            var results = consumer.poll(Duration.of(1, ChronoUnit.SECONDS));
            for(ConsumerRecord<String, Ride> result: results) {
                    System.out.println(result.value().DOLocationID);
            }
        }
    }

    public static void main(String[] args) {
        JsonConsumer jsonConsumer = new JsonConsumer();
        jsonConsumer.consumeFromKafka();
	}

}
