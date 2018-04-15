package com.websystique.springboot.service;

import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@Service("producerService")
public class ProducerService{

	
	public String handleRequest(String message) throws JsonProcessingException, IOException{
		ObjectMapper mapper = new ObjectMapper();

		JsonNode jsonNode = mapper.readTree(message);
	
		// Configure the Producer
		Properties configProperties = new Properties();
		configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
		configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

		Producer<String, JsonNode> producer = new KafkaProducer<String, JsonNode>(configProperties);

		ProducerRecord<String, JsonNode> rec = new ProducerRecord<String, JsonNode>("test", jsonNode);
		producer.send(rec);

		producer.close();
		
		return "{\"response\": \"Ok Got It ! Wait Please!\"}";
		
	}
	
}
