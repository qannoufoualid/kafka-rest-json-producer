package com.websystique.springboot.controller;

import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;
import java.util.Scanner;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.websystique.springboot.service.ProducerService;

@RestController
@RequestMapping("/api")
public class RestApiController {

	public static final Logger logger = LoggerFactory.getLogger(RestApiController.class);

	@Autowired
	ProducerService userService;

	@RequestMapping(value = "/sendMessageToTopic", method = RequestMethod.POST, produces = "application/json", consumes="application/json")
	public ResponseEntity<?> sendMessageToTopic(@RequestBody String stringToParse)
			throws JsonProcessingException, IOException {

		
		ObjectMapper mapper = new ObjectMapper();
		System.err.println(stringToParse);
		JsonNode jsonNode = mapper.readTree(stringToParse);
	
		// Configure the Producer
		Properties configProperties = new Properties();
		configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

		Producer<String, JsonNode> producer = new KafkaProducer<String, JsonNode>(configProperties);

		ProducerRecord<String, JsonNode> rec = new ProducerRecord<String, JsonNode>("test", jsonNode);
		producer.send(rec);

		producer.close();

		return new ResponseEntity<String>("{\"response\": \"Ok Got It ! Wait Please!\"}", HttpStatus.OK);
	}
	
	public String findKeyValue(JsonNode json, String key) {
    	
		Iterator<String> fieldNames = json.fieldNames();
		    while(fieldNames.hasNext()){
		        String fieldName = fieldNames.next();
		        if(fieldName.equals(key)) {
		        		JsonNode fieldValue = json.get(fieldName);
		        		return fieldValue.asText();
		        }
		    }
				
			return "UNDEFINED";
		}
	
}