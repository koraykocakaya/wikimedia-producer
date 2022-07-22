package com.kk.wikimedia;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;

public class WikimediaEventSourceHandler implements EventHandler{

	private static final Logger log = LoggerFactory.getLogger(WikimediaEventSourceHandler.class.getSimpleName());
	private KafkaProducer<String, String> producer;
	private String topic;
	
	public WikimediaEventSourceHandler(KafkaProducer<String, String> producer, String topic) {
		super();
		this.producer = producer;
		this.topic = topic;
	}
	
	@Override
	public void onOpen() throws Exception {
		
	}

	@Override
	public void onClosed() throws Exception {
		producer.close();
	}

	@Override
	public void onMessage(String event, MessageEvent messageEvent) throws Exception {
		
		ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, messageEvent.getData());
		producer.send(record);
		log.info("Message sent: " + messageEvent.getData());
		
	}

	@Override
	public void onComment(String comment) throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onError(Throwable t) {
		log.error("Exception thrown: ", t);
		
	}
 
}
