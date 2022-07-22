package com.kk.wikimedia;

import java.net.URI;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.serialization.StringSerializer;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;

/**
 * 
 * @author korayk
 *
 */
public class WikimediaProducer {
	
	private static final String bootstrapServerAddress = "10.10.10.122:9092";
	private static final String topic = "wikimedia.change";
	private static final String changeUrl = "https://stream.wikimedia.org/v2/stream/recentchange";

	public static void main(String[] args) throws InterruptedException {
		
		Properties producerProperties = new Properties();
		producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServerAddress);
		producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		// Compress message with snappy type
		producerProperties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, CompressionType.SNAPPY.name);
		
		// Increase batch size
		producerProperties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));
		
		// set linger ms
		producerProperties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "50");
		
//		producerProperties.setProperty(ProducerConfig.ACKS_CONFIG, "-1");
		
		
		KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties);
		
		EventHandler changeEventHandler = new WikimediaEventSourceHandler(producer, topic);
		EventSource.Builder eventSourceBuilder = new EventSource.Builder(changeEventHandler, URI.create(changeUrl));
		EventSource eventSource = eventSourceBuilder.build();
		
		eventSource.start();
		
		Thread.sleep(10000);
		
	}
}
