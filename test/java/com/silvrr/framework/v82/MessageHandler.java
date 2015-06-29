package com.silvrr.framework.v82;

import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecords;

public interface MessageHandler<K,V> {
	public void process(Map<String, ConsumerRecords<K,V>> records) throws Exception;
}
