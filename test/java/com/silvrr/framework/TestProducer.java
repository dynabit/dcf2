package com.silvrr.framework;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class TestProducer {
	private KafkaProducer<byte[], byte[]> producer;
	private static TestProducer instance = new TestProducer();
	public static TestProducer getInstance(){
		return instance;
	}
	private TestProducer(){
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
	    props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
	    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
	    // Use random partitioner. Don't need the key type. Just set it to Integer.
	    // The message is of type String.
	    producer = new KafkaProducer<byte[],byte[]>(props);
	}
	public <T> boolean send(String topic,T obj,Partitioner<T> partitioner,Callback callback){
		int partition = partitioner.getPartition(obj);
		try{
			byte[] bytes=obj.getClass().getName().getBytes("UTF-8");
			producer.send(new ProducerRecord<byte[],byte[]>(topic,partition,
					obj.getClass().getName().getBytes("UTF-8"),PSSerializer.getInstance().ser(obj)), 
					callback);
			return true;
		}catch(Exception e){
			e.printStackTrace();
			return false;
		}
	}
	public static void main(String[] args) throws Exception {
		TestProducer.getInstance();
		Thread.sleep(5000);
	}
}
