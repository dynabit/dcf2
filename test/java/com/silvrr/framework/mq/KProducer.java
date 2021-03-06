package com.silvrr.framework.mq;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.silvrr.framework.serialize.PSSerializer;

public class KProducer {
	private KafkaProducer<byte[], byte[]> producer;
	private static KProducer instance = null;//new KProducer();
	private static AtomicBoolean inited = new AtomicBoolean(false);
	public static KProducer getInstance(String ip){
		if(inited.compareAndSet(false, true)){
			instance = new KProducer(ip);
		}
		return instance;
	}
	private KProducer(String ip){
		Properties props = new Properties();
		props.put("bootstrap.servers", ip+":9092");
	    props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
	    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
	    // Use random partitioner. Don't need the key type. Just set it to Integer.
	    // The message is of type String.
	    producer = new KafkaProducer<byte[],byte[]>(props);
	}
	public <T> boolean send(String topic,T obj,Partitioner<T> partitioner,Callback callback){
		int partition = partitioner.getPartition(obj);
		try{
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
		KProducer.getInstance("localhost");
		Thread.sleep(5000);
	}
}
