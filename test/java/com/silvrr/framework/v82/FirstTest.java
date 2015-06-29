package com.silvrr.framework.v82;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.ConsumerRecords;

public class FirstTest {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put("enable.auto.commit", "false");
        props.put("group.id", "test");
        props.put("partition.assignment.strategy", "range");
        TestConsumer<byte[], byte[]> consumer = new TestConsumer<byte[], byte[]>(props, new TopicPartition("stateChange", 0), new TopicPartition("stateQuery", 0));
		consumer.start((records)->{
			for(Entry<String, ConsumerRecords<byte[], byte[]>> e : records.entrySet()){
				for(ConsumerRecord<byte[], byte[]> record : e.getValue().records()){
					System.out.println(record.offset()+" @ "+record.topicAndPartition());
				}
			}
		});
		Thread.sleep(10000);
		consumer.stop();
		System.out.println("exiting");
	}

}
