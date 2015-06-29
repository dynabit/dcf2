package com.silvrr.framework.v82;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

public class TestConsumer<K,V> {
	private static class BoolHolder{
		private boolean val;
		public BoolHolder(boolean val){this.val=val;}
		public void set(boolean val){
			this.val=val;
		}
		public boolean get(){
			return this.val;
		}
	}
	private AtomicBoolean started;
	private BoolHolder forRunning;
	private Properties props;
	private TopicPartition[] partitions;
	private TestConsumer(){;}
	public TestConsumer(Properties props,TopicPartition... partitions){
		this.props=props;
		this.partitions=partitions;
		started=new AtomicBoolean(false);
		forRunning=new BoolHolder(true);
	}
	public void start(MessageHandler<K,V> handler){
		if(started.compareAndSet(false, true)){
			new Thread(()->{
				KafkaConsumer<K,V> consumer = null;
				try{
					consumer = new KafkaConsumer<K,V>(props);
					consumer.subscribe(partitions);
					while(true){
						boolean x = forRunning.get();
						System.out.println("reading "+x);
						if(!x){
							break;
						}
						try{
							Map<String, ConsumerRecords<K,V>> records = consumer.poll(1000);
							if(records!=null)
								handler.process(records);
						}catch(Exception ex){
							ex.printStackTrace();
							forRunning.set(false);
						}
					}
					System.out.println("thread end");
				}catch(Exception e){
					e.printStackTrace();
				}finally{
					if(consumer!=null){
						consumer.close();
					}
				}
			}).start();
		}else{
			//started once already
		}
	}
	public void stop(){
		System.out.println("setting forRunning : "+this.forRunning.get());
		this.forRunning.set(false);
		System.out.println("to forRunning : "+this.forRunning.get());
	}
}
