package com.silvrr.framework.mq;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.Message;
import kafka.message.MessageAndOffset;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.silvrr.framework.serialize.PSSerializer;

public class KConsumer {
	private static Logger log = LoggerFactory.getLogger(KConsumer.class);
	private boolean infoValid=false;
	private String topic;
	private int partition;
	private AtomicBoolean started=new AtomicBoolean(false);
	private boolean running;
	private Handler<Object> defaultHandler;
	
	@SuppressWarnings("rawtypes")
	private Map<Class,Handler> handlerMap=new HashMap<Class,Handler>();
	
	public <T> KConsumer on(Class<T> clz,Handler<T> handler){
		handlerMap.put(clz, handler);
		return this;
	}
	
	public KConsumer onDefault(Handler<Object> handler){
		this.defaultHandler=handler;
		return this;
	}
	
    private List<SocketInfo> replicaBrokers;
 
    private KConsumer(){;}
    private void init(String topic,int partition,List<SocketInfo> replicaBrokers){
    	this.replicaBrokers=replicaBrokers;
    	this.topic=topic;
    	this.partition=partition;
    }
    public KConsumer(String topic,int partition,int port,String... seedBrokers){
    	this();
    	if(topic==null)return;
    	if(seedBrokers==null)return;
    	if(seedBrokers.length==0)return;
    	List<SocketInfo> brokers = new ArrayList<SocketInfo>();
    	for(int i=0;i<seedBrokers.length;i++){
    		brokers.add(new SocketInfo(seedBrokers[i],port));
    	}
    	init(topic,partition,brokers);
    	infoValid=true;
    }
    public KConsumer(String topic,int partition,String[] seedBrokers,int[] ports) {
    	this();
    	if(topic==null)return;
    	if(seedBrokers==null)return;
    	if(seedBrokers.length==0)return;
    	if(ports==null)return;
    	if(ports.length==0)return;
    	if(seedBrokers.length!=ports.length)return;
    	List<SocketInfo> brokers = new ArrayList<SocketInfo>();
    	for(int i=0;i<seedBrokers.length;i++){
    		brokers.add(new SocketInfo(seedBrokers[i],ports[i]));
    	}
    	init(topic,partition,brokers);
    	infoValid=true;
    }
    
    public boolean start(long offset){
    	if(!infoValid)return false;
    	if(started.compareAndSet(false, true)){
    		new Thread(
    			()->{
	    			try {
	    				this.running=true;
						this.run(offset);
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
	    		}
    		).start();
    		return true;
    	}else{
    		//started once
    		return false;
    	}
    }
    
    public void stop(){
    	this.running=false;
    }

	private void run(long offset) throws Exception {
        // find the meta data about the topic and partition we are interested in
        //
        SocketInfo lead = findLeader(replicaBrokers, topic, partition);
        if(lead==null){
        	return;
        }
        String clientName = "Client_" + topic + "_" + partition;
 
        SimpleConsumer consumer = new SimpleConsumer(lead.getHost(), lead.getPort(), 100000, 64 * 1024, clientName);
        long readOffset = getLastOffset(consumer,topic, partition, offset<0?kafka.api.OffsetRequest.LatestTime():kafka.api.OffsetRequest.EarliestTime(), clientName);
        if(offset>=0)
	        if(offset<readOffset){
	        	log.warn((readOffset-offset)+" messages are lost");
	        }else{
	        	readOffset=offset;
	        }
        int numErrors = 0;
        while (running) {
            if (consumer == null) {
                consumer = new SimpleConsumer(lead.getHost(), lead.getPort(), 100000, 64 * 1024, clientName);
            }
            FetchRequest req = new FetchRequestBuilder()
                    .clientId(clientName)
                    .addFetch(topic, partition, readOffset, 100000) // Note: this fetchSize of 100000 might need to be increased if large batches are written to Kafka
                    .build();
            FetchResponse fetchResponse = consumer.fetch(req);
 
            if (fetchResponse.hasError()) {
                numErrors++;
                // Something went wrong!
                short code = fetchResponse.errorCode(topic, partition);
                log.warn("Error fetching data from the Broker:" + lead.toString() + " Code: " + code + " Reason: " + KafkaV8Error.get(code));
                if (numErrors > 5) break;
                if (code == ErrorMapping.OffsetOutOfRangeCode())  {
                    // We asked for an invalid offset. For simple case ask for the last element to reset
                    readOffset = getLastOffset(consumer,topic, partition, kafka.api.OffsetRequest.LatestTime(), clientName);
                    continue;
                }
                consumer.close();
                consumer = null;
                lead = findNewLeader(lead, topic, partition);
                continue;
            }
            numErrors = 0;
 
            long numRead = 0;
            for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(topic, partition)) {
            	long currentOffset = messageAndOffset.offset();
                log.trace("currentOffset:"+currentOffset);
                if (currentOffset < readOffset) {
                    log.warn("Found an old offset: " + currentOffset + " Expecting: " + readOffset);
                    continue;
                }
                readOffset = messageAndOffset.nextOffset();
                Message msg = messageAndOffset.message();
                if(!msg.hasKey()){
                	log.warn("Expecting key as class full name");
                	continue;
                }
                ByteBuffer key = msg.key();
                String cls = null;
                try{
                	cls = new String(key.array(),key.arrayOffset(),key.limit(),"UTF-8");
                }catch(Exception e){
                	log.warn("Converting class full name failed",e);
                	continue;
                }
                if(key.capacity()-key.limit()<=4){
                	log.warn("Might be a null object of "+cls);
                	continue;
                }
                Class<?> clz = PSSerializer.getInstance().getClassByName(cls);
                if(clz!=null){
                	@SuppressWarnings("unchecked")
					Handler<Object> h = this.handlerMap.get(clz);
                	if(h!=null){
	                	Object object = PSSerializer.getInstance().deser(key.arrayOffset()+key.limit()+4, key.capacity()-4-key.limit(), key.array(), clz);
                    	h.handle(currentOffset,object);
                	}else if(this.defaultHandler!=null){
                    	log.warn("no handler defined for "+clz+", use default handler instead");
                    	Object object = PSSerializer.getInstance().deser(key.arrayOffset()+key.limit()+4, key.capacity()-4-key.limit(), key.array(), clz);
                    	this.defaultHandler.handle(currentOffset,object);
                	}else{
                    	log.warn("no handler defined for "+clz+", and no default handler defined either");
                    }
                }else{
                	log.warn("no class is register for serialization "+cls);
                }
                numRead++;
            }
 
            if (numRead == 0) {
                try {
                	Thread.sleep(1000);
                } catch (InterruptedException ie) {
                }
            }
        }
        if (consumer != null) consumer.close();
    }
 
    public static long getLastOffset(SimpleConsumer consumer, String topic, int partition,
                                     long whichTime, String clientName) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
        kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(
                requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
        OffsetResponse response = consumer.getOffsetsBefore(request);
 
        if (response.hasError()) {
            log.warn("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition) );
            return 0;
        }
        long[] offsets = response.offsets(topic, partition);
        return offsets[0];
    }
 
    private SocketInfo findNewLeader(SocketInfo oldLead, String topic, int partition) throws Exception {
        for (int i = 0; i < 3; i++) {
            boolean goToSleep = false;
            SocketInfo lead = findLeader(replicaBrokers, topic, partition);
            if (lead == null) {
                goToSleep = true;
            } else if (oldLead.getHost().equalsIgnoreCase(lead.getHost()) && oldLead.getPort()==lead.getPort() && i == 0) {
                // first time through if the leader hasn't changed give ZooKeeper a second to recover
                // second time, assume the broker did recover before failover, or it was a non-Broker issue
                //
                goToSleep = true;
            } else {
                return lead;
            }
            if (goToSleep) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                }
            }
        }
        log.error("Unable to find new leader after Broker failure. Exiting");
        throw new Exception("Unable to find new leader after Broker failure. Exiting");
    }
 
    private SocketInfo findLeader(List<SocketInfo> brokers, String topic, int partition) {
        PartitionMetadata returnMetaData = null;
        for (SocketInfo broker : brokers) {
            String seed=broker.getHost();
            int port=broker.getPort();
        	SimpleConsumer consumer = null;
            try {
                consumer = new SimpleConsumer(seed, port, 100000, 64 * 1024, "leaderLookup");
                List<String> topics = Collections.singletonList(topic);
                TopicMetadataRequest req = new TopicMetadataRequest(topics);
                kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);
 
                List<TopicMetadata> metaData = resp.topicsMetadata();
                for (TopicMetadata item : metaData) {
                    for (PartitionMetadata part : item.partitionsMetadata()) {
                        if (part.partitionId() == partition) {
                            returnMetaData = part;
                            break;
                        }
                    }
                    if(returnMetaData!=null)break;
                }
            } catch (Exception e) {
                log.error("Error communicating with Broker [" + seed + "] to find Leader for [" + topic
                        + ", " + partition + "]", e);
            } finally {
                if (consumer != null) consumer.close();
            }
            if(returnMetaData!=null)break;
        }
        if (returnMetaData != null) {
            replicaBrokers.clear();
            for (kafka.cluster.Broker replica : returnMetaData.replicas()) {
                replicaBrokers.add(new SocketInfo(replica.host(),replica.port()));
            }
            if(returnMetaData.leader()!=null)
            	return new SocketInfo(returnMetaData.leader().host(),returnMetaData.leader().port());
            else{
            	log.error("Can't find Leader for Topic and Partition. Exiting");
            	return null;
            }
        }else{
            log.error("Can't find metadata for Topic and Partition. Exiting");
        	return null;
        }
    }
    
    private static class SocketInfo{
    	private String host;
    	private int port;
		public SocketInfo(String host, int port) {
			super();
			this.host = host;
			this.port = port;
		}
		public String getHost() {
			return host;
		}
		public int getPort() {
			return port;
		}
		@Override
		public String toString() {
			return "SocketInfo [host=" + host + ", port=" + port + "]";
		}
    }
}
