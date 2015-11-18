package com.silvrr.test.biz;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.silvrr.framework.kvs.ASDKVS;
import com.silvrr.framework.mq.KConsumer;
import com.silvrr.framework.serialize.PSSerializer;

import static com.silvrr.test.biz.CVT.*;

public class TestNode {
	
	private Map<Long,Account> accounts = new HashMap<Long,Account>();
	private KConsumer consumer = null;//new KConsumer("stateChange", 0, 9092, "localhost");
	private ASDKVS kvs = null;//ASDKVS.initInstance("localhost", 3000);
	private long startFromOffset=-1;
	private List<Long> ts1 = new ArrayList<Long>(1100000);
	private List<Long> ts2 = new ArrayList<Long>(1100000);
	
	private Long start = 0l;
	
	public TestNode(String kafkaIp,String aerospikeIp){
		init(kafkaIp,aerospikeIp);
	}
	private void init(String kafkaIp,String aerospikeIp){
		consumer = new KConsumer("stateChange", 0, 9092, kafkaIp);
		kvs = ASDKVS.initInstance(aerospikeIp, 3000);
		System.out.println("begin scan all");
		try{
		this.kvs.scanAll("test", "partition0", (Key key, Record record)->{
			Account acct = toAcct(record);
			if(acct.uid==0L)System.out.println("fetched record : "+acct+" @ "+key);
			this.accounts.put(acct.uid, acct);
			this.startFromOffset=Math.max(this.startFromOffset, acct.offset);
		});
		this.kvs.scanAll("test", "partition0R", (Key key, Record record)->{
			long offset = CVT.cast(record.bins.get("offset"));
			System.out.println("fetched record : offset : "+offset);
			this.startFromOffset=Math.max(this.startFromOffset, offset);
		});
		}catch(Exception e){
			e.printStackTrace();
		}
		this.startFromOffset+=1;
		System.out.println("end scan all");
	}
	public void run(){
		consumer.on(OperateRequest.class, (offset,req)->{
			ts1.add(System.currentTimeMillis()-req.ts);
			if(start==0){
				start=System.currentTimeMillis();
				System.out.println("started!!##");
			}
			//if(true)return;
			//System.out.println("recved "+req+",offset:"+offset);
			Account acct = accounts.get(req.uid);
			if(acct==null){
				acct=new Account();
				acct.uid=req.uid;
				acct.offset=-1L;
				accounts.put(acct.uid, acct);
			}
			if(offset>acct.offset){
				//only handle if offset is newer
				acct.balance+=req.amount;
				acct.offset=offset;
				this.kvs.fire(acct,keyOfAccount,binsOfAccount);
				//System.out.println("sent "+acct);
			}
			ts2.add(System.currentTimeMillis()-req.ts);
		}).on(String.class, (offset,req)->{
			if(req.equalsIgnoreCase("stop")){
				this.kvs.fire("stop",(cmd)->{return new Key("test","partition0R",cmd);},(cmd)->{return new Bin[]{new Bin("offset",offset)};});
				System.out.println( "stopped, total time = "+(System.currentTimeMillis()-start)+", "+ this.accounts.get(0L));
				consumer.stop();
				DescriptiveStatistics stats = getStats(ts1,(int)(ts1.size()*0.2));
				DescriptiveStatistics stats2 = getStats(ts2,(int)(ts2.size()*0.2));
				System.out.println("##TS1## Mean:"+stats.getMean()+",Max:"+stats.getMax()+",Min:"+stats.getMin()+",50%:"+stats.getPercentile(50)
						+",90%:"+stats.getPercentile(90)+",99%:"+stats.getPercentile(99)+",99.9%:"+stats.getPercentile(99.9)+",Total:"+stats.getN());
				System.out.println("##TS2## Mean:"+stats2.getMean()+",Max:"+stats2.getMax()+",Min:"+stats2.getMin()+",50%:"+stats2.getPercentile(50)
						+",90%:"+stats2.getPercentile(90)+",99%:"+stats2.getPercentile(99)+",99.9%:"+stats2.getPercentile(99.9)+",Total:"+stats2.getN());
			}
		}).onDefault((offset,req)->{
			System.out.println("no handling logic defined for "+offset+" "+req.toString());
		}).start(this.startFromOffset);
	}
	private DescriptiveStatistics getStats(List<Long> data,int beginIdx){
		DescriptiveStatistics stats = new DescriptiveStatistics();
		int idx = 0;
		for(Long d:data){
			if(idx<beginIdx){idx++;continue;}
			stats.addValue((double)d);
			idx++;
		}
		return stats;
	}
	public static void main(String[] args) {
//		ASDKVS.initInstance("localhost", 3000).delete(new Key("test","partition0",1L));
//		if(true)return;
		if(args.length!=2){
			System.out.println("args: kafkaIp,aerospikeIp");
			return;
		}
		PSSerializer.getInstance().register(OperateRequest.class);
		PSSerializer.getInstance().register(String.class);
		TestNode node=new TestNode(args[0],args[1]);
		node.run();
	}
}
