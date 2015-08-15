package com.silvrr.test.biz;

import java.util.HashMap;
import java.util.Map;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.silvrr.framework.kvs.ASDKVS;
import com.silvrr.framework.mq.KConsumer;
import com.silvrr.framework.serialize.PSSerializer;

import static com.silvrr.test.biz.CVT.*;

public class TestNode {
	
	private Map<Long,Account> accounts = new HashMap<Long,Account>();
	private KConsumer consumer = new KConsumer("stateChange", 0, 9092, "localhost");
	private ASDKVS kvs = ASDKVS.initInstance("localhost", 3000);
	private long startFromOffset=-1;
	
	public TestNode(){
		init();
	}
	private void init(){
		System.out.println("begin scan all");
		this.kvs.scanAll("test", "partition0", (Key key, Record record)->{
			Account acct = toAcct(record);
			System.out.println("fetched record : "+acct+" @ "+key);
			this.accounts.put(acct.uid, acct);
			this.startFromOffset=Math.max(this.startFromOffset, acct.offset);
		});
		this.kvs.scanAll("test", "partition0R", (Key key, Record record)->{
			long offset = CVT.cast(record.bins.get("offset"));
			System.out.println("fetched record : offset : "+offset);
			this.startFromOffset=Math.max(this.startFromOffset, offset);
		});
		this.startFromOffset+=1;
		System.out.println("end scan all");
	}
	public void run(){
		consumer.on(OperateRequest.class, (offset,req)->{
			System.out.println("recved "+req+",offset:"+offset);
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
				System.out.println("sent "+acct);
			}
		}).on(String.class, (offset,req)->{
			if(req.equalsIgnoreCase("stop")){
				this.kvs.fire("stop",(cmd)->{return new Key("test","partition0R",cmd);},(cmd)->{return new Bin[]{new Bin("offset",offset)};});
				System.out.println(this.accounts);
				consumer.stop();
			}
		}).onDefault((offset,req)->{
			System.out.println("no handling logic defined for "+offset+" "+req.toString());
		}).start(this.startFromOffset);
	}
	public static void main(String[] args) {
//		ASDKVS.initInstance("localhost", 3000).delete(new Key("test","partition0",1L));
//		if(true)return;
		PSSerializer.getInstance().register(OperateRequest.class);
		PSSerializer.getInstance().register(String.class);
		TestNode node=new TestNode();
		node.run();
	}
}
