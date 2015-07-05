package com.silvrr.test.biz;

import java.util.HashMap;
import java.util.Map;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.silvrr.framework.kvs.ASDKVS;
import com.silvrr.framework.kvs.ASDConverter.BinsOf;
import com.silvrr.framework.kvs.ASDConverter.KeyOf;
import com.silvrr.framework.mq.KConsumer;

import static com.silvrr.test.biz.CVT.*;

class CVT{
	public static<T> T cast(Object o){
		try{
			return (T) o;
		}catch(Exception e){
			//e.printStackTrace();
			return null;
		}
	}
	public static Account toAcct(Record record){
		Account acct = new Account();
		acct.uid = cast(record.bins.get("uid"));
		acct.balance = cast(record.bins.get("balance"));
		acct.offset = cast(record.bins.get("offset"));
		return acct;
	}
	public static BinsOf<Account> binsOfAccount = (acct)->{
		return new Bin[]{ new Bin("uid",acct.uid),
		new Bin("balance",acct.balance),
		new Bin("offset",acct.offset)};
	};
	public static KeyOf<Account> keyOfAccount = (acct)->{
		return new Key("test","partition0",acct.uid);
	};
}

public class TestNode {
	
	private Map<Long,Account> accounts = new HashMap<Long,Account>();
	private KConsumer consumer = new KConsumer("stateChange", 0, 9092, "localhost");
	private ASDKVS kvs = ASDKVS.getInstance();
	private long startFromOffset=0;
	
	public TestNode(){
		init();
	}
	private void init(){
		System.out.println("begin scan all");
		this.kvs.scanAll("test", "partition0", (Key key, Record record)->{
			Account acct = toAcct(record);
			System.out.println("fetched record : "+acct);
			this.accounts.put(acct.uid, acct);
			this.startFromOffset=Math.max(this.startFromOffset, acct.offset);
		});
		System.out.println("end scan all");
	}
	public void run(){
		consumer.on(OperateRequest.class, (long offset,OperateRequest req)->{
			System.out.println("recved "+req);
			Account acct = accounts.get(req.uid);
			if(acct!=null){
				acct.balance+=req.amount;
			}else{
				acct=new Account();
				acct.balance=req.amount;
				acct.offset=offset;
				acct.uid=req.uid;
				accounts.put(acct.uid, acct);
			}
			ASDKVS.getInstance().fire(acct,keyOfAccount,binsOfAccount);
		}).onDefault((offset,req)->{
			System.out.println("no handling logic defined for "+offset+" "+req.toString());
		}).start(this.startFromOffset);
	}
	public static void main(String[] args) {
		TestNode node=new TestNode();
		node.run();
	}
}