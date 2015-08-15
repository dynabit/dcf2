package com.silvrr.test.biz;

import com.aerospike.client.Key;
import com.silvrr.framework.kvs.ASDKVS;
import com.silvrr.framework.mq.KProducer;
import com.silvrr.framework.serialize.PSSerializer;

public class TestSender {
	public static void main(String[] args) throws Exception {
		PSSerializer.getInstance().register(OperateRequest.class);
		PSSerializer.getInstance().register(String.class);
		ASDKVS kvs = ASDKVS.initInstance("localhost", 3000);
		Account acct = CVT.toAcct(kvs.get(new Key("test","partition0",1L)));
		
		long[] ops = new long[]{+30,-10,+25,+15,-1,-8};
		
		long balance = (acct==null?0L:acct.balance);
		for(long op:ops){
			balance+=op;
		}
		System.out.println("final bal should be "+balance);
		
		KProducer.getInstance();
		OperateRequest r = new OperateRequest();
		r.uid=1L;
		r.txID="";
		
		for(long op:ops){
			r.amount=op;r.message=""+System.currentTimeMillis();
			KProducer.getInstance().send("stateChange", r, (req)->{return 0;}, 
					(metadata, exception)->{
						if(exception!=null)
							exception.printStackTrace();
					});
			//Thread.sleep(5000L);
		}
		KProducer.getInstance().send("stateChange", "stop", (cmd)->{return 0;}, (metadata, exception)->{
			if(exception!=null)
				exception.printStackTrace();
		});
		Thread.sleep(5000L);
	}
}
