package com.silvrr.test.biz;

import com.silvrr.framework.mq.KProducer;
import com.silvrr.framework.serialize.PSSerializer;

public class TestSender {
	public static void main(String[] args) throws Exception {
		PSSerializer.getInstance().register(OperateRequest.class);
		long[] ops = new long[]{+30,-10,+25,+15,-1,-8};
		
		long balance = 0;
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
						System.out.println("cf:"+System.currentTimeMillis());
						System.out.println(String.format("sendCallback:md:%s %d %d",
								metadata.topic(),
								metadata.partition(),metadata.offset()));
						System.out.println("sendCallback:e:"+exception);
					});
			Thread.sleep(5000L);
		}
	}
}
