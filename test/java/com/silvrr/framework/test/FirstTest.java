package com.silvrr.framework.test;

import com.silvrr.framework.mq.KConsumer;
import com.silvrr.framework.mq.KProducer;
import com.silvrr.framework.serialize.PSSerializer;
import com.silvrr.test.biz.OperateRequest;

public class FirstTest {
	public static void print(String prefix,byte[] bytes,int offset,int length){
		StringBuilder sb = new StringBuilder();
		int to = Math.min(offset+length,bytes.length);
		for(int i=offset;i<to;i++){
			sb.append((int)bytes[i]).append(',');
		}
		System.out.println(prefix+"length="+(to-offset)+"|"+sb.toString());
	}
	public static void main(String args[]) throws Exception {
		PSSerializer.getInstance().register(OperateRequest.class);
		OperateRequest r = new OperateRequest();
		r.amount=100;r.uid=1L;r.message="heihei";r.txID="txid01";

		KProducer.getInstance();
		KConsumer c = new KConsumer("stateChange",0,9092,"localhost");
		c.on(OperateRequest.class, (offset,req)->{
			System.out.println("rc:"+System.currentTimeMillis());
			System.out.println(req.toString());
		})
		.start(100);
		Thread.sleep(1000);
		System.out.println("bf:"+System.currentTimeMillis());
		KProducer.getInstance().send("stateChange", r, (req)->{return 0;}, 
				(metadata, exception)->{
					System.out.println("cf:"+System.currentTimeMillis());
					System.out.println(String.format("sendCallback:md:%s %d %d",
							metadata.topic(),
							metadata.partition(),metadata.offset()));
					System.out.println("sendCallback:e:"+exception);
				});
		System.out.println("af:"+System.currentTimeMillis());
		Thread.sleep(5000);
		c.stop();
    }

}
