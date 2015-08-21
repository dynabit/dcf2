package com.silvrr.test.biz;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.aerospike.client.Key;
import com.silvrr.framework.kvs.ASDKVS;
import com.silvrr.framework.mq.KProducer;
import com.silvrr.framework.serialize.PSSerializer;

public class TestSender {
	public static void main(String[] args) throws Exception {
		if(args.length!=5){
			System.out.println("args: kafkaIp,aerospikeIp,cnt,sleep,batch");
			return;
		}
		int cnt = -1;
		int sleep  = -1;
		int batch = -1;
		try{
			cnt=Integer.parseInt(args[2]);
			sleep=Integer.parseInt(args[3]);
			batch=Integer.parseInt(args[4]);
		}catch(Exception e){
			System.out.println("args: kafkaIp,aerospikeIp,cnt,sleep,batch");
			return;
		}
		PSSerializer.getInstance().register(OperateRequest.class);
		PSSerializer.getInstance().register(String.class);
		ASDKVS kvs = ASDKVS.initInstance(args[1], 3000);
		Account acct = null;
		try{
			acct = CVT.toAcct(kvs.get(new Key("test","partition0",0L)));
		}catch(Exception e){
			e.printStackTrace();
		}
		
//		long[] ops = new long[]{+30,-10,+25,+15,-1,-8};
		int total = cnt;
		List<Long> ops = new ArrayList<Long>(total);
		Random rand = new Random();
		for(int i=0;i<total;i++){
			ops.add((long)(rand.nextInt(100)-50));
		}
		
		long balance = (acct==null?0L:acct.balance);
		for(int i=0;i<ops.size();i++){
			if(i%100==0)balance+=ops.get(i);
		}
		Thread.sleep(100L);
		System.out.println("final bal for acct[0] should be "+balance);
		
		KProducer producer = KProducer.getInstance(args[0]);
		OperateRequest r = new OperateRequest();
		r.txID="";
		long begin=System.currentTimeMillis();
		for(int i=0;i<ops.size();i++){
			r.uid=(long)i%100;r.amount=ops.get(i);r.ts = System.currentTimeMillis();
			producer.send("stateChange", r, (req)->{return 0;}, 
					(metadata, exception)->{
						if(exception!=null)
							exception.printStackTrace();
					});
			if(i%batch==0)Thread.sleep(sleep);
		}
		long end = System.currentTimeMillis();
		producer.send("stateChange", "stop", (cmd)->{return 0;}, (metadata, exception)->{
			if(exception!=null)
				exception.printStackTrace();
		});
		System.out.println("ttlMs:"+(end-begin)+",cnt:"+total+",avg"+((double)(end-begin)/(double)total)+",throughput:"+((double)total/((double)(end-begin)/1000.0)));
		Thread.sleep(1000L);
	}
}
