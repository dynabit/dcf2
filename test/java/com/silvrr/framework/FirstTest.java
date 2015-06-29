package com.silvrr.framework;

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
		r.amount=100;r.id=1L;r.message="heihei";r.txID="txid01";
		byte[] data = PSSerializer.getInstance().ser(r);
		print("tosend:",data,0,data.length);
		boolean exit=false;
		if(exit)return;
		TestConsumer c = new TestConsumer("stateChange",0,9092,"localhost");
		c.on(OperateRequest.class, (req)->{System.out.println(req.toString());});
		c.start(6);
		Thread.sleep(5000);
		exit=true;
		if(exit)return;
		//c.stop();
		TestProducer.getInstance().send("stateChange", r, (req)->{return 0;}, 
				(metadata, exception)->{
					System.out.println(String.format("sendCallback:md:%s %d %d",metadata.topic(),
							metadata.partition(),metadata.offset()));
					System.out.println("sendCallback:e:"+exception);
				});
		
    }

}
