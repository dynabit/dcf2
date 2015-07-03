package com.silvrr.framework.kvs;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.async.AsyncClient;
import com.aerospike.client.policy.CommitLevel;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.silvrr.framework.OperateRequest;
import com.silvrr.framework.kvs.ASDObjectConverter.BinsOf;
import com.silvrr.framework.kvs.ASDObjectConverter.KeyOf;

import static com.silvrr.framework.kvs.AerospikeCallback.SeperateWriteCallback.*;
import static com.silvrr.framework.kvs.AerospikeCallback.SingleWriteCallback.*;
import static com.silvrr.framework.kvs.ASDObjectConverter.*;

public class TestKVS {
	
	private static TestKVS instance=new TestKVS();
	public static TestKVS getInstance(){
		return instance;
	}
	
	private AsyncClient client = new AsyncClient("localhost", 3000);
	private WritePolicy wPolicy = new WritePolicy();
	private ScanPolicy sPolicy = new ScanPolicy();
	
	private TestKVS(){
		wPolicy.commitLevel=CommitLevel.COMMIT_MASTER;
	}
	
	@Override
	protected void finalize(){
		this.client.close();
	}
	
	public <T> void fire(long offset,T obj,KeyOf<T> key,BinsOf<T> bins){
		this.client.put(wPolicy,null,key.of(obj),bins.of(obj,offset));
	}
	
	public static void main(String[] args) throws Exception {
		OperateRequest r = new OperateRequest();
		r.amount=100;r.uid=1L;r.message="heihei";r.txID="txid01";
		TestKVS.getInstance().fire(1,r,keyOfOperateRequest,binsOfOperateRequest);
		Thread.sleep(5000L);
	}
	
	public static void rawTest() throws Exception{
		OperateRequest r = new OperateRequest();
		r.amount=100;r.uid=1L;r.message="heihei";r.txID="txid01";
		AsyncClient client = new AsyncClient("localhost", 3000);
		try{
			WritePolicy policy = new WritePolicy();
			policy.commitLevel=CommitLevel.COMMIT_MASTER;
			Key key = new Key("test","partition0",r.uid);
			Bin amt = new Bin("amount",100);
			Bin bal = new Bin("balance",100);
			Bin offset = new Bin("offset",1);
			
			client.put(policy, 
					success((donekey)->{
						System.out.println(donekey.toString()+" created successfully in seperate write callback");
					})
					.failure((exception)->{
						exception.printStackTrace();
					}),
					key, amt, bal, offset);
			client.put(policy, 
					done((donekey,exception)->{
						if(donekey!=null){
							System.out.println(donekey.toString()+" created successfully in single write callback");
						}else if(exception!=null){
							exception.printStackTrace();
						}else{
							throw new RuntimeException("this should never happen");
						}
					}),
					key, amt, bal, offset);
		}finally{
			Thread.sleep(5000);
			client.close();
		}
	}
}
