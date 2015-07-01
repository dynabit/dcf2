package com.silvrr.framework;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.async.AsyncClient;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.CommitLevel;
import com.aerospike.client.policy.WritePolicy;

public class TestKV {
	
	public static void main(String[] args) throws Exception {
		OperateRequest r = new OperateRequest();
		r.amount=100;r.uid=1L;r.message="heihei";r.txID="txid01";
		AsyncClient client = new AsyncClient("192.168.1.15", 3000);
		WritePolicy policy = new WritePolicy();
		policy.commitLevel=CommitLevel.COMMIT_MASTER;
		Key key = new Key("stateChange","partition0",r.uid);
		Bin amt = new Bin("amount",100);
		Bin bal = new Bin("balance",100);
		Bin offset = new Bin("offset",1);
		client.put(policy, 
				CB.success((donekey)->{
					System.out.println(donekey.toString()+" created successfully");
					client.close();
				})
				.failure((exception)->{exception.printStackTrace();}),
				key, amt,bal,offset);
		Thread.sleep(5000);
	}
	
	static class CB{
		public static LambdaWriteListener success(WriteSuccess success){
			return new LambdaWriteListener().success(success);
		}
		public static LambdaWriteListener failure(WriteFailure failure){
			return new LambdaWriteListener().failure(failure);
		}
	}
	
	private static interface WriteSuccess{
		public void onSuccess(Key key);
	}
	private static interface WriteFailure{
		public void onFailure(AerospikeException exception);
	}
	
	private static class LambdaWriteListener implements WriteListener{
		private WriteSuccess success;
		private WriteFailure failure;
		public LambdaWriteListener success(WriteSuccess success){
			this.success = success;
			return this;
		}
		public LambdaWriteListener failure(WriteFailure failure){
			this.failure = failure;
			return this;
		}
		@Override
		public void onSuccess(Key key) {
			if(this.success!=null)this.success.onSuccess(key);
		}

		@Override
		public void onFailure(AerospikeException exception) {
			if(this.failure!=null)this.failure.onFailure(exception);
		}
		
	}
}
