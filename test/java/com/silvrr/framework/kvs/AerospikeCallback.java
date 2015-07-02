package com.silvrr.framework.kvs;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.listener.WriteListener;

public class AerospikeCallback {
	public static class SeperateWriteCallback{
		public static LambdaSperateWriteListener success(WriteSuccess success){
			return new LambdaSperateWriteListener().success(success);
		}
		public static LambdaSperateWriteListener failure(WriteFailure failure){
			return new LambdaSperateWriteListener().failure(failure);
		}
	}
	
	public static interface WriteSuccess{
		public void onSuccess(Key key);
	}
	public static interface WriteFailure{
		public void onFailure(AerospikeException exception);
	}
	
	public static class LambdaSperateWriteListener implements WriteListener{
		private WriteSuccess success;
		private WriteFailure failure;
		public LambdaSperateWriteListener success(WriteSuccess success){
			this.success = success;
			return this;
		}
		public LambdaSperateWriteListener failure(WriteFailure failure){
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
	
	public static class SingleWriteCallback{
		public static LambdaSingleWriteListener done(WriteDone done){
			return new LambdaSingleWriteListener(done);
		}
	}
	
	public static interface WriteDone{
		public void onDone(Key key,AerospikeException exception);
	}
	
	public static class LambdaSingleWriteListener implements WriteListener{
		private WriteDone done;
		private LambdaSingleWriteListener(){;}
		public LambdaSingleWriteListener(WriteDone done){
			this();
			this.done=done;
		}
		@Override
		public void onSuccess(Key key) {
			if(this.done!=null)this.done.onDone(key, null);
		}

		@Override
		public void onFailure(AerospikeException exception) {
			if(this.done!=null)this.done.onDone(null, exception);
		}
		
	}
}
