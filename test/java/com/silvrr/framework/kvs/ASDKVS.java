package com.silvrr.framework.kvs;

import java.util.concurrent.atomic.AtomicBoolean;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.async.AsyncClient;
import com.aerospike.client.policy.CommitLevel;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.silvrr.framework.kvs.ASDConverter.BinsOf;
import com.silvrr.framework.kvs.ASDConverter.KeyOf;
/**
 * Aerospike kvs
 * @author gordon
 *
 */
public class ASDKVS {
	
	private static ASDKVS instance=new ASDKVS();
	public static ASDKVS initInstance(String hostname,int port){
		instance.init(hostname, port);
		return instance;
	}
	public static ASDKVS getInstance(){
		if(instance.inited.get()==false){
			throw new RuntimeException("ASDKVS not yet inited with hostname and port");
		}
		return instance;
	}
	private AtomicBoolean inited= new AtomicBoolean(false);
	
	private AsyncClient client = null;
	private WritePolicy wPolicy = new WritePolicy();
	private ScanPolicy sPolicy = new ScanPolicy();
	
	private ASDKVS(){
		wPolicy.commitLevel=CommitLevel.COMMIT_MASTER;
		wPolicy.sendKey=true;
		sPolicy.failOnClusterChange=true;
	}
	
	private void init(String hostname,int port){
		if(inited.compareAndSet(false, true)){
			this.client = new AsyncClient(hostname, port);
		}
	}
	
	@Override
	protected void finalize(){
		System.out.println("errorno of sending to aero spike is : "+err);
		this.client.close();
	}
	private int err=0;
	public <T> void fire(T obj,KeyOf<T> key,BinsOf<T> bins){
		this.client.put(wPolicy,new com.aerospike.client.listener.WriteListener(){
			@Override
			public void onSuccess(Key key) {
				// TODO Auto-generated method stub
				
			}

			@Override
			public void onFailure(AerospikeException exception) {
				// TODO Auto-generated method stub
				err++;System.out.println(".");
				if(err%10==0)System.out.println(exception.getMessage());
			}},key.of(obj),bins.of(obj));
	}
	
	public void scanAll(String namespace,String set,ScanCallback callback){
		this.client.scanAll(this.sPolicy, namespace, set, callback);
	}
	
	public void delete(Key key){
		this.client.delete(wPolicy, key);
	}
	
	public Record get(Key key){
		return this.client.get(null, key);
	}
}
