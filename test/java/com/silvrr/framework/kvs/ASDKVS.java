package com.silvrr.framework.kvs;

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
	public static ASDKVS getInstance(){
		return instance;
	}
	
	private AsyncClient client = new AsyncClient("192.168.1.15", 3000);
	private WritePolicy wPolicy = new WritePolicy();
	private ScanPolicy sPolicy = new ScanPolicy();
	
	private ASDKVS(){
		wPolicy.commitLevel=CommitLevel.COMMIT_MASTER;
		wPolicy.sendKey=true;
		sPolicy.failOnClusterChange=true;
	}
	
	@Override
	protected void finalize(){
		this.client.close();
	}
	
	public <T> void fire(T obj,KeyOf<T> key,BinsOf<T> bins){
		this.client.put(wPolicy,null,key.of(obj),bins.of(obj));
	}
	
	public void scanAll(String namespace,String set,ScanCallback callback){
		this.client.scanAll(this.sPolicy, namespace, set, callback);
	}
}
