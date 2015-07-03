package com.silvrr.framework.kvs;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.silvrr.framework.OperateRequest;

public class ASDObjectConverter {
	public static interface BinsOf<T>{
		public Bin[] of(T obj,long offset);
	}
	public static interface KeyOf<T>{
		public Key of(T obj);
	}
	public static BinsOf<OperateRequest> binsOfOperateRequest = (req,offset)->{
		return new Bin[]{ new Bin("uid",req.uid),
		new Bin("message",req.message),
		new Bin("txID",req.txID),
		new Bin("amount",req.amount),
		new Bin("offset",offset)};
	};
	public static KeyOf<OperateRequest> keyOfOperateRequest = (req)->{
		return new Key("test","partition0",req.uid);
	};
			
}
