package com.silvrr.framework.test;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.silvrr.framework.kvs.ASDConverter.BinsOf;
import com.silvrr.framework.kvs.ASDConverter.KeyOf;
import com.silvrr.test.biz.OperateRequest;

public class KVSConverter {
	public static BinsOf<OperateRequest> binsOfOperateRequest = (req)->{
		return new Bin[]{ new Bin("uid",req.uid),
		new Bin("message",req.message),
		new Bin("txID",req.txID),
		new Bin("amount",req.amount)};
	};
	public static KeyOf<OperateRequest> keyOfOperateRequest = (req)->{
		return new Key("test","partition0",req.uid);
	};
}
