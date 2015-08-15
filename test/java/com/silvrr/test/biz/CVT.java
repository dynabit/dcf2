package com.silvrr.test.biz;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.silvrr.framework.kvs.ASDConverter.BinsOf;
import com.silvrr.framework.kvs.ASDConverter.KeyOf;

public class CVT{
	@SuppressWarnings("unchecked")
	public static<T> T cast(Object o){
		try{
			return (T) o;
		}catch(Exception e){
			//e.printStackTrace();
			return null;
		}
	}
	public static Account toAcct(Record record){
		if(record==null)return null;
		Account acct = new Account();
		acct.uid = cast(record.bins.get("uid"));
		acct.balance = cast(record.bins.get("balance"));
		acct.offset = cast(record.bins.get("offset"));
		return acct;
	}
	public static BinsOf<Account> binsOfAccount = (acct)->{
		return new Bin[]{ new Bin("uid",acct.uid),
		new Bin("balance",acct.balance),
		new Bin("offset",acct.offset)};
	};
	public static KeyOf<Account> keyOfAccount = (acct)->{
		return new Key("test","partition0",acct.uid);
	};
}
