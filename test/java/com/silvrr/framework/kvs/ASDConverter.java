package com.silvrr.framework.kvs;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;

public class ASDConverter {
	public static interface BinsOf<T>{
		public Bin[] of(T obj);
	}
	public static interface KeyOf<T>{
		public Key of(T obj);
	}		
}
