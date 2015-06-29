package com.silvrr.framework;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.runtime.RuntimeSchema;

public class PSSerializer {
	
	private static Logger log = LoggerFactory.getLogger(PSSerializer.class);
	private static ThreadLocal<LinkedBuffer> th=new ThreadLocal<LinkedBuffer>();
	private static PSSerializer instance=new PSSerializer();
	
	private Map<String,Class<?>> clzMap=new HashMap<String,Class<?>>();
	private List<Class<?>> id2clz=new ArrayList<Class<?>>();
	private Map<Class<?>,Integer> clz2id=new HashMap<Class<?>,Integer>();

	private PSSerializer(){
		;
	}

	public static PSSerializer getInstance(){
		return instance;
	}
	
	private LinkedBuffer getApplicationBuffer(){
		LinkedBuffer lb=th.get();
		if(lb==null){
			lb = LinkedBuffer.allocate(65536);
			th.set(lb);
		}
		return lb;
	}
	
	public void register(Class<?> clz){
		RuntimeSchema.getSchema(clz);
		id2clz.add(clz);
		clz2id.put(clz, id2clz.size()-1);
		clzMap.put(clz.getName(),clz);
	}

	public <T> byte[] ser(Object o){
		if(!clz2id.containsKey(o.getClass()))return null;
		//if(!clzMap.containsKey(o.getClass().getName()))return null;
		return ser(o,o.getClass());
	}
	@SuppressWarnings("unchecked")
	private <T> byte[] ser(Object o,Class<T> cls){
		LinkedBuffer lb = getApplicationBuffer();
		try{
			byte[] protostuff = ProtostuffIOUtil.toByteArray((T)o, RuntimeSchema.getSchema(cls), lb);
			return protostuff;
		}finally{
		    lb.clear();
		}
	}
	public <T> T deser(byte[] data,Class<T> cls){
		return deser(0,data.length,data,cls);
	}
	public <T> T deser(int offset,int length,byte[] data,Class<T> cls){
		if(data==null)return null;
		if(!clz2id.containsKey(cls))return null;
		T f = RuntimeSchema.getSchema(cls).newMessage();
		ProtostuffIOUtil.mergeFrom(data,offset,length, f, RuntimeSchema.getSchema(cls));
		return f;
	}
	public Object deser(int offset,int length,byte[] buffer,String clz){
		if(buffer==null)return null;
		Class<?> cls = clzMap.get(clz);
		if(cls==null){
			log.warn("deser unknown type : "+clz);
			return null;
		}else
			return deser(offset,length,buffer,cls);
	}
	public Object deser(byte[] buffer,int clzId){
		return deser(0,buffer.length,buffer,clzId);
	}
	public Object deser(int offset,int length,byte[] buffer,int clzId){
		if(buffer==null)return null;
		Class<?> cls = getClassById(clzId);
		if(cls==null){
			log.warn("deser unknown type : "+clzId);
			return null;
		}else
			return deser(offset,length,buffer,cls);
	}
	public int getClassId(Class<?> clz){
		Integer id = clz2id.get(clz);
		return id==null?-1:id.intValue();
	}
	public Class<?> getClassById(int id){
		if(id>id2clz.size()-1){
			return null;
		}else{
			return id2clz.get(id);
		}
	}
	public Class<?> getClassByName(String cls){
		return clzMap.get(cls);
	}
	public Object deser(byte[] buffer,String clz){
		return deser(0,buffer.length,buffer,clz);
	}
}