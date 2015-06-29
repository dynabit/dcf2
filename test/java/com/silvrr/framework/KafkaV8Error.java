package com.silvrr.framework;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import kafka.common.ErrorMapping;

public class KafkaV8Error {
	private KafkaV8Error(){;}
	public static Map<Short,String> errorMap = new HashMap<Short,String>();
	static{
		for(java.lang.reflect.Method m:ErrorMapping.class.getDeclaredMethods()){
			if(java.lang.reflect.Modifier.isStatic(m.getModifiers())&&
					java.lang.reflect.Modifier.isPublic(m.getModifiers())&&
					m.getParameterCount()==0&&m.getReturnType().equals(short.class))
				try {
					errorMap.put((short)m.invoke(null),m.getName());
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
	}
	public static String get(Short code){
		String msg = errorMap.get(code);
		return msg==null?"unknownCode":msg;
	}
}
