package com.silvrr.framework.mq;

public interface Partitioner<T> {
	public int getPartition(T obj);
}
