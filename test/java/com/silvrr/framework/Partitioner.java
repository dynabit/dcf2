package com.silvrr.framework;

public interface Partitioner<T> {
	public int getPartition(T obj);
}
