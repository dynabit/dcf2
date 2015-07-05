package com.silvrr.framework.mq;

public interface Handler<E> {
	public void handle(long offset,E message);
}
