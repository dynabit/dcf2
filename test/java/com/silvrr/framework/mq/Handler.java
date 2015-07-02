package com.silvrr.framework.mq;

public interface Handler<E> {
	public void handle(E message);
}
