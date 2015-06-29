package com.silvrr.framework;

public interface Handler<E> {
	public void handle(E message);
}
