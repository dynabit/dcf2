package com.silvrr.test.biz;

public class Account{
	public Long uid;
	public Long balance=0L;
	public Long offset;
	@Override
	public String toString() {
		return "Account [uid=" + uid + ", balance=" + balance + ", offset="
				+ offset + "]";
	}
	
}
