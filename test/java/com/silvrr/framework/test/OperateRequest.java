package com.silvrr.framework.test;

public class OperateRequest {
	public Long uid;
	public String message;
	public String txID;
	public int amount;
	@Override
	public String toString() {
		return "OperateRequest [uid=" + uid + ", message=" + message + ", txID="
				+ txID + ", amount=" + amount + "]";
	}
}
