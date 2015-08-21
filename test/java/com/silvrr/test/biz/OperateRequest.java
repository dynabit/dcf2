package com.silvrr.test.biz;

public class OperateRequest {
	public Long uid;
	public String message;
	public Long ts;
	public String txID;
	public long amount;
	@Override
	public String toString() {
		return "OperateRequest [uid=" + uid + ", message=" + message + ", txID="
				+ txID + ", amount=" + amount + "]";
	}
}
