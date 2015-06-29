package com.silvrr.framework;

public class OperateRequest {
	public Long id;
	public String message;
	public String txID;
	public int amount;
	@Override
	public String toString() {
		return "OperateRequest [id=" + id + ", message=" + message + ", txID="
				+ txID + ", amount=" + amount + "]";
	}
}
