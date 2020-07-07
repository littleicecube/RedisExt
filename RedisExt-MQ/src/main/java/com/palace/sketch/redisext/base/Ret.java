package com.palace.sketch.redisext.base;

public class Ret {

	static int SUCC = 0;
	static int ERROR = -1;
	
	private int code;
	private String msg;
	private Object object;
	public Ret(int code,String msg,Object object) {
		this.code = code;
		this.msg = msg;
		this.object = object;
	}
	
	public static Ret succ() {
		return new Ret(SUCC,null,null);
	}
	
	public static Ret err(String msg) {
		return new Ret(ERROR,msg,null);
	}
}
