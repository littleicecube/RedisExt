package com.palace.sketch.redisext.beans;

public class Entity {

	private String id;
	private long deathline;
	private String data;
	
	public Entity() {}
	public Entity(String id,long deathline,String msg) {
		this.id = id;
		this.deathline = deathline;
		this.data = msg;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public long getDeathline() {
		return deathline;
	}
	public void setDeathline(long deathline) {
		this.deathline = deathline;
	}
	public String getData() {
		return data;
	}
	public void setData(String data) {
		this.data = data;
	}
	
}
