package com.sketch.reidsext.mq.delayed;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.palace.sketch.redisext.beans.Entity;
import com.palace.sketch.redisext.mq.ConsumeStatus;
import com.palace.sketch.redisext.mq.Listener;
import com.palace.sketch.redisext.mq.delayed.DMQ;

public class DelayedMQTest {

	static DMQ consumer;
	static {
		consumer = new DMQ("127.0.0.1",6379,"sim");
	}
	
	@Test
	public void consumerTest() {
		consumer.setLisenter(new Listener() {
			@Override
			public ConsumeStatus onMessage(Entity entity) {
				String data = entity.getData();
				String date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
				System.err.println(data+";"+data);
				return null;
			}
		}).start();
		try {
			Thread.currentThread().sleep(9000000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	 
	@Test
	public void pullData() {
		
		consumer.pull();
	}
	@Test
	public void pushData() {
		for(int i=0;i<1;i++) {
			Entity ent = new Entity(i+"",System.currentTimeMillis()+(60*1000),data(i));
			consumer.pushData(ent);
			try {
				Thread.currentThread().sleep(5*1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	public String data(int index) {
		Map<String,Object> map = new HashMap();
		map.put("name","xiaoming"+index);
		map.put("age",13+index);
		map.put("state",2);
		String data = "";
		try {
			data = new ObjectMapper().writeValueAsString(map);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
		
		return data;
	}
}
