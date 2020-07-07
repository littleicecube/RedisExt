package com.palace.sketch.redisext.mq.delayed;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.palace.sketch.redisext.base.Ret;
import com.palace.sketch.redisext.beans.Entity;
import com.palace.sketch.redisext.mq.ConsumeStatus;
import com.palace.sketch.redisext.mq.DefaultRedisClient;
import com.palace.sketch.redisext.mq.IConsumer;
import com.palace.sketch.redisext.mq.Listener;

import io.lettuce.core.KeyValue;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.sync.RedisCommands;

public class DMQ implements IConsumer{
    final Logger log = LoggerFactory.getLogger(getClass());
    
    String dataset;
	String ackset;
	String dispatchlist;
	String datahash;
	
	private long counter;
	private long ackWaitSec;
	private int pageSize = 10;
	private String topic;
	private Listener listener;
	private DefaultRedisClient defaultClient;
	private Thread loop;
	JsonMapper mapper = new JsonMapper();

	public DMQ(String host,int port,String topic) {
		this(new DefaultRedisClient(host,port),topic,30,10);
	}
	public DMQ(DefaultRedisClient redisClient,String topic,int ackWaitSec,int pageSize) {
		Objects.requireNonNull(topic);
		this.topic = topic;
		this.defaultClient = redisClient;
		this.dataset  =  topic+"-dataset";
		this.dispatchlist =  topic+"-dislist";
		this.datahash = topic+"-datahash";
		this.pageSize = pageSize;
		this.ackWaitSec = ackWaitSec;
	}
	
	public DMQ setLisenter(Listener listener) {
		this.listener = listener;
		return this;
	}
	public DMQ start() {
		this.loop = new Thread(new Runnable() {
			@Override
			public void run() {
				pull();
			}
		});
		this.loop.start();
		return this;
	}
	
	public void pull() {
		int timeout = 1;
		while(true) {
			long ll = System.currentTimeMillis();
			//每隔timeout就检查一下是否存在待处理数据
			KeyValue<String,String> kv = defaultClient.getSyncCmd().brpop(timeout,this.dispatchlist);
			//System.err.println("获取数据:"+counter++);
			long curr = System.currentTimeMillis();
			//如果获取数据的时间小于超时时间,表明是队里中存在未处理完的数据
			if(curr - ll < timeout * 1000) {
				//处理延时队列中的数据
				exe(kv);
				//每隔64个检查一次待处理数据
				if(counter % 64 == 0) {
					chekData();
				}
			}else {
				//获取完数据后,立即检查是否存在待处理数据
				chekData();
			}
		}
	}
	
	 /**
	  选取到期需要处理的数据添加到dispatchlist
	  */
	public void chekData() {
		if(this.listener == null) {
			return;
		}
		String script =  
					 "local expiredValues = redis.call('zrangebyscore', KEYS[1], 0, ARGV[1], 'limit', 0,ARGV[3]); " 
				 	+"if #expiredValues > 0 then "
				 	  +"for i, v in ipairs(expiredValues) do "
				 	  	+"redis.call('zrem', KEYS[1], v); "	//从数据延时队里中移除
				 	  	+"redis.call('zadd', KEYS[2], ARGV[2], v); "//将待处理的数据添加到ack延时队列
				 		+"redis.call('lpush', KEYS[3], v); "		//将待处理数据添加到事件触发队列
				 	  +"end; "
				 	  +"return 1;"
				 	+"end; "
				 	+"return 0;";
		long current = System.currentTimeMillis();
		long expireTime = current + this.ackWaitSec * 1000;
		String[] keys = new String[] {dataset,dataset,dispatchlist};
		String[] values = new String[] {String.valueOf(current),String.valueOf(expireTime),String.valueOf(this.pageSize)};
		defaultClient.getSyncCmd().eval(script,ScriptOutputType.INTEGER,keys,values);
	}
	public void exe(KeyValue<String,String> kv) {
		String id = kv.getValue();
		try {
			String data = defaultClient.getSyncCmd().hget(this.datahash,id);
			Entity ent = mapper.readValue(data,Entity.class);
			ConsumeStatus status = this.listener.onMessage(ent);
			if(ConsumeStatus.CONSUMED.equals(status)) {
				removeConsumed(ent.getId());
			}
		} catch (JsonProcessingException e) {
			log.error("json格式化错误id:"+id);
			throw new RuntimeException(e);
		}
	}
	
	public Ret removeConsumed(String id) {
		String script =  
				  "redis.call('zrem', KEYS[1], v[1]);"
				+ "redis.call('hdel', KEYS[2], v[1]);"
	            + "return 1; ";
		RedisCommands<String, String> cmd = defaultClient.getSyncCmd();
		String[] keys = new String[] {this.dataset,this.datahash};
		String[] values = new String[] {String.valueOf(id)};
		Long ret = cmd.eval(script,ScriptOutputType.INTEGER,keys,values);
		if(ret != null && ret == 1) {
			return Ret.succ();
		}
		return Ret.err("数据删除失败,id:"+id);
	}
	
	public Ret pushData(Entity entity) {
		String script =  
				  "redis.call('hset', KEYS[1], ARGV[2],ARGV[3]);"
				+ "redis.call('zadd', KEYS[2], ARGV[1],ARGV[2]);"
		  		+ "return 1; ";
		String[] keys = new String[] {this.datahash,this.dataset};
		String deathline = String.valueOf(entity.getDeathline());
		String id = entity.getId();
		String data;
		try {
			data = new JsonMapper().writeValueAsString(entity);
		} catch (JsonProcessingException e) {
			log.error("json格式化错误",e);
			return Ret.err(e.getMessage());
		}
		RedisCommands<String, String> cmd = defaultClient.getSyncCmd();
		Long ret = cmd.eval(script,ScriptOutputType.INTEGER,keys,deathline,id,data);
		if(ret != null && ret == 1) {
			return Ret.succ();
		}
		return Ret.err("数据添加失败,id:"+id);
	}
	public String getId() {
		return UUID.randomUUID().toString().replace("-","");
	}
}
