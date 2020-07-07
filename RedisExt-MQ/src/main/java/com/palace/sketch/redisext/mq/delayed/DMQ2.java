package com.palace.sketch.redisext.mq.delayed;

import java.util.List;
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

import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.sync.RedisCommands;

public class DMQ2 implements IConsumer{
    final Logger log = LoggerFactory.getLogger(getClass());
    
    String workmq;
	String ackmq;
	String msghash;
	
	private long ackWaitSec;
	private int pageSize = 10;
	private String topic;
	private Listener msgListener;
	private DefaultRedisClient redisClient;
	private ScheduledExecutorService schedulerService;
	
	public DMQ2(String host,int port,String topic) {
		this(new DefaultRedisClient(host,port),topic,30,10);
	}
	public DMQ2(DefaultRedisClient redisClient,String topic,int ackWaitSec,int pageSize) {
		Objects.requireNonNull(topic);
		this.topic = topic;
		this.redisClient = redisClient;
		this.workmq  =  topic+"-workmq";
		this.ackmq = topic+"-ackmq";
		this.msghash =  topic+"-msghash";
		this.pageSize = pageSize;
		this.ackWaitSec = ackWaitSec;
		initScheduler();
	}
	
	public void registerListener(Listener listener) {
		this.msgListener = listener;
	}
	
	public void initScheduler() {
		this.schedulerService = Executors.newScheduledThreadPool(1);
		schedulerService.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				 
			}
		},1,1,TimeUnit.SECONDS);
	}
	/**
	 * 等待一定时间后数据没有被确认,则将数据重新添加到延时队列里重试
	 */
	public void reDo() {
		String script =  
				 "local expiredValues = redis.call('zrangebyscore', KEYS[1], 0, ARGV[1], 'limit', 0,ARGV[3]); " 
			 	+"if #expiredValues > 0 then "
			 	  +"for i, v in ipairs(expiredValues) do "
			 	  	+"redis.call('zrem', KEYS[1], v); "
			 	  	+"redis.call('zadd', KEYS[2], ARGV[2], v); "
			 	  +"end; "
			 	  +"return expiredValues "
			 	+"end; "
	            +"return nil;";
		RedisCommands<String, String> cmd = redisClient.getSyncCmd();
		long current = System.currentTimeMillis();
		long expireTime = current + this.ackWaitSec * 1000;
		String[] keys = new String[] {workmq,ackmq,msghash};
		String[] values = new String[] {String.valueOf(current),String.valueOf(expireTime),String.valueOf(this.pageSize)};
		List<String> list = cmd.eval(script,ScriptOutputType.MULTI,keys,values);
	}
	/**
	 * 获取所有符合条件的待处理数据
	 * 从延时队列里移除,
	 * 将数据添加到ack队列里
	 * 从hash中获取消息体
	 * @param func
	 */
	public void pull(Function<String,ConsumeStatus> func) {
		String script =  
					 "local expiredValues = redis.call('zrangebyscore', KEYS[1], 0, ARGV[1], 'limit', 0,ARGV[3]); " 
				 	+"if #expiredValues > 0 then "
				 	  +"local result = {};"
				 	  +"for i, v in ipairs(expiredValues) do "
				 	  	+"redis.call('zrem', KEYS[1], v); "
				 	  	+"redis.call('zadd', KEYS[2], ARGV[2], v); "
				 	  	+"local data = redis.call('hget', KEYS[3], v); "
                        +"local value = struct.pack('ss0', v, data);" 
                        +"table.insert(result, value);"
				 	  +"end; "
				 	  +"return result "
				 	+"end; "
		            +"return nil;";
		RedisCommands<String, String> cmd = redisClient.getSyncCmd();
		long current = System.currentTimeMillis();
		long expireTime = current + this.ackWaitSec * 1000;
		String[] keys = new String[] {workmq,ackmq,msghash};
		String[] values = new String[] {String.valueOf(current),String.valueOf(expireTime),String.valueOf(this.pageSize)};
		List<String> list = cmd.eval(script,ScriptOutputType.MULTI,keys,values);
		if(list != null ) {
			JsonMapper jsonMapper = new JsonMapper();
			for(String data : list) {
				if(data == null) {
					continue;
				}
			    try {
					Entity ent = jsonMapper.readValue(data,Entity.class);
					ConsumeStatus status = func.apply(ent.getData());
					if(ConsumeStatus.CONSUMED.equals(status)) {
						removeConsumed(ent.getId());
					}
				} catch (JsonProcessingException e) {
					throw new RuntimeException(e);
				}
			}
		}
	}
	
	public Ret removeConsumed(String id) {
		String script =  
				  "redis.call('zrem', KEYS[1], v[1]);"
				+ "redis.call('hdel', KEYS[2], v[1]);"
	            + "return 1; ";
		RedisCommands<String, String> cmd = redisClient.getSyncCmd();
		String[] keys = new String[] {workmq,ackmq};
		String[] values = new String[] {String.valueOf(id)};
		Long ret = cmd.eval(script,ScriptOutputType.INTEGER,keys,values);
		if(ret != null && ret == 1) {
			return Ret.succ();
		}
		return Ret.err("数据删除失败,id:"+id);
	}
	
	public Ret pushData(Entity entity) {
		return pushData(entity.getId(),entity.getDeathline(),entity.getData());
	}
	public Ret pushData(String id,long deathline,String data) {
		String script =  
				  "redis.call('hset', KEYS[1], ARGV[1],ARGV[2]);"
				+ "redis.call('zadd', KEYS[2], ARGV[3],ARGV[1]);"
		  		+ "return 1; ";
		RedisCommands<String, String> cmd = redisClient.getSyncCmd();
		String[] keys = new String[] {msghash,workmq};
		String[] values = new String[] {String.valueOf(id),data,String.valueOf(deathline)};
		Long ret = cmd.eval(script,ScriptOutputType.INTEGER,keys,values);
		if(ret != null && ret == 1) {
			return Ret.succ();
		}
		return Ret.err("数据添加失败,id:"+id);
	}
	public String getId() {
		return UUID.randomUUID().toString().replace("-","");
	}
}
