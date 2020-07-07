package com.palace.sketch.redisext.mq;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

public class DefaultRedisClient {
	
	private String uri;
	RedisClient redisClient;
    StatefulRedisConnection<String, String> connection;

	public DefaultRedisClient(String uri,int port) {
		RedisURI redisUri = RedisURI.builder()                   
	            .withHost(uri)
	            .withPort(port)
	            .withTimeout(Duration.of(10, ChronoUnit.SECONDS))
	            .build();
	    this.redisClient = RedisClient.create(redisUri); 
	    this.connection = redisClient.connect();
	}
	public  RedisCommands<String, String> getSyncCmd(){
		return this.connection.sync();
	}
	public RedisClient getRedisClient() {
		return this.redisClient;
	}
}
