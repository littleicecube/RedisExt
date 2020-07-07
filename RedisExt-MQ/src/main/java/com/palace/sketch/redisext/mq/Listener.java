package com.palace.sketch.redisext.mq;

import com.palace.sketch.redisext.beans.Entity;

public interface Listener {

	public ConsumeStatus onMessage(Entity entity);
}
