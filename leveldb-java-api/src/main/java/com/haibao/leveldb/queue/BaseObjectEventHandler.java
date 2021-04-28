package com.haibao.leveldb.queue;

import com.lmax.disruptor.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 基础事件实现类
 *
 * @author ml.c
 * @date 3:56 PM 3/18/21
 **/
public class BaseObjectEventHandler implements EventHandler<ObjectEvent> {
    static final Logger LOG = LoggerFactory
            .getLogger(BaseObjectEventHandler.class);

    @Override
    public void onEvent(ObjectEvent messageEvent, long sequence, boolean endOfBatch) {

        System.out.println("EventValue:"+messageEvent.getValue());
        LOG.info("EventValue: {}",messageEvent.getValue());
    }
}
