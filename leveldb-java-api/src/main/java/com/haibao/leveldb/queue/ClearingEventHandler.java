package com.haibao.leveldb.queue;

import com.lmax.disruptor.EventHandler;

/**
 * 从RingBuffer中清除对象
 *
 * @author ml.c
 * @date 7:52 PM 3/18/21
 **/
public class ClearingEventHandler <T> implements EventHandler<ObjectEvent<T>>{

    @Override
    public void onEvent(ObjectEvent<T> event, long sequence, boolean endOfBatch) throws Exception {
        event.clear();
    }
}
