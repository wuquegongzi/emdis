package com.haibao.leveldb.queue;

import com.lmax.disruptor.EventFactory;

/**
 * EventFactory接口的实现
 *
 * @author ml.c
 * @date 3:55 PM 3/18/21
 **/
public class ObjectEventFactory implements EventFactory<ObjectEvent> {

    @Override
    public ObjectEvent newInstance() {
        return new ObjectEvent();
    }


}
