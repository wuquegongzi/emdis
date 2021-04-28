package com.haibao.leveldb.queue;

import com.lmax.disruptor.RingBuffer;

/**
 *
 * 基础 事件生产者
 * @author ml.c
 * @date 1:58 PM 3/16/21
 **/
public class BaseObjectEventProducer{

    public  final RingBuffer<ObjectEvent> ringBuffer;

    public BaseObjectEventProducer(RingBuffer<ObjectEvent> ringBuffer){
        this.ringBuffer = ringBuffer;
    }

    /**
     * ringbuffer 生产
     * @param bb
     */
    public void publish(Object bb) {
        // Grab the next sequence
        long sequence = ringBuffer.next();
        try {
            // Get the entry in the Disruptor
            // for the sequence
            ObjectEvent event = ringBuffer.get(sequence);
            // Fill with data
            event.setValue(bb);
        } finally {
            ringBuffer.publish(sequence);
        }
    }

}
