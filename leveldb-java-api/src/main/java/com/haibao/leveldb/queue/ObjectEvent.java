package com.haibao.leveldb.queue;

/**
 * 用来传递数据
 *
 * @author ml.c
 * @date 3:53 PM 3/18/21
 **/
public class ObjectEvent<T> {

    private T value;
    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }

    void clear(){
        value = null;
    }
}
