package com.haibao.emdis.datastructure;

/**
 * 字符串
 *
 * @author ml.c
 * @date 12:26 AM 5/5/21
 **/
public class EmdisSdshdr {

    //  记录已使用长度
    int len;
//    // 记录空闲未使用的长度
//    int free;
    // 字符数组
    char[] buf;

    public int getLen() {
        return len;
    }

    public void setLen(int len) {
        this.len = len;
    }

    public char[] getBuf() {
        return buf;
    }

    public void setBuf(char[] buf) {
        this.buf = buf;
    }
}
