package com.haibao.emdis.common.enums;


/**
 * 对象编码
 *
 * @author ml.c
 * @date 11:29 PM 5/4/21
 **/
public enum  EmidsEncodingEnums {

    EMDIS_ENCODING_RAW(0, "EMDIS_ENCODING_RAW", "编码为字符串"),
    EMDIS_ENCODING_INT(1, "EMDIS_ENCODING_INT", "编码为整数"),
    EMDIS_ENCODING_HT(2, "EMDIS_ENCODING_HT", "编码为哈希表"),
    EMDIS_ENCODING_ZIPMAP(3, "EMDIS_ENCODING_ZIPMAP", "编码为zipmap"),
    EMDIS_ENCODING_LINKEDLIST(4, "EMDIS_ENCODING_LINKEDLIST", "编码为双端链表"),
    EMDIS_ENCODING_ZIPLIST(5, "EMDIS_ENCODING_ZIPLIST", "编码为压缩列表"),
    EMDIS_ENCODING_INTSET(6, "EMDIS_ENCODING_INTSET", "编码为整数集合"),
    EMDIS_ENCODING_SKIPLIST(7, "EMDIS_ENCODING_SKIPLIST", "编码为跳跃表");


    private int encodingVal;
    private String encodingName;
    private String encodingDesc;

    EmidsEncodingEnums(int encodingVal, String encodingName, String encodingDesc) {
        this.encodingVal = encodingVal;
        this.encodingName = encodingName;
        this.encodingDesc = encodingDesc;
    }

    public int getEncodingVal() {
        return encodingVal;
    }

    public void setEncodingVal(int encodingVal) {
        this.encodingVal = encodingVal;
    }

    public String getEncodingName() {
        return encodingName;
    }

    public void setEncodingName(String encodingName) {
        this.encodingName = encodingName;
    }

    public String getEncodingDesc() {
        return encodingDesc;
    }

    public void setEncodingDesc(String encodingDesc) {
        this.encodingDesc = encodingDesc;
    }}

