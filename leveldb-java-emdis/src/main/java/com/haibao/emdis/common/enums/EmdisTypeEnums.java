package com.haibao.emdis.common.enums;

/**
 * 对象类型
 *
 * @author ml.c
 * @date 11:24 PM 5/4/21
 **/
public enum  EmdisTypeEnums {

    EMDIS_STRING(0,"EMDIS_STRING","字符串"),
    EMDIS_LIST(1,"EMDIS_LIST","列表"),
    EMDIS_SET(2,"EMDIS_SET","集合"),
    EMDIS_ZSET(3,"EMDIS_ZSET","有序集"),
    EMDIS_HASH(4,"EMDIS_HASH","哈希表");

    private int typeVal;
    private String typeName;
    private String typeDesc;

    EmdisTypeEnums(int typeVal, String typeName, String typeDesc) {
        this.typeVal = typeVal;
        this.typeName = typeName;
        this.typeDesc = typeDesc;
    }

    public int getTypeVal() {
        return typeVal;
    }

    public void setTypeVal(int typeVal) {
        this.typeVal = typeVal;
    }

    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    public String getTypeDesc() {
        return typeDesc;
    }

    public void setTypeDesc(String typeDesc) {
        this.typeDesc = typeDesc;
    }}
