package com.haibao.emdis.common.model;

/**
 * 底层的数据支持
 *
 * @author ml.c
 * @date 12:06 AM 5/5/21
 **/
public class EmdisObject {
    /**
     * 对象类型 EmdisTypeEnums ->typeVal
     */
    int type;
    /**
     * 对象编码 EmidsEncodingEnums ->encodingVal
     */
    int encoding;
    /**
     * 引用计数
     */
    int refcount;

    /**
     * 对象的值
     */
    Object ptr;

    /**
     * 到期时间的时间戳
     */
    Long expires;
}
