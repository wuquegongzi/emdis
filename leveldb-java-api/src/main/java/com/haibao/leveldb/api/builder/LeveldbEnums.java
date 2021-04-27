package com.haibao.leveldb.api.builder;

import java.util.Map;

/**
 *
 *
 * @author ml.c
 * @date 6:50 PM 4/27/21
 **/
public enum LeveldbEnums {

    SIZE("SIZE","数据量","0",0);

    /**
     * 属性名
     */
    private String property;
    /**
     * 属性 中文名
     */
    private String propertyName;
    /**
     * 属性 默认值
     */
    private String defaultValue;
    /**
     * 属性 索引
     */
    private int index;

    // 构造方法
    private LeveldbEnums(String property, String propertyName, String defaultValue, int index) {
        this.property = property;
        this.propertyName = propertyName;
        this.defaultValue = defaultValue;
        this.index = index;
    }

    public static String getProperty(int index) {
        for (LeveldbEnums c : LeveldbEnums.values()) {
            if (c.getIndex() == index) {
                return c.property;
            }
        }
        return null;
    }

    public String getProperty() {
        return property;
    }

    public void setProperty(String property) {
        this.property = property;
    }

    public String getPropertyName() {
        return propertyName;
    }

    public void setPropertyName(String propertyName) {
        this.propertyName = propertyName;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }}