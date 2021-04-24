package com.haibao.leveldb.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * 序列化 反序列化
 *
 * @author ml.c
 * @date 6:45 PM 4/23/21
 **/
public class ObjectAndByte {


    /**
     * 对象转数组
     * @param obj
     * @return
     */
    public static byte[] toByteArray(Object obj) {

        byte[] bytes = null;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = null;
        try {
            oos = new ObjectOutputStream(bos);
            oos.writeObject(obj);

            bytes = bos.toByteArray();

        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            if (oos != null) {
                try {
                    oos.flush();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                try {
                    oos.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            try {
                bos.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return bytes;
    }


    /**
     *  数组转对象
     * @param bytes
     * @return
     */
    public static Object toObject (byte[] bytes) {

        Object obj = null;
        ObjectInputStream ois = null;
        ByteArrayInputStream bis = null;
        try {
            bis = new ByteArrayInputStream(bytes);
            ois = new ObjectInputStream(bis);
            obj = ois.readObject();

        } catch (IOException ex) {
            ex.printStackTrace();
        } catch (ClassNotFoundException ex) {
            ex.printStackTrace();
        } finally {
            if (ois != null) {
                try {
                    ois.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (bis != null) {
                try {
                    bis.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return obj;
    }
}
