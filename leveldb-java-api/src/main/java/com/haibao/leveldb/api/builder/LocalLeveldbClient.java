package com.haibao.leveldb.api.builder;

import cn.hutool.core.lang.Console;
import java.io.File;
import java.io.IOException;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBFactory;
import org.iq80.leveldb.Logger;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.impl.Iq80DBFactory;

/**
 * Leveldb 统一客户端
 *
 * @author ml.c
 * @date 7:20 PM 4/25/21
 **/
public class LocalLeveldbClient {

    private static final String PATH = "cache/data/emdisdb";
    private static final File FILE = new File(PATH);

    private static DBFactory factory;
    private static Options options;
    private static DB instance = null;


    /**
     * 获取DB
     *
     * @return
     * @throws IOException
     */
    public static DB getInstance() {

        if(null == instance){
            synchronized(LocalLeveldbClient.class) {
                if ( null == instance) {
                     if(null == factory){
                         factory = new Iq80DBFactory();
                     }
                     if(null == options){
                         Logger logger = new Logger() {
                             @Override
                             public void log(String message) {
                                 Console.log(message);
                             }
                         };

                         options = new LocalOptions.Builder()
                                 .createIfMissing(true)
                                 .blockSize(8092)
                                 .logger(logger)
                                 // 100MB cache
                                 .cacheSize(100 * 1048576).build();

                     }
                    try {
                        instance = factory.open(FILE, options);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        return instance;
    }

    /**
     * 关闭 DB
     */
    public static void close() {
        if (null != instance) {
            try {
                instance.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
