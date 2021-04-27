package com.haibao.leveldb.api.builder;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.lang.Console;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileLock;
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
    public final static  String EMDIS_CONFIG_KEY = "emdis_config";
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
                    createGlobalSingleDBConn();
                }
            }
        }

        return instance;
    }

    /**
     *  创建 全局 单一 DB连接
     */
    private static void createGlobalSingleDBConn() {

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
                    .verifyChecksums(true)
                    // 100MB cache
                    .cacheSize(100 * 1048576).build();

        }
        try {
            instance = factory.open(FILE, options);
        } catch (IOException e) {
            e.printStackTrace();
        }
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

    /**
     * 危险操作，销毁 db
     */
    public static void destroy() {
        try {
            factory.destroy(FILE,options);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 修复db
     * @param dbDir 可为null
     * @param optionsParm 可为null
     * @return
     */
    public static boolean repairDB(String dbDir, Options optionsParm) {

        boolean is = true;

        File file;
        if(StrUtil.isEmpty(dbDir)){
            file = FILE;
        }else{
            file = new File(dbDir);
        }
        if(ObjectUtil.isEmpty(optionsParm)){
            optionsParm = options;
        }
        try {
            factory.repair(file,optionsParm);
        } catch (IOException e) {
            e.printStackTrace();
            is = false;
        }

        return is;
    }
}
