package com.haibao.leveldb.queue;

import com.google.common.collect.Maps;
import com.haibao.leveldb.api.builder.LeveldbEnums;
import com.haibao.leveldb.api.builder.LocalLeveldbClient;
import com.haibao.leveldb.utils.KVStoreSerializer;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;

/**
 * Leveldb 自定义ringbuffer事件处理器
 *
 * @author ml.c
 * @date 2:06 PM 4/28/21
 **/
public class LeveldbEventHandler extends BaseObjectEventHandler {

    //init KVStoreSerializer
    KVStoreSerializer kvStoreSerializer = new KVStoreSerializer();

    @Override
    public void onEvent(ObjectEvent messageEvent, long sequence, boolean endOfBatch) {

        DB db = LocalLeveldbClient.getInstance();

//        System.out.println("EventValue:" + messageEvent.getValue());
//        LOG.info("EventValue: {}", messageEvent.getValue());

        Map<String,Object> eventMap = null;
        try {
            eventMap = (Map<String,Object>)messageEvent.getValue();
        } catch (Exception e) {
            e.printStackTrace();
        }
        if(null == eventMap){
          return;
        }
        String configKey = eventMap.keySet().stream().findFirst().get();
        Object configVal = eventMap.get(configKey);
        int newVal = (Integer) configVal;
        newVal = newVal > 0 ? newVal : 0;

        Map<String, Object> configMap;

        byte[] data = db.get(kvStoreSerializer.serialize(LocalLeveldbClient.EMDIS_CONFIG_KEY));
        if (null == data || data.length < 1) {
            configMap = Maps.newHashMap();
            configMap.put(configKey, configVal);
        } else {
            configMap = kvStoreSerializer.deserialize(data, Map.class);
            if (configMap.containsKey(configKey)) {
                Object objVal = configMap.get(configKey);
                if (LeveldbEnums.SIZE.getProperty().equals(configKey)) {
                    int oldVal = (Integer) objVal;

                    if(oldVal != newVal){
                        configMap.put(configKey,newVal);
                    }
                } else {
                    //扩展 todo
                }
            } else {
                configMap.put(configKey, configVal);
            }
        }

        db.put(kvStoreSerializer.serialize(LocalLeveldbClient.EMDIS_CONFIG_KEY), kvStoreSerializer.serialize(configMap));
    }


    /**
     * 调度 更新 size
     */
    static {

        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        service.scheduleAtFixedRate(()->{

            DB db = LocalLeveldbClient.getInstance();

            DBIterator iterator = db.iterator();
            try {
                int size = 0;
                for(iterator.seekToFirst(); iterator.hasNext(); iterator.next()) {
                    size++;
                }
                updateEmdisConfiguration(LeveldbEnums.SIZE,size);
            } finally {
                try {
                    iterator.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }, 0, 1, TimeUnit.MINUTES);
    }

    /**
     * 更新emdis自定义配置项
     *
     * @param leveldbEnums
     * @param v
     */
    private static void updateEmdisConfiguration(LeveldbEnums leveldbEnums, Object v) {
        Map<String, Object> map = Maps.newHashMap();
        map.put(leveldbEnums.getProperty(), v);
        DisruptorClient.producer.publish(map);
    }
}
