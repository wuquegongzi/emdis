package com.haibao.leveldb.cache;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.haibao.leveldb.api.LeveldbService;
import com.haibao.leveldb.api.LeveldbServiceImpl;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 扩展 guava Leveldb做存储
 *
 * @author ml.c
 * @date 2:52 PM 4/25/21
 **/
public class LeveldbPersistingCache<K, V> extends AbstractPersistingCache<K, V> {

    private static final Logger LOGGER = LoggerFactory.getLogger(LeveldbPersistingCache.class);

    LeveldbService<K, V> leveldbService;

    protected LeveldbPersistingCache(CacheBuilder<Object, Object> cacheBuilder) {
        this(cacheBuilder, new LeveldbServiceImpl<K, V>());
    }

    protected LeveldbPersistingCache(CacheBuilder<Object, Object> cacheBuilder, LeveldbService<K, V> leveldbService) {
        this(cacheBuilder, leveldbService,null);
    }

    protected LeveldbPersistingCache(CacheBuilder<Object, Object> cacheBuilder, RemovalListener<K, V> removalListener) {
        this(cacheBuilder, new LeveldbServiceImpl<K, V>(),removalListener);
    }

    @Override
    protected boolean isPersist(K key) {
        return true;
    }

    protected LeveldbPersistingCache(CacheBuilder<Object, Object> cacheBuilder, LeveldbService<K, V> leveldbService, RemovalListener<K, V> removalListener) {
        super(cacheBuilder, removalListener);
        this.leveldbService = leveldbService;
        LOGGER.info("Persisting to {}", leveldbService.getClass());
    }

    @Override
    protected void persistValue(K key, V value) throws IOException {
        if (!isPersist(key)) {
            return;
        }
//        leveldbService.setSync(key,value);
        leveldbService.set(key,value);
    }


    @Override
    protected V findPersisted(K key) {
        if (!isPersist(key)) {
            return null;
        }
        return (V)leveldbService.get(key);
    }

    @Override
    protected void deletePersistedIfExistent(K key) {
        leveldbService.removeSync(key);
    }

    @Override
    protected void deleteAllPersisted() {
        leveldbService.removeAll();
    }

    @Override
    protected long sizeOfPersisted() {
        return leveldbService.size();
    }
}
