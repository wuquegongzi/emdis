package com.haibao.leveldb.cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheStats;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.haibao.leveldb.api.builder.LeveldbEnums;
import com.haibao.leveldb.queue.DisruptorClient;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 自定义LeveldbCache，实现guava接口
 * 参考 Extending Guava caches to overflow to disk ：
 * https://www.javacodegeeks.com/2013/12/extending-guava-caches-to-overflow-to-disk.html
 *
 * @author ml.c
 * @date 8:45 PM 4/24/21
 **/
public abstract class AbstractPersistingCache<K, V> implements Cache<K, V> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractPersistingCache.class);

    private final LoadingCache<K, V> underlyingCache;
    private final RemovalListener<K, V> removalListener;

    protected AbstractPersistingCache(CacheBuilder<Object, Object> cacheBuilder) {
        this(cacheBuilder, null);
    }

    protected AbstractPersistingCache(CacheBuilder<Object, Object> cacheBuilder, RemovalListener<K, V> removalListener) {
        this.underlyingCache = makeCache(cacheBuilder);
        this.removalListener = removalListener;
    }

    protected LoadingCache<K, V> getUnderlyingCache() {
        return underlyingCache;
    }

    /**
     * 构建 leveldbCache
     *
     * @param cacheBuilder
     * @return
     */
    protected LoadingCache<K, V> makeCache(CacheBuilder<Object, Object> cacheBuilder) {

        return cacheBuilder
                .removalListener(new LeveldbPersistingRemovalListener())
                .build(new LeveldbPersistedStateCacheLoader());
    }

    /**
     * 自定义 移除后监听器，Leveldb做保留
     */
    private class LeveldbPersistingRemovalListener implements RemovalListener<K, V> {
        @Override
        public void onRemoval(RemovalNotification<K, V> removalNotification) {
            if (isLeveldbRelevant(removalNotification.getCause())) {
                try {
                    //保留数据
                    persistValue(removalNotification.getKey(), removalNotification.getValue());

                } catch (IOException e) {
                    LOGGER.warn(String.format("Could not persist value %s to key %s",
                            removalNotification.getKey(), removalNotification.getValue()), e);
                }
            } else if (removalListener != null) {
                removalListener.onRemoval(removalNotification);
            }
        }
    }

    /**
     * 加载persisted value
     */
    private class LeveldbPersistedStateCacheLoader extends CacheLoader<K, V> {
        @Override
        public V load(K k){
            V value = null;
            try {
                value = findPersisted(k);
                if (value != null) {
                    //deletePersistedIfExistent(k);
                    underlyingCache.put(k, value);
                }
            } catch (Exception e) {
                LOGGER.warn(String.format("Could not load persisted value to key %s", k), e);
            }
            if (value != null) {
                return value;
            } else {
                throw new NotPersistedException();
            }
        }
    }

    /**
     *
     */
    private class LeveldbPersistedStateValueLoader extends LeveldbPersistedStateCacheLoader implements Callable<V> {

        private final K key;
        private final Callable<? extends V> valueLoader;

        public LeveldbPersistedStateValueLoader(K k, Callable<? extends V> valueLoader) {
            this.key = k;
            this.valueLoader = valueLoader;
        }

        @Override
        public V call() throws Exception {
            V value;
            try {
                value = load(key);
            } catch (NotPersistedException e) {
                value = null;
            }
            if (value != null) {
                return value;
            }
            return valueLoader.call();
        }
    }

    /**
     * 失效分析
     *
     * @param removalCause
     * @return
     */
    protected boolean isLeveldbRelevant(RemovalCause removalCause) {
        // Note: RemovalCause#wasEvicted is package private
        //如果由于换出而自动删除，则返回true（不是由于 RemovalCause.EXPLICIT 和 RemovalCause.REPLACED）
        return removalCause != RemovalCause.EXPLICIT
                && removalCause != RemovalCause.REPLACED;
    }

    protected abstract boolean isPersist(K key);

    protected abstract void persistValue(K key, V value) throws IOException;

    protected abstract V findPersisted(K k);

    protected abstract void deletePersistedIfExistent(K key);

    protected abstract void deleteAllPersisted();

    protected abstract long sizeOfPersisted();

    @Override
    public V getIfPresent(Object key) {

        try {
            K castKey = (K) key;
            return underlyingCache.get(castKey);
        } catch (ClassCastException e) {
            LOGGER.info(String.format("Could not cast key %s to desired type", key), e);
        } catch (ExecutionException e) {
            LOGGER.warn(String.format("Persisted value to key %s could not be retrieved", key), e);
            throw new RuntimeException("Error while loading persisted value", e);
        } catch (RuntimeException e) {
            if (!(e.getCause() instanceof NotPersistedException)) {
                throw e;
            }
        }
        return null;
    }

    @Override
    public V get(K k, Callable<? extends V> valueLoader) throws ExecutionException {

        return underlyingCache.get(k, new LeveldbPersistedStateValueLoader(k, valueLoader));
    }

    @Override
    public ImmutableMap<K, V> getAllPresent(Iterable<?> keys) {
        ImmutableMap.Builder<K, V> allPresent = ImmutableMap.builder();
        for (Object key : keys) {
            V value = getIfPresent(key);
            if (value != null) {
                allPresent.put((K) key, value);
            }
        }
        return allPresent.build();
    }

    @Override
    public void put(K k, V v) {
        underlyingCache.put(k, v);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        underlyingCache.putAll(map);
    }

    @Override
    public void invalidate(Object key) {
        underlyingCache.invalidate(key);
        invalidatePersisted(key);
    }

    @Override
    public void invalidateAll(Iterable<?> keys) {
        underlyingCache.invalidateAll(keys);
        for (Object key : keys) {
            invalidatePersisted(key);
        }
    }

    private void invalidatePersisted(Object key) {
        try {
            K castKey = (K) key;
            if (removalListener == null) {
                deletePersistedIfExistent(castKey);
            } else {
                V value = findPersisted(castKey);
                if (value != null) {
                    removalListener.onRemoval(RemovalNotifications.make(castKey, value));
                    deletePersistedIfExistent(castKey);
                }
            }
        } catch (ClassCastException e) {
            LOGGER.info(String.format("Could not cast key %s to desired type", key), e);
        }
    }

    @Override
    public void invalidateAll() {
        underlyingCache.invalidateAll();
        deleteAllPersisted();
    }

    @Override
    public long size() {
        return  underlyingCache.size() + sizeOfPersisted();
    }

    @Override
    public CacheStats stats() {
        return underlyingCache.stats();
    }

    @Override
    public ConcurrentMap<K, V> asMap() {
        return underlyingCache.asMap();
    }

    @Override
    public void cleanUp() {
        underlyingCache.cleanUp();
    }

}
