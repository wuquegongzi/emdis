package com.haibao.leveldb.cache;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.collect.ImmutableMap;
import com.haibao.leveldb.api.LeveldbService;
import com.haibao.leveldb.api.LeveldbServiceImpl;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

/**
 * 扩展 LoadingCache  Leveldb
 *
 * @author ml.c
 * @date 3:18 PM 4/25/21
 **/
public class LeveldbLoadingPersistingCache<K, V> extends LeveldbPersistingCache<K, V> implements LoadingCache<K, V> {

    private final CacheLoader<? super K, V> cacheLoader;

    protected LeveldbLoadingPersistingCache(CacheBuilder<Object, Object> cacheBuilder, CacheLoader<? super K, V> cacheLoader) {
        this(cacheBuilder, cacheLoader, new LeveldbServiceImpl<K,V>());
    }

    protected LeveldbLoadingPersistingCache(CacheBuilder<Object, Object> cacheBuilder, CacheLoader<? super K, V> cacheLoader, LeveldbService<K,V> leveldbService) {
        this(cacheBuilder, cacheLoader, leveldbService, null);
    }

    protected LeveldbLoadingPersistingCache(CacheBuilder<Object, Object> cacheBuilder, CacheLoader<? super K, V> cacheLoader, RemovalListener<K, V> removalListener) {
        this(cacheBuilder, cacheLoader,  new LeveldbServiceImpl<K,V>(), removalListener);
    }

    protected LeveldbLoadingPersistingCache(CacheBuilder<Object, Object> cacheBuilder, CacheLoader<? super K, V> cacheLoader,
                                            LeveldbService<K,V> leveldbService, RemovalListener<K, V> removalListener) {
        super(cacheBuilder, leveldbService, removalListener);
        this.cacheLoader = cacheLoader;
    }

    private class ValueLoaderFromCacheLoader implements Callable<V> {

        private final K key;
        private final CacheLoader<? super K, V> cacheLoader;

        private ValueLoaderFromCacheLoader(CacheLoader<? super K, V> cacheLoader, K key) {
            this.key = key;
            this.cacheLoader = cacheLoader;
        }

        @Override
        public V call() throws Exception {
            return cacheLoader.load(key);
        }
    }

    @Override
    public V get(K key) throws ExecutionException {
        return get(key, new ValueLoaderFromCacheLoader(cacheLoader, key));
    }

    @Override
    public V getUnchecked(K key) {
        try {
            return get(key, new ValueLoaderFromCacheLoader(cacheLoader, key));
        } catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        }
    }

    @Override
    public ImmutableMap<K, V> getAll(Iterable<? extends K> keys) throws ExecutionException {
        ImmutableMap.Builder<K, V> all = ImmutableMap.builder();
        for (K key : keys) {
            all.put(key, get(key));
        }
        return all.build();
    }

    @Override
    public V apply(K key) {
        try {
            return cacheLoader.load(key);
        } catch (Exception e) {
            throw new RuntimeException(String.format("Could not apply cache on key %s", key), e);
        }
    }

    @Override
    public void refresh(K key) {
        try {
            getUnderlyingCache().put(key, cacheLoader.load(key));
        } catch (Exception e) {
            throw new RuntimeException(String.format("Could not refresh value for key %s", key), e);
        }
    }
}
