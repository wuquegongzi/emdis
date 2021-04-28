package com.haibao.leveldb.cache;

import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheBuilderSpec;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.Weigher;
import com.haibao.leveldb.api.LeveldbService;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * EmdisCache 构造者
 * {@link CacheBuilder}
 */
public final class EmdisCacheBuilder<K, V> {

    /**
     * {@link CacheBuilder#from(CacheBuilderSpec)}
     */
    public static EmdisCacheBuilder<Object, Object> from(CacheBuilderSpec spec) {
        return new EmdisCacheBuilder<Object, Object>(CacheBuilder.from(spec));
    }

    /**
     * {@link CacheBuilder#from(String)}
     */
    public static EmdisCacheBuilder<Object, Object> from(String spec) {
        return new EmdisCacheBuilder<Object, Object>(CacheBuilder.from(spec));
    }

    /**
     * {@link CacheBuilder#newBuilder()}
     */
    public static EmdisCacheBuilder<Object, Object> newBuilder() {
        return new EmdisCacheBuilder<Object, Object>();
    }

    private final CacheBuilder<Object, Object> underlyingCacheBuilder;

    private RemovalListener<? super K, ? super V> removalListener;
    private LeveldbService leveldbService;
    private long maximumSize;

    private EmdisCacheBuilder() {
        this.underlyingCacheBuilder = CacheBuilder.newBuilder();
    }

    private EmdisCacheBuilder(CacheBuilder<Object, Object> cacheBuilder) {
        this.underlyingCacheBuilder = cacheBuilder;
    }

    /**
     * {@link CacheBuilder#concurrencyLevel(int)}
     */
    public EmdisCacheBuilder<K, V> concurrencyLevel(int concurrencyLevel) {
        underlyingCacheBuilder.concurrencyLevel(concurrencyLevel);
        return this;
    }

    /**
     * {@link CacheBuilder#expireAfterAccess(long, TimeUnit)}
     */
    public EmdisCacheBuilder<K, V> expireAfterAccess(long duration, TimeUnit unit) {
        underlyingCacheBuilder.expireAfterWrite(duration, unit);
        return this;
    }

    /**
     * {@link CacheBuilder#expireAfterWrite(long, TimeUnit)}
     */
    public EmdisCacheBuilder<K, V> expireAfterWrite(long duration, TimeUnit unit) {
        underlyingCacheBuilder.expireAfterWrite(duration, unit);
        return this;
    }

    /**
     * {@link CacheBuilder#refreshAfterWrite(long, TimeUnit)}
     */
    public EmdisCacheBuilder<K, V> refreshAfterWrite(long duration, TimeUnit unit) {
        underlyingCacheBuilder.refreshAfterWrite(duration, unit);
        return this;
    }

    /**
     * {@link CacheBuilder#initialCapacity(int)}
     */
    public EmdisCacheBuilder<K, V> initialCapacity(int initialCapacity) {
        underlyingCacheBuilder.initialCapacity(initialCapacity);
        return this;
    }

    /**
     * {@link CacheBuilder#maximumSize(long)}
     */
    public EmdisCacheBuilder<K, V> maximumSize(long size) {
        underlyingCacheBuilder.maximumSize(size);
        maximumSize = size;
        return this;
    }

    /**
     * {@link CacheBuilder#maximumWeight(long)}
     */
    public EmdisCacheBuilder<K, V> maximumWeight(long weight) {
        underlyingCacheBuilder.maximumWeight(weight);
        return this;
    }

    /**
     * {@link CacheBuilder#recordStats()}
     */
    public EmdisCacheBuilder<K, V> recordStats() {
        underlyingCacheBuilder.recordStats();
        return this;
    }

    /**
     * {@link CacheBuilder#softValues()}
     */
    public EmdisCacheBuilder<K, V> softValues() {
        underlyingCacheBuilder.softValues();
        return this;
    }

    /**
     * {@link CacheBuilder#weakKeys()}
     */
    public EmdisCacheBuilder<K, V> weakKeys() {
        underlyingCacheBuilder.weakKeys();
        return this;
    }

    /**
     * {@link CacheBuilder#weakValues()}
     */
    public EmdisCacheBuilder<K, V> weakValues() {
        underlyingCacheBuilder.weakValues();
        return this;
    }

    /**
     * {@link CacheBuilder#ticker(Ticker)}
     */
    public EmdisCacheBuilder<K, V> ticker(Ticker ticker) {
        underlyingCacheBuilder.ticker(ticker);
        return this;
    }

    /**
     * {@link CacheBuilder#weigher(Weigher)}
     */
    @SuppressWarnings("unchecked")
    public <K1 extends K, V1 extends V> EmdisCacheBuilder<K1, V1> weigher(Weigher<? super K1, ? super V1> weigher) {
        underlyingCacheBuilder.weigher(weigher);
        return (EmdisCacheBuilder<K1, V1>) this;
    }

    /**
     * {@link CacheBuilder#removalListener(RemovalListener)}
     */
    public <K1 extends K, V1 extends V> EmdisCacheBuilder<K1, V1> removalListener(RemovalListener<? super K1, ? super V1> listener) {
        checkState(this.removalListener == null);
        @SuppressWarnings("unchecked")
        EmdisCacheBuilder<K1, V1> castThis = (EmdisCacheBuilder<K1, V1>) this;
        castThis.removalListener = checkNotNull(listener);
        return castThis;
    }

    /**
     * Sets leveldbService for persisting data. This service <b>must not be used for other purposes</b>.
     *
     * @param leveldbService LeveldbService is used by this file cache.
     * @return This builder.
     */
    public EmdisCacheBuilder<K, V> leveldbService(LeveldbService leveldbService) {
        checkState(this.leveldbService == null);
        this.leveldbService = checkNotNull(leveldbService);
        return this;
    }

    /**
     * {@link CacheBuilder#build()}
     */
    public <K1 extends K, V1 extends V> Cache<K1, V1> build() {
        if(maximumSize < 0){
            maximumSize = 0;
        }
        underlyingCacheBuilder.maximumSize(maximumSize);

        if (leveldbService == null) {
            return new LeveldbPersistingCache<K1, V1>(underlyingCacheBuilder, EmdisCacheBuilder.<K1, V1>castRemovalListener(removalListener));
        } else {
            return new LeveldbPersistingCache<K1, V1>(underlyingCacheBuilder, leveldbService, EmdisCacheBuilder.<K1, V1>castRemovalListener(removalListener));
        }
    }

    /**
     * {@link CacheBuilder#build(CacheLoader)}
     */
    public <K1 extends K, V1 extends V> LoadingCache<K1, V1> build(CacheLoader<? super K1, V1> loader) {
        if (leveldbService == null) {
            return new LeveldbLoadingPersistingCache<K1, V1>(underlyingCacheBuilder, loader, EmdisCacheBuilder.<K1, V1>castRemovalListener(removalListener));
        } else {
            return new LeveldbLoadingPersistingCache<K1, V1>(underlyingCacheBuilder, loader, leveldbService, EmdisCacheBuilder.<K1, V1>castRemovalListener(removalListener));
        }
    }

    @SuppressWarnings("unchecked")
    private static <K, V> RemovalListener<K, V> castRemovalListener(RemovalListener<?, ?> removalListener) {
        if (removalListener == null) {
            return null;
        } else {
            return (RemovalListener<K, V>) removalListener;
        }
    }

    @Override
    public String toString() {
        return "EmdisCacheBuilder{" +
                "underlyingCacheBuilder=" + underlyingCacheBuilder +
                ", leveldbService=" + leveldbService +
                '}';
    }
}
