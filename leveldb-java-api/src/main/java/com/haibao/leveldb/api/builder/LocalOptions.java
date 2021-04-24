package com.haibao.leveldb.api.builder;

import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.DBComparator;
import org.iq80.leveldb.Logger;
import org.iq80.leveldb.Options;

/**
 *
 * Options 构造者
 * @author ml.c
 * @date 9:05 PM 4/24/21
 **/
public class LocalOptions extends Options {

    /**
     *
     * @param builder
     */
    public LocalOptions(Builder builder){

        this.createIfMissing(builder.createIfMissing);
        this.errorIfExists(builder.errorIfExists);
        this.writeBufferSize(builder.writeBufferSize);
        this.maxOpenFiles(builder.maxOpenFiles);
        this.blockRestartInterval(builder.blockRestartInterval);
        this.blockSize(builder.blockSize);

        if(null != builder.compressionType){
            this.compressionType(builder.compressionType);
        }

        this.paranoidChecks(builder.paranoidChecks);

        if(null != builder.comparator){
            this.comparator(builder.comparator);
        }
        if(null != builder.logger){
            this.logger(builder.logger);
        }

        if(builder.cacheSize > 0){
            this.cacheSize(builder.cacheSize);
        }
    }

    public static class Builder {
        //默认如果没有则创建
        private boolean createIfMissing = true;
        private boolean errorIfExists;
        private int writeBufferSize = 4194304;
        private int maxOpenFiles = 1000;
        private int blockRestartInterval = 16;
        //LevelDB 的磁盘数据是以数据库块的形式存储的，默认的块大小是 4k。
        // 适当提升块大小将有益于批量大规模遍历操作的效率，如果随机读比较频繁，这时候块小点性能又会稍好，这就要求我们自己去折中选择。
        private int blockSize = 4096;
        private CompressionType compressionType;
        private boolean verifyChecksums;
        private boolean paranoidChecks;
        //Disabling Compression  CompressionType.NONE
        //默认是开启压缩  CompressionType.SNAPPY
        private DBComparator comparator;
        //Getting informational log messages.
        private Logger logger;
        //Configuring the Cache
        private long cacheSize = 104857600;

        public Builder() {
        }

        public Builder createIfMissing(boolean createIfMissing) {
            this.createIfMissing = createIfMissing;
            return this;
        }
        public Builder errorIfExists(boolean errorIfExists) {
            this.errorIfExists = errorIfExists;
            return this;
        }
        public Builder writeBufferSize(boolean createIfMissing) {
            this.createIfMissing = createIfMissing;
            return this;
        }
        public Builder maxOpenFiles(int maxOpenFiles) {
            this.maxOpenFiles = maxOpenFiles;
            return this;
        }
        public Builder blockRestartInterval(int blockRestartInterval) {
            this.blockRestartInterval = blockRestartInterval;
            return this;
        }
        public Builder blockSize(int blockSize) {
            this.blockSize = blockSize;
            return this;
        }
        public Builder compressionType(CompressionType compressionType) {
            this.compressionType = compressionType;
            return this;
        }

        public Builder verifyChecksums(boolean createIfMissing) {
            this.verifyChecksums = verifyChecksums;
            return this;
        }
        public Builder paranoidChecks(boolean paranoidChecks) {
            this.paranoidChecks = paranoidChecks;
            return this;
        }

        public Builder comparator(DBComparator comparator) {
            this.comparator = comparator;
            return this;
        }

        public Builder logger(Logger logger) {
            this.logger = logger;
            return this;
        }
        public Builder cacheSize(long cacheSize) {
            this.cacheSize = cacheSize;
            return this;
        }

        public LocalOptions build() {
            return new LocalOptions(this);
        }
    }

}
