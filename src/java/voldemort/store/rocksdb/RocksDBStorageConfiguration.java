package voldemort.store.rocksdb;

import voldemort.VoldemortException;
import voldemort.routing.RoutingStrategy;
import voldemort.server.VoldemortConfig;
import voldemort.store.StorageConfiguration;
import voldemort.store.StorageEngine;
import voldemort.store.StoreDefinition;
import voldemort.utils.ByteArray;

import org.rocksdb.*;
import org.rocksdb.util.SizeUnit;

public class RocksDBStorageConfiguration implements StorageConfiguration {
    public static final String TYPE_NAME = "rocksdb";
    private final Options options;
    private final String  dataDirectory;

    static {
        RocksDB.loadLibrary();
    }

    public RocksDBStorageConfiguration(VoldemortConfig config) {
        // todo: parse options from config like bdb

        Options options = new Options();
        Filter filter   = new BloomFilter(10);

        options.setCreateIfMissing(true)
               .setBlockSize(524288)
               .setMaxOpenFiles(10000)
               .setUseFsync(false)
               .setBytesPerSync(8388608)
               .setDisableDataSync(false)
               .setCacheSize(8589934592)
               .setTableCacheNumshardbits(6)
               .setMaxWriteBufferNumber(32)
               .setWriteBufferSize(536870912)
               .setTargetFileSizeBase(1073741824)
               .setMinWriteBufferNumberToMerge(4)
               .setLevelZeroStopWritesTrigger(2000)
               .setLevelZeroSlowdownWritesTrigger(0)
               .setMemTableConfig(new SkipListMemTableConfig())
               .setCompactionStyle((byte)0x1) // this may enable universal rather than level
               .setMaxBackgroundCompactions(4)
               .setMaxBackgroundFlushes(4)
               .setFilterDeletes(false)
               .setDisableSeekCompaction(true)
               .setFilter(filter);

        this.options = options;
        this.dataDirectory = "/data/1";
    }

    public StorageEngine<ByteArray, byte[], byte[]> getStore(StoreDefinition storeDef, RoutingStrategy strategy) {
        return new RocksDBStorageEngine(storeDef.getName(), this.dataDirectory, this.options);
    }

    public String getType() {
        return TYPE_NAME;
    }

    public void update(StoreDefinition storeDef) {
        throw new VoldemortException("Storage config updates not permitted for " + this.getClass().getCanonicalName());
    }

    public void close() {}

    // Nothing to do here: we're not tracking the created storage engine.
    @Override
    public void removeStorageEngine(StorageEngine<ByteArray, byte[], byte[]> engine) {}
}
