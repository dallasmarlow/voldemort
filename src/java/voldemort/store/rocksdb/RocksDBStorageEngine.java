package voldemort.store.rocksdb;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;

import java.io.File;
import java.io.IOException;

import org.rocksdb.*;
import org.rocksdb.util.SizeUnit;

import voldemort.VoldemortException;
import voldemort.store.StoreUtils;
import voldemort.store.AbstractStorageEngine;
import voldemort.store.PersistenceFailureException;
import voldemort.store.rocksdb.RocksDBStorageUtil;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Occurred;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;

/**
 * A StorageEngine that uses RocksDB for persistence
 */

public class RocksDBStorageEngine extends AbstractStorageEngine<ByteArray, byte[], byte[]> {
    private RocksDB db;
    private final Options options;
    private final String  databasePath;
    private static final Logger logger = Logger.getLogger(RocksDBStorageEngine.class);

    public RocksDBStorageEngine(String storeName, String dataDirectory, Options options) {
        super(storeName);

        this.databasePath = StringUtils.join(new String[] {dataDirectory, storeName}, "/");
        this.options = options;
        try {
            this.db = RocksDB.open(this.options, this.databasePath) ;
        } catch(RocksDBException e) {
            logger.error(e);
        }
    }

    @Override
    public ClosableIterator<ByteArray> keys() {
        return StoreUtils.keys(entries());
    }

    @Override
    public ClosableIterator<ByteArray> keys(int partition) {
        throw new UnsupportedOperationException("Partition based key scan not supported for this storage type");
    }

    @Override
    public ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> entries() {
        try {
            return new RocksDBClosableIterator(this.db, "first");
        } catch(Exception e) {
            throw new PersistenceFailureException("Unable to instantiate store iterator!", e);
        }
    }

    @Override
    public ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> entries(int partition) {
        throw new UnsupportedOperationException("Partition based entries scan not supported for this storage type");
    }

    @Override
    public void put(ByteArray key, Versioned<byte[]> value, byte[] transforms) throws PersistenceFailureException {
        StoreUtils.assertValidKey(key);

        throw new PersistenceFailureException("Does it look like your mom stores data here!?");
    }

    @Override
    public List<Versioned<byte[]>> get(ByteArray key, byte[] transforms) throws PersistenceFailureException {
        StoreUtils.assertValidKey(key);
        // return StoreUtils.get(this, key, transforms);

        throw new PersistenceFailureException("Unable to get store entry!");
    }

    @Override
    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys,
                                                          Map<ByteArray, byte[]> transforms) {

        throw new UnsupportedOperationException("`getAll` is not a supported store operation!");
    }

    @Override
    public List<Version> getVersions(ByteArray key) {
        // return StoreUtils.getVersions(get(key, null));

        throw new UnsupportedOperationException("Do you even know how vector clocks work!?");
    }

    @Override
    public boolean delete(ByteArray key, Version maxVersion) throws PersistenceFailureException {
        try {
            StoreUtils.assertValidKey(key);
            Boolean entriesDeleted = false;
            RocksDBClosableIterator iterator = new RocksDBClosableIterator(this.db, key.get());

            while (iterator.hasNext()) {
                Pair<ByteArray, Versioned<byte[]>> entry = iterator.next();

                key             = entry.getFirst();
                Version version = entry.getSecond().getVersion();

                if ((maxVersion == null) || (version.compare(maxVersion) == Occurred.BEFORE)) {
                    iterator.remove();

                    if (! entriesDeleted) entriesDeleted = true;
                }
            }

            return entriesDeleted;
        } catch(Exception e) {
            throw new PersistenceFailureException("Unable to delete store entry!", e);
        }
    }

    @Override
    public void close() throws PersistenceFailureException {
        try {
            this.options.dispose();
            this.db.close();

        } catch(Exception e) {
            throw new PersistenceFailureException("Unable to cleanly close store!", e);
        }
    }

    @Override
    public void truncate() {
        try {
            // close database and delete directory
            this.db.close();
            FileUtils.deleteDirectory(new File(this.databasePath));
        } catch(Exception e) {
            throw new VoldemortException("Failed to truncate store!", e);
        } finally {
            // reopen database (which will recreate the directory)
            try {
                this.db = RocksDB.open(this.options, this.databasePath) ;
            } catch(RocksDBException e) {
                logger.error(e);
            }
        }
    }

    private class RocksDBClosableIterator implements ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> {
        private RocksDB db;
        private Boolean isValid;
        private Boolean hasIterated;
        private final Iterator iterator;

        public RocksDBClosableIterator(RocksDB db, byte[] key) {
            this.db = db;
            this.iterator = db.newIterator();
            this.iterator.seek(key);
        }

        public RocksDBClosableIterator(RocksDB db, String absolutePosition) {
            if (! (absolutePosition.equals("first") || absolutePosition.equals("last"))) {
                throw new PersistenceFailureException("Invalid iterator absolute position passed to `RocksDBClosableIterator`");
            }

            this.db = db;
            this.iterator = db.newIterator();

            if (absolutePosition.equals("first")) {
                this.iterator.seekToFirst();
            } else {
                this.iterator.seekToLast();
            }
        }

        @Override
        public boolean hasNext() {
            if (this.isValid == null && this.iterator.isValid()) this.isValid = true;

            return this.isValid;
        }

        @Override
        public Pair<ByteArray, Versioned<byte[]>> next() {
            try {
                if (! (hasNext() && this.iterator.isValid())) throw new PersistenceFailureException("Next called on invalid iterator!");

                // calling next on a new iterator will skip a record
                if (this.hasIterated == true) {
                    this.iterator.next();
                    this.iterator.status();

                    // if we've reached the end of the iterator, then prevent future iteration
                    if (! this.iterator.isValid()) this.isValid = false;
                } else {
                    this.hasIterated = true;
                }

                byte[] key   = this.iterator.key();
                byte[] value = this.iterator.value();

                // the last 8 bytes of a raw key are the stored version
                int versionlessKeyLength = key.length - 8;
                byte[] versionlessKey     = new byte[versionlessKeyLength];
                System.arraycopy(key, 0, versionlessKey, 0, versionlessKeyLength);
                VectorClock clock = new VectorClock(key, versionlessKeyLength);

                return Pair.create(new ByteArray(versionlessKey), new Versioned<byte[]>(value, clock));
            } catch(Exception e) {
                throw new PersistenceFailureException(e);
            }
        }

        @Override
        public void remove() {
            try {
                this.db.remove(this.iterator.key());
            } catch(RocksDBException e) {
                logger.error(e);
            }
        }

        @Override
        public void close() {
            this.isValid = false;
            this.iterator.dispose();
        }
    }
}
