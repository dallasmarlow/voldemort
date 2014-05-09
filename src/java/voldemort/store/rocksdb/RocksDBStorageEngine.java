package voldemort.store.rocksdb;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;

import java.io.File;
import java.io.IOException;

import org.rocksdb.*;
import org.rocksdb.util.SizeUnit;

import voldemort.store.rocksdb.RocksDBStorageUtil;

import voldemort.VoldemortException;
import voldemort.store.StoreUtils;
import voldemort.store.StoreBinaryFormat;
import voldemort.store.AbstractStorageEngine;
import voldemort.store.PersistenceFailureException;
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
        return new RocksDBClosableKeysIterator(this.db, "first");
    }

    @Override
    public ClosableIterator<ByteArray> keys(int partition) {
        throw new UnsupportedOperationException("Partition based key scan not supported for this storage type");
    }

    @Override
    public ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> entries() {
        try {
            return new RocksDBClosableEntriesIterator(this.db, "first");
        } catch(Exception e) {
            throw new PersistenceFailureException("Unable to instantiate store iterator!", e);
        }
    }

    @Override
    public ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> entries(int partition) {
        throw new UnsupportedOperationException("Partition based entries scan not supported for this storage type");
    }

    //TODO implement key-specific locking to reduce contention
    @Override
    public synchronized void put(ByteArray key, Versioned<byte[]> value, byte[] transforms) throws PersistenceFailureException {
        StoreUtils.assertValidKey(key);

        boolean succeeded = false;
        List<Versioned<byte[]>> vals = null;

        try {
            // do a get for the existing values
            byte[] result = db.get(key.get());
            if(result != null) {
                // update
                vals = StoreBinaryFormat.fromByteArray(result);
                // compare vector clocks and throw out old ones, for updates

                java.util.Iterator<Versioned<byte[]>> iter = vals.iterator();
                while(iter.hasNext()) {
                    Versioned<byte[]> curr = iter.next();
                    Occurred occurred = value.getVersion().compare(curr.getVersion());
                    if(occurred == Occurred.BEFORE)
                        throw new ObsoleteVersionException("Key "
                                                           + value.getVersion().toString()
                                                           + " is obsolete, it is no greater than the current version of "
                                                           + curr.getVersion().toString() + ".");
                    else if(occurred == Occurred.AFTER)
                        iter.remove();
                }
            } else {
                // insert
                vals = new ArrayList<Versioned<byte[]>>(1);
            }

            // update the new value
            vals.add(value);

            try {
                db.put(key.get(), StoreBinaryFormat.toByteArray(vals));
            } catch(RocksDBException e) {
                logger.error(e);
                throw new PersistenceFailureException("Does it look like your mom stores data here!?");
            }

            succeeded = true;

        } catch(RocksDBException e) {
            logger.error("Error in put for store " + this.getName(), e);
            throw new PersistenceFailureException(e);
        }
    }

    @Override
    public List<Versioned<byte[]>> get(ByteArray key, byte[] transforms) throws PersistenceFailureException {
        // return StoreUtils.get(this, key, transforms);
        StoreUtils.assertValidKey(key);

        try {
            // uncommitted reads are perfectly fine now, since we have no
            // je-delete() in put()
            byte[] realkey = key.get();
            if (realkey == null)
                return java.util.Collections.emptyList();
            byte[] result = db.get(realkey);
            if (result == null)
                return java.util.Collections.emptyList();
            else
                return StoreBinaryFormat.fromByteArray(result);
        } catch(RocksDBException e) {
            logger.error(e);
            throw new PersistenceFailureException("Unable to get store entry!");
        } finally {
            if(logger.isTraceEnabled()) {
                logger.trace("Completed GET (" + getName() + ") from key " + key + " (keyRef: "
                             + System.identityHashCode(key) + ") in "
                             + System.currentTimeMillis());
            }
        }
    }

    @Override
    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys,
                                                          Map<ByteArray, byte[]> transforms) {
        StoreUtils.assertValidKeys(keys);
        return StoreUtils.getAll(this, keys, transforms);
    }


    @Override
    public List<Version> getVersions(ByteArray key) {
        return StoreUtils.getVersions(get(key, null));
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

    private static class RocksDBClosableKeysIterator implements ClosableIterator<ByteArray> {
        private RocksDB db;
        private Boolean isValid;
        private Boolean hasIterated;
        private final Iterator iterator;

        public RocksDBClosableKeysIterator(RocksDB db, byte[] key) {
            this.db = db;
            this.iterator = db.newIterator();
            this.iterator.seek(key);
        }

        public RocksDBClosableKeysIterator(RocksDB db, String absolutePosition) {
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

        @Override
        public ByteArray next() {
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

            return new ByteArray(this.iterator.key());
        }
    }

    private class RocksDBClosableEntriesIterator implements ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> {
        private RocksDB db;
        private Boolean isValid;
        private Boolean hasIterated;
        private final Iterator iterator;


        private List<Pair<ByteArray, Versioned<byte[]>>> iteratorCache;

        public RocksDBClosableEntriesIterator(RocksDB db, byte[] key) {
            this.iteratorCache = new ArrayList<Pair<ByteArray, Versioned<byte[]>>>();

            this.db = db;
            this.iterator = db.newIterator();
            this.iterator.seek(key);
        }

        public RocksDBClosableEntriesIterator(RocksDB db, String absolutePosition) {
            if (! (absolutePosition.equals("first") || absolutePosition.equals("last"))) {
                throw new PersistenceFailureException("Invalid iterator absolute position passed to `RocksDBClosableIterator`");
            }

            this.iteratorCache = new ArrayList<Pair<ByteArray, Versioned<byte[]>>>();

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
            if (iteratorCache.size() > 0) return true;

            if (this.isValid == null && this.iterator.isValid()) this.isValid = true;
            return this.isValid;
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

        @Override
        public Pair<ByteArray, Versioned<byte[]>> next() {
            try {
                if (iteratorCache.size() > 0) {
                    return iteratorCache.remove(iteratorCache.size() - 1);
                } else {
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

                    ByteArray key   = new ByteArray(StoreBinaryFormat.extractKey(this.iterator.key()));
                    ByteArray value = new ByteArray(this.iterator.value());

                    for (Versioned<byte[]> valueVersion: StoreBinaryFormat.fromByteArray(value)) {
                        this.iteratorCache.add(Pair.create(key, valueVersion));
                    }

                    return next();
                }
            } catch(Exception e) {
                throw new PersistenceFailureException(e);
            }
        }
    }
}
