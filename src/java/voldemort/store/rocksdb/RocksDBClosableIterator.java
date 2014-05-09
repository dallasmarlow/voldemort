package voldemort.store.rocksdb;

import org.rocksdb.*;

import voldemort.utils.ClosableIterator;
import voldemort.store.PersistenceFailureException;

abstract class RocksDBClosableIterator<T> implements ClosableIterator<T> {
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