package voldemort.store.rocksdb;

public class RocksDBStorageUtil {
    public static byte[] serializeVersionedKey(byte[] key, long version) {
        int bLen = key.length;
        byte[] result = new byte[bLen + 8];

        result[bLen - 8] = (byte) (0xff & (version >> 56));
        result[bLen - 7] = (byte) (0xff & (version >> 48));
        result[bLen - 6] = (byte) (0xff & (version >> 40));
        result[bLen - 5] = (byte) (0xff & (version >> 32));
        result[bLen - 4] = (byte) (0xff & (version >> 24));
        result[bLen - 3] = (byte) (0xff & (version >> 16));
        result[bLen - 2] = (byte) (0xff & (version >> 8));
        result[bLen - 1] = (byte) (0xff & version);

        System.arraycopy(key, 0, result, 0, bLen);
        return result;
    }
}