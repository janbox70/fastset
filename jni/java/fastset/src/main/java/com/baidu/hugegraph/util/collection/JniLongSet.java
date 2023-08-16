package com.baidu.hugegraph.util.collection;

public class JniLongSet extends NativeReference  implements Iterable<Long> {
    long handle;

    public JniLongSet(int partitionBits, int capacityBits) {
        handle = init(true, partitionBits, capacityBits);
    }

    private native boolean add(long handle, long value);

    public boolean add(long value) {
        return add(handle, value);
    }

    private native long addAll(long handle, long other);

    public long addAll(JniLongSet other) {
        return addAll(handle, other.handle);
    }

    private native boolean addExclusive(long handle, long value, long other);

    public boolean addExclusive(long value, JniLongSet other) {
        return addExclusive(handle, value, other.handle);
    }

    private native boolean remove(long handle, long value);

    public boolean remove(long value) {
        return remove(handle, value);
    }

    private native int erase(long handle, long it, int count);

    public int erase(JniLongSetIterator it, int count) {
        return erase(handle, it.handle, count);
    }

    private native boolean contains(long handle, long value);

    public boolean contains(long value) {
        return contains(handle, value);
    }

    private native long size(long handle);

    public long size() {
        return handle != 0 ? size(handle) : 0;
    }

    private native void clear(long handle);

    public void clear() {
        clear(handle);
    }
    @Override
    public void close() {
        if (handle != 0) {
            deleteNative(handle);
            handle = 0;
        }
    }

    private native long iterator(long handle);

    public JniLongSetIterator iterator() {
        return new JniLongSetIterator(iterator(handle));
    }

    private native long init(boolean coCurrent, int partitionBits, int capacityBits);

    private native void deleteNative(long handle);

}
