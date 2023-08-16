package com.baidu.hugegraph.util.collection;

public class JniBytesSet extends NativeReference implements Iterable<byte[]> {
    long handle;

    public JniBytesSet(int partitionBits, int capacityBits) {
        handle = init(true, partitionBits, capacityBits);
    }

    private native boolean add(long handle, byte[] value);

    public boolean add(byte[] value) {
        return add(handle, value);
    }

    private native long addAll(long handle, long other);

    public long addAll(JniBytesSet other) {
        return addAll(handle, other.handle);
    }

    private native boolean addExclusive(long handle, byte[] value, long other);

    public boolean addExclusive(byte[] value, JniBytesSet other) {
        return addExclusive(handle, value, other.handle);
    }

    private native boolean remove(long handle, byte[] value);

    public boolean remove(byte[] value) {
        return remove(handle, value);
    }

    private native int erase(long handle, long it, int count);

    public int erase(JniBytesSetIterator it, int count) {
        return erase(handle, it.handle, count);
    }

    private native boolean contains(long handle, byte[] value);

    public boolean contains(byte[] value) {
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

    public JniBytesSetIterator iterator() {
        return new JniBytesSetIterator(iterator(handle));
    }

    private native long init(boolean coCurrent, int partitionBits, int capacityBits);

    private native void deleteNative(long handle);

}
