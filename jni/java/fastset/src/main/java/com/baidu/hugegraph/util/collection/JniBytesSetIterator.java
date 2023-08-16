package com.baidu.hugegraph.util.collection;

import java.util.Iterator;

public class JniBytesSetIterator extends NativeReference implements Iterator<byte[]> {
    long handle;

    public JniBytesSetIterator(long handle) {
        this.handle = handle;
    }
    private native boolean hasNext(long handle);
    @Override
    public boolean hasNext(){
        return handle != 0 ? hasNext(handle) : false;
    }
    private native byte[] next(long handle);
    @Override
    public byte[] next(){
        if ( handle != 0) {
            return next(handle);
        }
        throw new NullPointerException();
    }
    @Override
    public void close() {
        if (handle != 0) {
            deleteNative(handle);
            handle = 0;
        }
    }

    private native void deleteNative(long handle);
}
