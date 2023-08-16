package com.baidu.hugegraph.util.collection;

import java.util.Iterator;

public class JniLongSetIterator extends NativeReference implements Iterator<Long> {
    long handle;

    public JniLongSetIterator(long handle) {
        this.handle = handle;
    }

    private native boolean hasNext(long handle);

    @Override
    public boolean hasNext() {
        return handle != 0 ? hasNext(handle) : false;
    }

    private native long next(long handle);

    @Override
    public Long next(){
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
