package com.baidu.hugegraph.util.collection;

import java.io.Closeable;

public abstract class NativeReference implements Closeable {
    @Override
    @Deprecated
    protected void finalize() throws Throwable {
        close();
        super.finalize();
    }
}
