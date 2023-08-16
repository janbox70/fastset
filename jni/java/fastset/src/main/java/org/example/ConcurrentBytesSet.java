
package org.example;

import com.baidu.hugegraph.util.collection.JniBytesSet;
import com.baidu.hugegraph.util.collection.JniBytesSetIterator;
import com.baidu.hugegraph.util.collection.JniLongSet;
import com.baidu.hugegraph.util.collection.JniLongSetIterator;
import com.baidu.hugegraph.util.collection.JniSetLoader;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

public class ConcurrentBytesSet implements Set<byte[]> {
    static {
      //  JniSetLoader.loadLibrary();
    }
    JniBytesSet jniObj;

    public ConcurrentBytesSet() {
        jniObj = new JniBytesSet(6, 0);
    }

    public void close() {
        jniObj.close();
    }

    @Override
    public int size() {
        return (int) jniObj.size();
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean contains(Object value) {
        return jniObj.contains((byte[]) value);
    }
    @Override
    public Iterator<byte[]> iterator() {
        return new Iterator<byte[]>() {

            JniBytesSetIterator iterator = jniObj.iterator();
            @Override
            public boolean hasNext() {
                boolean has = iterator != null && iterator.hasNext();
                if (!has && iterator != null) {
                    close();
                }
                return has;
            }

            @Override
            public byte[] next() {
                if (iterator != null) {
                    return iterator.next();
                }
                return null;
            }
        };
    }

    @Override
    public Object[] toArray() {
        throw new RuntimeException("Method not implement.");
    }

    @Override
    public <T> T[] toArray(T[] a) {
        throw new RuntimeException("Method not implement.");
    }

    @Override
    public boolean add(byte[] value) {
        return jniObj.add(value);
    }

    @Override
    public boolean remove(Object o) {
        throw new RuntimeException("Method not implement.");
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        throw new RuntimeException("Method not implement.");
    }

    @Override
    public boolean addAll(Collection<? extends byte[]> c) {
        boolean change = false;
        if (c instanceof ConcurrentBytesSet) {
            jniObj.addAll(((ConcurrentBytesSet) c).jniObj);
            return true;
        } else {
            for (byte[] value : c) {
                change = add(value) | change;
            }
        }
        return change;
    }


    @Override
    public boolean retainAll(Collection<?> c) {
        throw new RuntimeException("Method not implement.");
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new RuntimeException("Method not implement.");
    }

    @Override
    public void clear() {
        jniObj.clear();
    }
}