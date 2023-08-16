
package org.example;

import com.baidu.hugegraph.util.collection.JniLongSet;
import com.baidu.hugegraph.util.collection.JniLongSetIterator;
import com.baidu.hugegraph.util.collection.JniSetLoader;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

public class ConcurrentLongSet implements Set<Long> {
    static {
      //  JniSetLoader.loadLibrary();
    }
    JniLongSet jniObj;

    public ConcurrentLongSet() {
        jniObj = new JniLongSet(6, 0);
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
        return jniObj.contains((Long) value);
    }
    @Override
    public Iterator<Long> iterator() {
        return new Iterator<Long>() {

            JniLongSetIterator iterator = jniObj.iterator();
            @Override
            public boolean hasNext() {
                boolean has = iterator != null && iterator.hasNext();
                if (!has && iterator != null) {
                    iterator.close();
                }
                return has;
            }

            @Override
            public Long next() {
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
    public boolean add(Long value) {
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
    public boolean addAll(Collection<? extends Long> c) {
        boolean change = false;
        if (c instanceof ConcurrentLongSet) {
            jniObj.addAll(((ConcurrentLongSet) c).jniObj);
            return true;
        } else {
            for (Long value : c) {
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