package org.example;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * Unit test for simple App.
 */

public class AppTest {

    static {
        System.load("/tmp/libJniFastSet.so");
    }
    @Test
    public void testLongSet() {

        ConcurrentLongSet set = new ConcurrentLongSet();
        for (int i = 0; i < 100; i++)
            set.add((long) i);
        Assert.assertEquals(100, set.size());

        for (int i = 0; i < 100; i++)
            Assert.assertTrue(set.contains((long) i));

        int counter = 0;
        Iterator<Long> itr = set.iterator();
        while (itr.hasNext()) {
            itr.next();
            counter++;
        }
        Assert.assertEquals(100, counter);


        ConcurrentLongSet set2 = new ConcurrentLongSet();
        for (int i = 50; i < 150; i++)
            set2.add((long) i);
        set2.addAll(set);
        Assert.assertEquals(150, set2.size());

        set.clear();
        Assert.assertEquals(0, set.size());
    }

    @Test
    public void testBytesSet() {
        ConcurrentBytesSet set = new ConcurrentBytesSet();
        
        ArrayList<byte[]> data = new ArrayList<byte[]>();
        for( int i = 0; i < 200; i ++) {
            byte[] bytes = new byte[i%19 + 4];
            for(int n = 0; n < bytes.length; n++) {
                bytes[n] = (byte) (i+n);
            }
            data.add(bytes);
        }

        for (int i = 0; i < 100; i++)
            set.add(data.get(i));
        Assert.assertEquals(100, set.size());

        for (int i = 0; i < 100; i++)
            Assert.assertTrue(set.contains(data.get(i)));

        int counter = 0;
        Iterator<byte[]> itr = set.iterator();
        while (itr.hasNext()) {
            itr.next();
            counter++;
        }
        Assert.assertEquals(100, counter);


        ConcurrentBytesSet set2 = new ConcurrentBytesSet();
        for (int i = 50; i < 150; i++)
            set2.add(data.get(i));
        set2.addAll(set);
        Assert.assertEquals(150, set2.size());

        set.clear();
        Assert.assertEquals(0, set.size());
    }

}