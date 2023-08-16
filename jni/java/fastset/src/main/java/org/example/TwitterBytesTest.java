package org.example;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

public class TwitterBytesTest {

    byte[][] sampleData;
    int totalCount;

    static AtomicLong testCounter = new AtomicLong(0);

    public TwitterBytesTest(int totalCount) {
        this.totalCount = totalCount;
        sampleData = new byte[totalCount][];
    }

    public void readyData(File file) throws IOException {
        // 读取文件
        try (InputStreamReader isr = new InputStreamReader(new FileInputStream(file), "UTF-8");
                BufferedReader reader = new BufferedReader(isr)) {
            String strLine;
            int count = 0;
            while (count < totalCount && (strLine = reader.readLine()) != null) {
                String[] data = strLine.split("\t");
                if (data.length > 1) {
                    sampleData[count++] = data[1].trim().getBytes();
                }
            }
            totalCount = count;
            System.out.println("Load data size is " + count);
        }
    }

    public void testSet(Set<byte[]> sets, int times) throws InterruptedException {
        int oneTotal = totalCount / times;
        long addTm = 0, containTm = 0, itrTm = 0;
        {
            long start = System.currentTimeMillis();
            CountDownLatch latch = new CountDownLatch(times);
            for (int i = 0; i < times; i++) {
                int t = oneTotal * i;
                Set<byte[]> finalSets = sets;
                new Thread(() -> {
                    for (int l = 0; l < oneTotal; l++) {
                        finalSets.add(sampleData[t + l]);
                    }
                    latch.countDown();
                }).start();
            }
            latch.await();
            addTm = (System.currentTimeMillis() - start);
        }

        {
            long start = System.currentTimeMillis();
            CountDownLatch latch = new CountDownLatch(times);
            for (int i = 0; i < times; i++) {
                int t = oneTotal * i;
                Set<byte[]> finalSets = sets;
                new Thread(() -> {
                    long c = 0;
                    for (int l = oneTotal - 1 ; l >= 0; l--) {
                        c = c + (finalSets.contains(sampleData[t + l]) ? 1 : 0);
                    }
                    testCounter.addAndGet(c);
                    latch.countDown();
                }).start();
            }
            latch.await();
            containTm = (System.currentTimeMillis() - start) ;
        }

        {
            long start = System.currentTimeMillis();

            Iterator<byte[]> iterator = sets.iterator();
            int counter = 0;
            while(iterator.hasNext()){
                iterator.next();
                counter++;
            }
            if ( counter != sets.size())
                System.out.println("Error, iterator count is " + counter);
            itrTm = (System.currentTimeMillis() - start) ;
        }
        System.out.println("Add time is " + addTm + " contains time is " + containTm +
                " iterator time is " + itrTm + " set size is " + sets.size());
    }
}
