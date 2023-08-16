package org.example;

import com.baidu.hugegraph.util.collection.JniBytesSet;
import com.baidu.hugegraph.util.collection.JniLongSet;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Hello world!
 */
public class App {

    public static void main(String[] args) throws IOException, InterruptedException {

        if (args.length < 2) {
            System.out.println("FilePath totalCount threads times type(0|1)");
            System.exit(0);
        }
        System.load("/tmp/libJniFastSet.so");

        String filePath = args[0];

        int total = Integer.valueOf(args[1]);

        int threads = 64;
        if (args.length > 2)
            threads = Integer.valueOf(args[2]);

        int times = 5;
        if (args.length > 3)
            times = Integer.valueOf(args[3]);
        int type = 0;
        if (args.length > 4)
            type = Integer.valueOf(args[4]);


        if (type == 0) {
            TwitterLongTest test = new TwitterLongTest(total);
            test.readyData(new File(filePath));

            System.out.println("\nTest " + times + " Java HashMap" + "Threads is " + threads);
            long start = System.currentTimeMillis();
            for (int j = 0; j < 1; j++) {
                Set<Long> sets = ConcurrentHashMap.newKeySet();
                test.testSet(sets, threads);
                sets.clear();
                System.gc();
            }

            System.out.println("" + times + " total time is " + (System.currentTimeMillis() - start));
            System.out.println("\nTest " + times + " long FastSet");
            start = System.currentTimeMillis();
            for (int j = 0; j < times; j++) {
                Set<Long> sets = new ConcurrentLongSet();
                test.testSet(sets, threads);
                sets.clear();
                if (sets instanceof ConcurrentLongSet)
                    ((ConcurrentLongSet) sets).close();
            }
            System.out.println("" + times + " total time is " + (System.currentTimeMillis() - start));
        }
        if (type == 1) {
            TwitterBytesTest test = new TwitterBytesTest(total);
            test.readyData(new File(filePath));

            System.out.println("\nTest " + times + " bytes  FastSet");
            long start = System.currentTimeMillis();
            for (int j = 0; j < times; j++) {
                Set<byte[]> sets = new ConcurrentBytesSet();
                test.testSet(sets, threads);
                sets.clear();
                if (sets instanceof ConcurrentBytesSet)
                    ((ConcurrentBytesSet) sets).close();
            }
            System.out.println("" + times + " total time is " + (System.currentTimeMillis() - start));
        } else if (type == 2) {
            System.out.println("\nTest " + times + " init  JniLongSet");
            long start = System.currentTimeMillis();
            JniLongSet[] set = new JniLongSet[times];
            for (int j = 0; j < times; j++) {
                set[j] = new JniLongSet(6, 0);
                set[j].add(1);
            }
            long time1 = System.currentTimeMillis() - start;
            for (int j = 0; j < times; j++) {
                set[j].close();
            }
            System.out.println("" + times + " total time is " + time1 + "/" + (System.currentTimeMillis() - start));

        } else if (type == 3) {
            System.out.println("\nTest " + times + " init  JniBytesSet");
            byte[] tmp = "1".getBytes();
            long start = System.currentTimeMillis();

            JniBytesSet[] set = new JniBytesSet[times];
            for (int j = 0; j < times; j++) {
                set[j] = new JniBytesSet(6, 0);
                set[j].add(tmp);
            }
            long time1 = System.currentTimeMillis() - start;
            for (int j = 0; j < times; j++) {
                set[j].close();
            }
            System.out.println("" + times + " total time is " + time1 + "/" + (System.currentTimeMillis() - start));

        } else  if (type == 4) {
            System.out.println("\nTest " + times + " init  JniBytesSet");
            byte[] tmp = "1".getBytes();
            long start = System.currentTimeMillis();
            JniBytesSet[] set = new JniBytesSet[times];
            JniLongSet[] set2 = new JniLongSet[times];
            for (int j = 0; j < times; j++) {
                set[j] = new JniBytesSet(6, 0);
                set[j].add(tmp);
                set2[j] = new JniLongSet(6, 0);
                set2[j].add(1);
            }
            long time1 = System.currentTimeMillis() - start;
            for (int j = 0; j < times; j++) {
                set[j].close();
                set2[j].close();
            }
            System.out.println("" + times + " total time is " + time1 + "/" + (System.currentTimeMillis() - start));
        }else if (type == 5) {
            System.out.println("\nTest " + times + " init  JniLongSet");
            long start = System.currentTimeMillis();
            long count = 0;
            JniLongSet set = new JniLongSet(6, 0);
            for (int j = 0; j < times; j++) {
                set.clear();;
                set.add(1);
                count += set.size();
            }

            System.out.println("" + times + " total time is " + (System.currentTimeMillis() - start) + "   " + count);

        }else if (type == 6) {
            System.out.println("\nTest " + times + " init  JniLongSet");
            long start = System.currentTimeMillis();
            JniLongSet[] set = new JniLongSet[times];
            for (int j = 0; j < times; j++) {
                set[j] = new JniLongSet(6, 0);
                set[j].add(1);
                set[j].close();
            }

            System.out.println("" + times + " total time is " + (System.currentTimeMillis() - start));

        }
    }
}
