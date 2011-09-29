package test;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;

import okuyama.imdst.util.serializemap.*;


public class SerializeMapGetTest extends Thread {

    private static String serializeClassName = "okuyama.imdst.util.serializemap.ObjectStreamSerializer";
    private static boolean status = true;

    private static Map testMap = new SerializeMap(2000000, 1900000, 1000000, serializeClassName);
    //private static Map testMap = new ConcurrentHashMap(2000000, 1900000, 64);

    private volatile static int testCount = -1;

    public static ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
    public static Lock r = rwl.readLock();
    public static Lock w = rwl.writeLock();


    public int execCount = 0;

    public SerializeMapGetTest() {
    }

    public static void main(String[] args) {
        try {
            long totalExecCount = 0L;
            int maxThreads = Integer.parseInt(args[0]);
            testCount = Integer.parseInt(args[1]);
            w.lock();
            SerializeMap initMap = new SerializeMap(100, 90, 10, serializeClassName);
            initMap.put("a", "b");
            initMap.get("a");
            initMap = null;

            for (int i = 0; i < testCount; i++) {

                testMap.put("KeyABCDEFG_" + i, "Value123456789ABCDEFGHIJKLMNOPQRSTUWXYZ_" + i);
                if((i % 100000) == 0) System.out.println("Set Count=" + i);
            }
            SerializeMapGetTest[] tList = new SerializeMapGetTest[maxThreads];

            for (int idx = 0; idx < maxThreads; idx++) {
                SerializeMapGetTest me = new SerializeMapGetTest();
                me.start();
                tList[idx] = me;
            }
            
            w.unlock();
            Thread.sleep(10000);
            SerializeMapGetTest.status = false;
            int allCount = 0;

            for (int idx = 0; idx < maxThreads; idx++) {
                tList[idx].join();
                allCount += tList[idx].execCount;
            }
            System.out.println("TotalExecCount = " + allCount);
            System.out.println("QPS = " + (allCount / 10));
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void run () {
        try {
            r.lock();
            r.unlock();
            String key = "KeyABCDEFG_";
            Random rnd = new Random();

            while (SerializeMapGetTest.status) {
                testMap.get(key + rnd.nextInt(testCount));
                execCount++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
}