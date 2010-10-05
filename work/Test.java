import java.util.*;
import java.io.*;

import java.util.concurrent.locks.*;
import java.util.*;


public class Test {

    public static void main(String[] args) {
        try {
            Object[] accessors = new Object[5];
            Object[] accessor = null;
            ValueCacheMap valueCacheMap = new ValueCacheMap(6);
        

            for (int i = 0; i < 5; i++) {

                accessor = new Object[5];
                File file = new File("./datafile" + (i+1) + ".txt");
                BufferedWriter wr = new BufferedWriter(new FileWriter(file, true));
                RandomAccessFile raf = new RandomAccessFile(file, "rwd");
                accessor[0] = file;
                accessor[1] = wr;
                accessor[2] = raf;
                accessor[3] = file.length();

                if (file.length() == 0) {
                    accessor[4] = 0;
                } else {
                    accessor[4] = file.length() - 1;
                }


                int mapperLen = 1024 * 1024;

                if (file.length() * 2 > mapperLen) {
                    mapperLen = new Long(file.length()).intValue() * 2;
                }

                byte[] lineBufs = new byte[mapperLen];

                raf.seek(0);
                int readLen = -1;

                long start = System.nanoTime();
                if((readLen = raf.read(lineBufs)) != -1) {
                    System.out.println(readLen);
                    long end = System.nanoTime();

                }

                valueCacheMap.put(new Integer(i),lineBufs);
                long end2 = System.nanoTime();

                accessors[i] = accessor;
            }



System.out.println("==========================================");
            //long start = System.nanoTime();
            byte[] testData = (byte[])valueCacheMap.get(new Integer(0));
            matchByte("1234567891012345678910123456789101234567891012345678910123456789101234567891012345678910&".getBytes(), testData);
            //long end = System.nanoTime();
            //System.out.println((end - start));
            
System.out.println("==========================================");
            testData = (byte[])valueCacheMap.get(new Integer(1));
            //start = System.nanoTime();

            matchByte("1234567891012345678910123456789101234567891012345678910123456789101234567891012345678910&".getBytes(), testData);
            //end = System.nanoTime();
            //System.out.println((end - start));


    

        } catch (Exception e) {
            e.printStackTrace();
        } 
    }

    /**
     *
     */
    public static boolean matchByte(byte[] targetStr, byte[] matchData) {
        boolean matchFlg = true;
        String tmpValue = null;
        int targetSize = targetStr.length;
        long lineCount = 0L;


        int lineDataSize = 120;
        int loop = matchData.length / lineDataSize;


        long start = System.nanoTime();

        for (int loopIdx = 0; loopIdx < loop; loopIdx++) {

            int assist = (lineDataSize * loopIdx);
            lineCount++;

            matchFlg = true;
            if (targetStr[0] == matchData[assist] && targetStr[targetSize - 1] == matchData[assist + targetSize - 1]) {

                for (int i = 0; i < targetSize; i++) {


                    if (targetStr[i] != matchData[assist + i]) {
                        matchFlg = false;
                        break;
                    }
                }
            } else {
                matchFlg = false;
            }

            // マッチした場合のみ文字列化
            if (matchFlg) {
                //tmpValue = new String(matchData, assist, 120);
                //System.out.println(lineCount);
                break;
            }
        }

        long end = System.nanoTime();
        System.out.println((end -  start));
        return matchFlg;
    }
}

class ValueCacheMap extends LinkedHashMap {

    private boolean fileWrite = false;

    private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
    private final Lock r = rwl.readLock();
    private final Lock w = rwl.writeLock();

    private int maxCacheSize = 5;

    // コンストラクタ
    public ValueCacheMap() {
        super(1024, 0.75f, true);
    }


    // コンストラクタ
    public ValueCacheMap(int maxCacheCapacity) {
        super(maxCacheCapacity, 0.75f, true);
        maxCacheSize = maxCacheCapacity;
    }


    /**
     * set<br>
     *
     * @param key
     * @param value
     */
    public Object put(Object key, Object value) {
        w.lock();
        try { 
            return super.put(key, value);
        } finally {
            w.unlock(); 
        }
    }


    /**
     * containsKey<br>
     *
     * @param key
     * @return boolean
     */
    public boolean containsKey(Object key) {
        r.lock();
        try { 
            return super.containsKey(key);
        } finally { 
            r.unlock(); 
        }
    }


    /**
     * get<br>
     *
     * @param key
     * @return Object
     */
    public Object get(Object key) {
        r.lock();
        try { 
            return super.get(key); 
        } finally { 
            r.unlock(); 
        }
    }


    /**
     * remove<br>
     *
     * @param key
     * @return Object
     */
    public Object remove(Object key) {
        w.lock();
        try {
            return super.remove(key);
        } finally {
            w.unlock(); 
        }
    }


    /**
     * clear<br>
     *
     */
    public void clear() {
        w.lock();
        try { 
            super.clear();
        } finally {
            w.unlock(); 
        }
    }


    /**
     * 削除指標実装.<br>
     */
    protected boolean removeEldestEntry(Map.Entry eldest) {
        return size() > maxCacheSize;
    }
}
