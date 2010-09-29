import java.util.*;
import java.io.*;

import java.util.concurrent.locks.*;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;

public class Main extends Thread {

    public volatile int name = -1;

    public static int dataSize = 10000000;

    public static FileHashMap[] fileHashMaps = null;

    public static void main(String[] args) {
        fileHashMaps = new FileHashMap[2];
        String[] dirs1 = {"/usr/local/okuyama/work1/data1/","/usr/local/okuyama/work1/data2/","/usr/local/okuyama/work1/data3/","/usr/local/okuyama/work1/data4/","/usr/local/okuyama/work1/data5/","/usr/local/okuyama/work1/data6/","/usr/local/okuyama/work1/data7/","/usr/local/okuyama/work1/data8/"};
        String[] dirs2 = {"/usr/local/okuyama/work2/data1/","/usr/local/okuyama/work2/data2/","/usr/local/okuyama/work2/data3/","/usr/local/okuyama/work2/data4/","/usr/local/okuyama/work2/data5/","/usr/local/okuyama/work2/data6/","/usr/local/okuyama/work2/data7/","/usr/local/okuyama/work2/data8/"};
        fileHashMaps[0] = new FileHashMap(dirs1);
        fileHashMaps[1] = new FileHashMap(dirs2);

        Main[] me = new Main[Integer.parseInt(args[0])];


        try {
            long start2 = System.nanoTime();


            fileHashMaps[getHashCode("keyAbCddEfGhIjK`;:8547asdf7822kuioZj_201_0_848558") % fileHashMaps.length].put("keyAbCddEfGhIjK`;:8547asdf7822kuioZj_201_0_848558", "123");
            long end2 = System.nanoTime();
            System.out.println((end2 - start2));

            start2 = System.nanoTime();
            fileHashMaps[getHashCode("keyAbCddEfGhIjK`;:8547asdf7822kuioZj_201_0_177999") % fileHashMaps.length].get("keyAbCddEfGhIjK`;:8547asdf7822kuioZj_201_0_177999");
            end2 = System.nanoTime();
            System.out.println((end2 - start2));

            for (int i = 0; i < Integer.parseInt(args[0]); i++) {
                me[i] = new Main(i);
            }

            for (int i = 0; i < Integer.parseInt(args[0]); i++) {
                me[i].start();
            }

            for (int i = 0; i < Integer.parseInt(args[0]); i++) {
                me[i].join();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Main(int setName) {
        this.name = setName;
    }


    public static int getHashCode(String key) {
        int index = key.hashCode();
        if (index < 0) {
            index = index - index - index;
        }

        return index;
    }


    public void run() {
        exec();
    }

    public void exec() {
// getHashCode() % fileHashMaps.length
        long start1 = 0L;
        long end1 = 0L;
        Random rdn = new Random();

        long start = System.currentTimeMillis();
        for (int i = 0; i < dataSize / 5; i++) {

            String rndKey = "keyAbCddEfGhIjK`;:8547asdf7822kuioZj_201" + "0" + "_" + rdn.nextInt(30000000);
            String var = fileHashMaps[getHashCode(rndKey) % fileHashMaps.length].get(rndKey);
            if ((i % 10000) == 0) {
                if (var == null) break;
                long end2 = System.currentTimeMillis();
                System.out.println(i + "=" + (end2 - start1) + "[" + var + "]");
                start1 = System.currentTimeMillis();
            }
        }
        long end = System.currentTimeMillis();
        System.out.println("Data Read Time = [" + (end - start) + "]");

        System.out.println("getCacheSize-1[" + fileHashMaps[0].getCacheSize() + "]");
        System.out.println("getCacheSize-2[" + fileHashMaps[1].getCacheSize() + "]");
        start = System.currentTimeMillis();
        start1 = System.currentTimeMillis();
        for (int i = 0; i < dataSize; i++) {
            String putKey = "keyAbCddEfGhIjK`;:8547asdf7822kuioZj_201" + this.name + "_" + i;
            fileHashMaps[getHashCode(putKey) % fileHashMaps.length].put(putKey, new Integer(i).toString() + "_" + "1");
            if ((i % 10000) == 0) {
                end1 = System.currentTimeMillis();
                System.out.println(i + "=" + (end1 - start1));
                start1 = System.currentTimeMillis();
            }
        }
        end = System.currentTimeMillis();
        System.out.println((end - start));
        System.out.println("Data Write Time = [" + (end - start) + "]");
        System.out.println("getCacheSize-1[" + fileHashMaps[0].getCacheSize() + "]");
        System.out.println("getCacheSize-2[" + fileHashMaps[1].getCacheSize() + "]");
        System.out.println("------------------ END-1 -------------------");


        start = System.currentTimeMillis();
        for (int i = dataSize; i < dataSize * 2; i++) {

            String rndKey = "keyAbCddEfGhIjK`;:8547asdf7822kuioZj_201" + "0" + "_" + rdn.nextInt(10000000);
            String var = fileHashMaps[getHashCode(rndKey) % fileHashMaps.length].get(rndKey);
            if ((i % 10000) == 0) {
                if (var == null) break;
                long end2 = System.currentTimeMillis();
                System.out.println(i + "=" + (end2 - start1) + "[" + var + "]");
                start1 = System.currentTimeMillis();
            }
        }
        end = System.currentTimeMillis();
        System.out.println("Data Read Time = [" + (end - start) + "]");

        System.out.println("getCacheSize-1[" + fileHashMaps[0].getCacheSize() + "]");
        System.out.println("getCacheSize-2[" + fileHashMaps[1].getCacheSize() + "]");

        start = System.currentTimeMillis();
        start1 = System.currentTimeMillis();

        for (int i = dataSize; i < dataSize * 2; i++) {
            String putKey = "keyAbCddEfGhIjK`;:8547asdf7822kuioZj_201" + this.name + "_" + i;
            fileHashMaps[getHashCode(putKey) % fileHashMaps.length].put(putKey, new Integer(i).toString() + "_" + "2");
            if ((i % 10000) == 0) {
                end1 = System.currentTimeMillis();
                System.out.println(i + "=" + (end1 - start1));
                start1 = System.currentTimeMillis();
            }
        }
        end = System.currentTimeMillis();
        System.out.println((end - start));
        System.out.println("Data Write Time = [" + (end - start) + "]");
        System.out.println("getCacheSize-1[" + fileHashMaps[0].getCacheSize() + "]");
        System.out.println("getCacheSize-2[" + fileHashMaps[1].getCacheSize() + "]");
        System.out.println("------------------ END-2 -------------------");


        start = System.currentTimeMillis();
        for (int i = dataSize * 2; i < dataSize * 3; i++) {

            String rndKey = "keyAbCddEfGhIjK`;:8547asdf7822kuioZj_201" + "0" + "_" + rdn.nextInt(20000000);
            String var = fileHashMaps[getHashCode(rndKey) % fileHashMaps.length].get(rndKey);
            if ((i % 10000) == 0) {
                if (var == null) break;
                long end2 = System.currentTimeMillis();
                System.out.println(i + "=" + (end2 - start1) + "[" + var + "]");
                start1 = System.currentTimeMillis();
            }
        }
        end = System.currentTimeMillis();
        System.out.println("Data Read Time = [" + (end - start) + "]");

        System.out.println("getCacheSize-1[" + fileHashMaps[0].getCacheSize() + "]");
        System.out.println("getCacheSize-2[" + fileHashMaps[1].getCacheSize() + "]");

        start = System.currentTimeMillis();
        start1 = System.currentTimeMillis();

        for (int i = dataSize * 2; i < dataSize * 3; i++) {
            String putKey = "keyAbCddEfGhIjK`;:8547asdf7822kuioZj_201" + this.name + "_" + i;
            fileHashMaps[getHashCode(putKey) % fileHashMaps.length].put(putKey, new Integer(i).toString() + "_" + "3");
            if ((i % 10000) == 0) {
                end1 = System.currentTimeMillis();
                System.out.println(i + "=" + (end1 - start1));
                start1 = System.currentTimeMillis();
            }
        }
        end = System.currentTimeMillis();
        System.out.println((end - start));
        System.out.println("Data Write Time = [" + (end - start) + "]");
        System.out.println("getCacheSize-1[" + fileHashMaps[0].getCacheSize() + "]");
        System.out.println("getCacheSize-2[" + fileHashMaps[1].getCacheSize() + "]");

        System.out.println("------------------ END-3 -------------------");


    }
}

class FileHashMap extends Thread {

    String[] baseFileDirs = {"./data/data1/","./data/data2/"};
    String[] fileDirs = null;

    ArrayBlockingQueue writeQueue = new ArrayBlockingQueue(10);

    // 割り当てるメモリ量を超えないようにする
    // たとえば1ファイルが1MBだとすると1MB(1024 * 1024) / (64 + 11)byte * 8192 = 格納件数になる
    // 均等にKeyが分散した場合は1億件以上管理出来る
    ValueCacheMap valueCacheMap = new ValueCacheMap(128);

    int keyDataLength = 64;

    int oneDataLength = 11;

    int lineDataSizeNoRt =  keyDataLength + oneDataLength;

    int lineDataSize =  keyDataLength + oneDataLength;

    // 一度に取得するデータサイズ
    int getDataSize = lineDataSize * 108;

    // ファイル数 ファイルストリーム数になる(1ストリームあたり25KBになる)
    int accessCount = 1024 * 8;

    Object[] fileAccessList = new Object[accessCount];

    public FileHashMap(String[] dirs) {
        this.baseFileDirs = dirs;
        try {
            fileDirs = new String[baseFileDirs.length * 10];
            int counter = 0;
            for (int idx = 0; idx < baseFileDirs.length; idx++) {
                for (int idx2 = 0; idx2 < 10; idx2++) {

                    fileDirs[counter] = baseFileDirs[idx] + idx2 + "/";
                    File dir = new File(fileDirs[counter]);
                    if (!dir.exists()) dir.mkdirs();
                    counter++;
                }
            }

            for (int i = 0; i < accessCount; i++) {
                File file = new File(fileDirs[i % fileDirs.length] + i + ".data");

                BufferedWriter wr = new BufferedWriter(new FileWriter(file, true));

                RandomAccessFile raf = new RandomAccessFile(file, "rw");
                Object[] accessors = new Object[3];
                accessors[0] = file;
                accessors[1] = raf;
                accessors[2] = wr;
                fileAccessList[i] = accessors;
            }
            this.start();
            System.out.println("Start OK");


            Runtime runtime = Runtime.getRuntime();
System.out.println("--------------------");
            System.out.println((runtime.totalMemory() / 1000) + "KB");
            System.out.println((runtime.freeMemory() / 1000) + "KB");
System.out.println("--------------------");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void run() {
        try {
            while(true) {

                WriteContener writeContener = (WriteContener)writeQueue.take();

                synchronized (writeContener.raf) {
                    writeContener.raf.seek(writeContener.seekPoint);
                    writeContener.raf.write(writeContener.writeDatas, 0, lineDataSize);
                    writeContener = null;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void put(String key, String value) {
        try {
            int index = key.hashCode();
            if (index < 0) {
                index = index - index - index;
            }

            Object[] accessors = (Object[])fileAccessList[index % accessCount];

            RandomAccessFile raf = (RandomAccessFile)accessors[1];

            BufferedWriter wr = (BufferedWriter)accessors[2];

            StringBuffer buf = new StringBuffer(this.fillCharacter(key, keyDataLength));
            buf.append(this.fillCharacter(value, oneDataLength));

            synchronized (raf) {

                long dataLineNo = this.getLinePoint(key);
                if (dataLineNo == -1) {
                    wr.write(buf.toString());
                    wr.flush();
                    byte[] keyStrBytes = buf.toString().getBytes();

                    CacheContener cacheContener = (CacheContener)valueCacheMap.get(new Integer(index % accessCount));
                    byte[] lineBufs = cacheContener.datas;
                    int nowSize = cacheContener.size;

                    if ((nowSize + lineDataSize) >= lineBufs.length) {

                        byte[] newBufs = new byte[lineBufs.length * 2];

                        for (int oldIdx = 0; oldIdx < lineBufs.length; oldIdx++) {
                            newBufs[oldIdx] = lineBufs[oldIdx];
                        }
                        lineBufs = newBufs;
                    }

                    int keyIdx = 0;
                    for(int cpIdx = nowSize; cpIdx < (nowSize + lineDataSize); cpIdx++) {

                        lineBufs[cpIdx] = keyStrBytes[keyIdx];
                        keyIdx++;
                    }

                    cacheContener.datas = lineBufs;
                    cacheContener.size = nowSize + lineDataSize;
                    valueCacheMap.put(new Integer(index % accessCount), cacheContener);
                } else {


                    //raf.seek(dataLineNo * lineDataSize);
                    //raf.write(buf.toString().getBytes(), 0, lineDataSize);
                    byte[] keyStrBytes = buf.toString().getBytes();

                    CacheContener cacheContener = (CacheContener)valueCacheMap.get(new Integer(index % accessCount));
                    byte[] lineBufs = cacheContener.datas;
                    int keyIdx = 0;

                    for(int cpIdx = new Long(dataLineNo * lineDataSize).intValue(); cpIdx < ((dataLineNo * lineDataSize) + lineDataSize); cpIdx++) {

                        lineBufs[cpIdx] = keyStrBytes[keyIdx];
                        keyIdx++;
                    }
                    valueCacheMap.put(new Integer(index % accessCount), cacheContener);


                    raf.seek(dataLineNo * lineDataSize);
                    raf.write(keyStrBytes, 0, lineDataSize);
                    // Queue Register
                    /*WriteContener writeContener = new WriteContener();
                    writeContener.raf = raf;
                    writeContener.seekPoint = dataLineNo * lineDataSize;
                    writeContener.writeDatas = keyStrBytes;

                    writeQueue.put(writeContener);*/
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public long getLinePoint(String key) {

        long line = -1;
        long lineCount = 0L;


        String ret = null;
        byte[] keyBytes = key.getBytes();
        byte[] equalKeyBytes = new byte[keyBytes.length + 1];
        byte[] lineBufs = null;
        boolean matchFlg = true;

        // マッチング用配列作成
        for (int idx = 0; idx < keyBytes.length; idx++) {
            equalKeyBytes[idx] = keyBytes[idx];
        }

        equalKeyBytes[equalKeyBytes.length - 1] = 38;

        try {
            int index = key.hashCode();
            if (index < 0) {
                index = index - index - index;
            }

            Object[] accessors = (Object[])fileAccessList[index % accessCount];

            RandomAccessFile raf = (RandomAccessFile)accessors[1];

            raf.seek(0);
            int readLen = -1;
            CacheContener cacheContener = (CacheContener)valueCacheMap.get(new Integer(index % accessCount));


            if (cacheContener == null) {

                int size = new Long(((File)accessors[0]).length() * 2).intValue();
                if (size < this.getDataSize) size = this.getDataSize;
                lineBufs = new byte[size];
                readLen = raf.read(lineBufs);

                int bufSize = readLen;
                if (readLen == -1 || readLen == 0) bufSize = 0;

                cacheContener = new CacheContener();
                cacheContener.datas = lineBufs;
                cacheContener.size = bufSize;
                valueCacheMap.put(new Integer(index % accessCount), cacheContener);
            } else {
                lineBufs = cacheContener.datas;
                readLen = cacheContener.size;
            }

            if(readLen != -1) {

                matchFlg = true;

                int loop = readLen / lineDataSize;

                for (int loopIdx = 0; loopIdx < loop; loopIdx++) {

                    int assist = (lineDataSize * loopIdx);

                    matchFlg = true;
                    if (equalKeyBytes[equalKeyBytes.length - 1] == lineBufs[assist + (equalKeyBytes.length - 1)]) {

                        for (int i = 0; i < equalKeyBytes.length; i++) {

                            if (equalKeyBytes[i] != lineBufs[assist + i]) {
                                matchFlg = false;
                                break;
                            }
                        }
                    } else {
                        matchFlg = false;
                    }
                    // マッチした場合のみ文字列化
                    if (matchFlg) {
                        line = lineCount;
                        break;
                    }
                    lineCount++;
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return line;
    }




    public String get(String key) {
        String tmpValue = null;

        String ret = null;
        byte[] keyBytes = key.getBytes();
        byte[] equalKeyBytes = new byte[keyBytes.length + 1];
        byte[] lineBufs = null;
        boolean matchFlg = true;

        // マッチング用配列作成
        for (int idx = 0; idx < keyBytes.length; idx++) {
            equalKeyBytes[idx] = keyBytes[idx];
        }

        equalKeyBytes[equalKeyBytes.length - 1] = 38;

        try {
            int index = key.hashCode();
            if (index < 0) {
                index = index - index - index;
            }

            Object[] accessors = (Object[])fileAccessList[index % accessCount];

            RandomAccessFile raf = (RandomAccessFile)accessors[1];

            synchronized (raf) {
                raf.seek(0);
                int readLen = -1;
                CacheContener cacheContener = (CacheContener)valueCacheMap.get(new Integer(index % accessCount));


                if (cacheContener == null) {

                    int size = new Long(((File)accessors[0]).length() * 2).intValue();
                    if (size < this.getDataSize) size = this.getDataSize;
                    lineBufs = new byte[size];
                    readLen = raf.read(lineBufs);

                    int bufSize = readLen;

                    if (readLen == -1 || readLen == 0) bufSize = 0;

                    cacheContener = new CacheContener();
                    cacheContener.datas = lineBufs;
                    cacheContener.size = bufSize;
                    valueCacheMap.put(new Integer(index % accessCount), cacheContener);
                } else {

                    lineBufs = cacheContener.datas;
                    readLen = cacheContener.size;
                }

                if(readLen != -1) {

                    matchFlg = true;

                    int loop = readLen / lineDataSize;

                    for (int loopIdx = 0; loopIdx < loop; loopIdx++) {

                        int assist = (lineDataSize * loopIdx);

                        matchFlg = true;

                        if (equalKeyBytes[equalKeyBytes.length - 1] == lineBufs[assist + (equalKeyBytes.length - 1)]) {
                            for (int i = 0; i < equalKeyBytes.length; i++) {

                                if (equalKeyBytes[i] != lineBufs[assist + i]) {
                                    matchFlg = false;
                                    break;
                                }
                            }
                        } else {
                            matchFlg = false;
                        }

                        // マッチした場合のみ文字列化
                        if (matchFlg) {
                            tmpValue = new String(lineBufs, assist, lineDataSizeNoRt);
                            break;
                        }
                    }
                }
            }

            if (tmpValue != null) {
                byte[] workBytes = tmpValue.getBytes();

                if (workBytes[keyDataLength] != 38) {
                    int i = keyDataLength;
                    int counter = 0;
                    for (; i < workBytes.length; i++) {
                        if (workBytes[i] == 38) break;
                        counter++;
                    }

                    ret = new String(workBytes, keyDataLength, counter, "UTF-8");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ret;
    }

    private String fillCharacter(String data, int fixSize) {
        StringBuffer writeBuf = new StringBuffer(data);

        int valueSize = data.length();

        // 渡されたデータが固定の長さ分ない場合は足りない部分を補う
        // 足りない文字列は固定の"&"で補う(38)
        byte[] appendDatas = new byte[fixSize - valueSize];

        for (int i = 0; i < appendDatas.length; i++) {
            appendDatas[i] = 38;
        }

        writeBuf.append(new String(appendDatas));
        return writeBuf.toString();
    }

    public int getCacheSize() {
        return valueCacheMap.getSize();
    }

    class CacheContener {
        public byte[] datas = null;
        public int size = -1;
    }

    class WriteContener {
        public RandomAccessFile raf = null;
        public long seekPoint = -1;
        public byte[] writeDatas = null;
    }

}


class ValueCacheMap extends LinkedHashMap {

    private boolean fileWrite = false;

    private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
    private final Lock r = rwl.readLock();
    private final Lock w = rwl.writeLock();

    private int maxCacheSize = 16;

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

    public int getSize() {
        return size();
    }
}