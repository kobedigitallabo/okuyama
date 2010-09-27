import java.util.*;
import java.io.*;

import java.util.concurrent.locks.*;
import java.util.*;


public class Main extends Thread {

    public volatile int name = -1;

    public static int dataSize = 1000000;


    public static FileHashMap fileHashMap = null;

    public static void main(String[] args) {
        fileHashMap = new FileHashMap();

        Main[] me = new Main[Integer.parseInt(args[0])];


        try {
            long start2 = System.nanoTime();
            fileHashMap.put("keyAbCddEfGhIjK`;:p;:pp;:ppo8547asdf7822kuioZj_20100101235959999_1_177999", "123");
            long end2 = System.nanoTime();
            System.out.println((end2 - start2));

            start2 = System.nanoTime();
            fileHashMap.get("keyAbCddEfGhIjK`;:p;:pp;:ppo8547asdf7822kuioZj_20100101235959999_1_177999");
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

    public void run() {
        exec();
    }

    public void exec() {

        long start1 = 0L;
        long end1 = 0L;
        Random rdn = new Random();
        long start = System.currentTimeMillis();
        for (int i = 0; i < dataSize; i++) {
            fileHashMap.get("keyAbCddEfGhIjK`;:p;:pp;:ppo8547asdf7822kuioZj_20100101235959999_" + this.name + "_" + i);
            //fileHashMap.get("keyAbCddEfGhIjK`;:p;:pp;:ppo8547asdf7822kuioZj_20100101235959999_" + this.name + "_" + rdn.nextInt(10000));
        }
        long end = System.currentTimeMillis();
        System.out.println("Data Read Time = [" + (end - start) + "]");

        start = System.currentTimeMillis();
        for (int i = 0; i < dataSize; i++) {
            fileHashMap.put("keyAbCddEfGhIjK`;:p;:pp;:ppo8547asdf7822kuioZj_20100101235959999_" + this.name + "_" + i, new Integer(i).toString());
            if ((i % 10000) == 0) {
                end1 = System.currentTimeMillis();
                System.out.println(i + "=" + (end1 - start1));
                start1 = System.currentTimeMillis();
            }
        }
        end = System.currentTimeMillis();
        System.out.println((end - start));
        System.out.println("Data Write Time = [" + (end - start) + "]");

        start = System.currentTimeMillis();
        for (int i = 0; i < dataSize; i++) {

            fileHashMap.get("keyAbCddEfGhIjK`;:p;:pp;:ppo8547asdf7822kuioZj_20100101235959999_" + this.name + "_" + rdn.nextInt(dataSize));
        }
        end = System.currentTimeMillis();
        System.out.println("Data Read Time = [" + (end - start) + "]");

        start = System.currentTimeMillis();
        for (int i = 0; i < dataSize; i++) {
            fileHashMap.put("keyAbCddEfGhIjK`;:p;:pp;:ppo8547asdf7822kuioZj_20100101235959999_" + this.name + "_" + rdn.nextInt(dataSize), new Integer(i).toString());
            if ((i % 10000) == 0) {
                end1 = System.currentTimeMillis();
                System.out.println(i + "=" + (end1 - start1));
                start1 = System.currentTimeMillis();
            }
        }
        end = System.currentTimeMillis();
        System.out.println((end - start));
        System.out.println("Data Write Time = [" + (end - start) + "]");
        System.out.println("----------------------------------------");

    }
}

class FileHashMap {
    String[] baseFileDirs = {"C:/desktop/tools/java/okuyama/trunk/work/data/data1/","C:/desktop/tools/java/okuyama/trunk/work/data/data2/","C:/desktop/tools/java/okuyama/trunk/work/data/data3/","C:/desktop/tools/java/okuyama/trunk/work/data/data4/","C:/desktop/tools/java/okuyama/trunk/work/data/data5/","C:/desktop/tools/java/okuyama/trunk/work/data/data6/","C:/desktop/tools/java/okuyama/trunk/work/data/data7/","C:/desktop/tools/java/okuyama/trunk/work/data/data8/"};
    String[] fileDirs = null;

    ValueCacheMap valueCacheMap = new ValueCacheMap(1024);

    int keyDataLength = 128;

    int oneDataLength = 20;

    int lineDataSizeNoRt =  keyDataLength + oneDataLength;
    
    int lineDataSize =  keyDataLength + oneDataLength;

    // 一度に取得するデータサイズ
    int getDataSize = lineDataSize * 55;

    int accessCount = 512;

    Object[] fileAccessList = new Object[accessCount];

    public FileHashMap() {
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
            System.out.println("Start OK");
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

                    byte[] lineBufs = (byte[])valueCacheMap.get(new Integer(index % accessCount));
                    int nowSize = ((Integer)valueCacheMap.get((index % accessCount) + "size")).intValue();

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
                    valueCacheMap.put(new Integer(index % accessCount), lineBufs);
                    valueCacheMap.put((index % accessCount) + "size", nowSize + lineDataSize);
                } else {

                    raf.seek(dataLineNo * lineDataSize);
                    raf.write(buf.toString().getBytes(), 0, lineDataSize);

                    byte[] keyStrBytes = buf.toString().getBytes();

                    byte[] lineBufs = (byte[])valueCacheMap.get(new Integer(index % accessCount));
                    int keyIdx = 0;

                    for(int cpIdx = new Long(dataLineNo * lineDataSize).intValue(); cpIdx < ((dataLineNo * lineDataSize) + lineDataSize); cpIdx++) {

                        lineBufs[cpIdx] = keyStrBytes[keyIdx];
                        keyIdx++;
                    }
                    //valueCacheMap.put(new Integer(index % accessCount), lineBufs);
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
            lineBufs = (byte[])valueCacheMap.get(new Integer(index % accessCount));

            if (lineBufs == null) {

                int size = new Long(((File)accessors[0]).length() * 2).intValue();
                if (size < this.getDataSize) size = this.getDataSize;
                lineBufs = new byte[size];
                readLen = raf.read(lineBufs);
                valueCacheMap.put(new Integer(index % accessCount), lineBufs);
                int bufSize = readLen;

                if (readLen == -1 || readLen == 0) bufSize = 0;
                valueCacheMap.put((index % accessCount) + "size", new Integer(bufSize));
            } else {
                readLen = ((Integer)valueCacheMap.get((index % accessCount) + "size")).intValue();
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
                lineBufs = (byte[])valueCacheMap.get(new Integer(index % accessCount));

                if (lineBufs == null) {

                    int size = new Long(((File)accessors[0]).length() * 2).intValue();
                    if (size < this.getDataSize) size = this.getDataSize;
                    lineBufs = new byte[size];
                    readLen = raf.read(lineBufs);
                    valueCacheMap.put(new Integer(index % accessCount), lineBufs);

                    int bufSize = readLen;

                    if (readLen == -1 || readLen == 0) bufSize = 0;

                    valueCacheMap.put((index % accessCount) + "size", new Integer(bufSize));
                } else {
                    readLen = ((Integer)valueCacheMap.get((index % accessCount) + "size")).intValue();
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
}
