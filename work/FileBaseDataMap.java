//package okuyama.imdst.util;

import java.io.*;
import java.util.concurrent.locks.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.codec.digest.DigestUtils;

/**
 * To manage files using a key-value.<br>
 * A small amount of memory usage, so File.<br>
 * Memory capacity can be managed independently of the number of data.<br>
 *
 * Inside, you are using a CoreFileBaseDataMap.<br>
 * This class is passed as an argument in one directory CoreFileBaseDataMap assigned.<br>
 * The specified directory should be different disk performance can be improved.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class FileBaseDataMap {

    CoreFileBaseKeyMap[] coreFileBaseKeyMaps = null;

    int numberOfCoreMap = 0;

    // Using a single cache 25 KB per
    int innerCacheSizeTotal = 1024 * 4;


    public FileBaseDataMap(String[] baseDirs) {

        numberOfCoreMap = baseDirs.length;
        coreFileBaseKeyMaps = new CoreFileBaseKeyMap[baseDirs.length];
        int oneCacheSizePer = innerCacheSizeTotal / numberOfCoreMap;

        for (int idx = 0; idx < baseDirs.length; idx++) {
            String[] dir = {baseDirs[idx]};
            coreFileBaseKeyMaps[idx] = new CoreFileBaseKeyMap(dir, oneCacheSizePer);
        }
    }


    /**
     * put.<br>
     * 
     * @param key
     * @param value
     */
    public void put(String key, String value) {
        int hashCode = CoreFileBaseKeyMap.createHashCode(key);

        coreFileBaseKeyMaps[hashCode % numberOfCoreMap].put(key, value, hashCode);
    }


    /**
     * get.<br>
     * 
     * @param key
     */
    public String get(String key) {
        int hashCode = CoreFileBaseKeyMap.createHashCode(key);

        return coreFileBaseKeyMaps[hashCode % numberOfCoreMap].get(key, hashCode);
    }


    /**
     * remove.<br>
     * 
     * @param key
     */
    public String remove(String key) {
        int hashCode = CoreFileBaseKeyMap.createHashCode(key);

        return coreFileBaseKeyMaps[hashCode % numberOfCoreMap].remove(key, hashCode);
    }


    /**
     * containsKey.<br>
     *
     * @param key 
     */
    public boolean containsKey(String key) {
        boolean ret = true;
        int hashCode = CoreFileBaseKeyMap.createHashCode(key);

        if (coreFileBaseKeyMaps[hashCode % numberOfCoreMap].get(key, hashCode) == null) {
            ret = false;
        }
        return ret;
    }

    /**
     * size.<br>
     *
     */
    public int size() {
        int ret = 0;

        for (int idx = 0; idx < coreFileBaseKeyMaps.length; idx++) {
            ret = ret + coreFileBaseKeyMaps[idx].totalSize.intValue();
        }
        return ret;
    }

    /**
     * size.<br>
     *
     */
    public int entrySet() {
        int ret = 0;

        return ret;
    }
}


/**
 * Managing Data.<br>
 * Inner Class.<br>
 */
class CoreFileBaseDataMap {

    public static int lengthOfData = 1024;

    private String dataFileDir = null;

    private String dataFileName = null;

    private File dataFile = null;

    private RandomAccessFile raf = null;

    private BufferedWriter wr = null;



    public CoreFileBaseDataMap(String dataDir) {

        this.dataFileDir = dataDir;
        this.dataFileName = "datafile";
        this.dataFile = new File(this.dataFileDir);

        try {
            File dir = new File(this.dataFileDir);
            if (!dir.exists()) dir.mkdirs();
            this.dataFile.delete();

            this.raf = new RandomAccessFile(this.dataFile, "rwd");
            this.wr = new BufferedWriter(new FileWriter(this.dataFile, true));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public byte[] readData(long orderOfReg) throws Exception {
        byte[] ret = new byte[lengthOfData];
        try {
            raf.seek((orderOfReg - 1) * lengthOfData);
            raf.read(ret);
        } catch (Exception e) {
            throw e;
        }
        return ret;
    }


    public void writeExistingData(byte[] datas, long orderOfReg) throws Exception {
        try {
            raf.seek((orderOfReg - 1) * lengthOfData);
            raf.write(datas, 0, lengthOfData);
        } catch (Exception e) {
            throw e;
        }
    }


    public void writeNewData(byte[] datas) throws Exception {
        try {

            wr.write(new String(datas));
            wr.flush();
        } catch (Exception e) {
            throw e;
        }
    }
}


/**
 * Managing Key.<br>
 * Inner Class.<br>
 */
class CoreFileBaseKeyMap {

    // Create a data directory(Base directory)
    private String[] baseFileDirs = null;

    // Data File Name
    private String[] fileDirs = null;

    // The Maximum Length Key
    private int keyDataLength = 129;

    // The Maximun Length Value
    private int oneDataLength = 16;

    // The total length of the Key and Value
    private int lineDataSize =  keyDataLength + oneDataLength;

    // The length of the data read from the file stream at a time(In bytes)
    private int getDataSize = lineDataSize * (8192 / lineDataSize);

    // The number of data files created
    private int numberOfDataFiles = 1024 * 10;
    //private int numberOfDataFiles = 512;


    // データファイルを格納するディレクトリ分散係数
    //private int dataDirsFactor = 10;
    private int dataDirsFactor = 20;

    // Fileオブジェクト格納用
    private File[] dataFileList = new File[numberOfDataFiles];

    // アクセススピード向上の為に、Openしたファイルストリームを一定数キャッシュする
    private InnerCache innerCache = null;

    // Total Size
    protected AtomicInteger totalSize = new AtomicInteger(0);



    public CoreFileBaseKeyMap(String[] dirs, int innerCacheSize) {
        this.baseFileDirs = dirs;
        innerCache = new InnerCache(innerCacheSize);
        try {
            fileDirs = new String[baseFileDirs.length * dataDirsFactor];
            int counter = 0;
            for (int idx = 0; idx < baseFileDirs.length; idx++) {
                for (int idx2 = 0; idx2 < dataDirsFactor; idx2++) {

                    fileDirs[counter] = baseFileDirs[idx] + idx2 + "/";
                    File dir = new File(fileDirs[counter]);
                    if (!dir.exists()) dir.mkdirs();
                    counter++;
                }
            }

            for (int i = 0; i < numberOfDataFiles; i++) {

                File file = new File(fileDirs[i % fileDirs.length] + i + ".data");

                file.delete();
                dataFileList[i] = file;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * put Method.<br>
     * 
     * @param key
     * @param value
     * @param hashCode This is a key value hash code
     */
    public void put(String key, String value, int hashCode) {
        try {

            File file = dataFileList[hashCode % numberOfDataFiles];

            StringBuffer buf = new StringBuffer(this.fillCharacter(key, keyDataLength));
            buf.append(this.fillCharacter(value, oneDataLength));

            synchronized (innerCache.syncObj) {

                CacheContainer accessor = (CacheContainer)innerCache.get(file.getAbsolutePath());
                RandomAccessFile raf = null;
                BufferedWriter wr = null;

                if (accessor == null || accessor.isClosed == true) {

                    raf = new RandomAccessFile(file, "rwd");
                    wr = new BufferedWriter(new FileWriter(file, true));
                    accessor = new CacheContainer();
                    accessor.raf = raf;
                    accessor.wr = wr;
                    accessor.file = file;
                    innerCache.put(file.getAbsolutePath(), accessor);
                } else {

                    raf = accessor.raf;
                    wr = accessor.wr;
                }

                long dataLineNo = this.getLinePoint(key, raf);

                if (dataLineNo == -1) {

                    wr.write(buf.toString());
                    wr.flush();

                    // The size of an increment
                    this.totalSize.getAndIncrement();
                } else {

                    // 過去に存在したデータなら1増分
                    if (this.get(key, hashCode) == null) this.totalSize.getAndIncrement();

                    raf.seek(dataLineNo * (lineDataSize));
                    raf.write(buf.toString().getBytes(), 0, lineDataSize);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    // 指定のキー値が指定のファイル内でどこにあるかを調べる
    private long getLinePoint(String key, RandomAccessFile raf) {

        long line = -1;
        long lineCount = 0L;

        byte[] keyBytes = key.getBytes();
        byte[] equalKeyBytes = new byte[keyBytes.length + 1];
        byte[] lineBufs = new byte[this.getDataSize];
        boolean matchFlg = true;

        // マッチング用配列作成
        for (int idx = 0; idx < keyBytes.length; idx++) {
            equalKeyBytes[idx] = keyBytes[idx];
        }

        equalKeyBytes[equalKeyBytes.length - 1] = 38;

        try {

            raf.seek(0);
            int readLen = -1;
            while((readLen = raf.read(lineBufs)) != -1) {

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

                    // マッチした場合のみ返す
                    if (matchFlg) {
                        line = lineCount;
                        break;
                    }
                    lineCount++;
                }
                if (matchFlg) break;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return line;
    }


    /**
     * 指定のキー値でvalueを取得する.<br>
     *
     * @param key 
     * @param hashCode This is a key value hash code
     */
    public String get(String key, int hashCode) {
        byte[] tmpBytes = null;

        String ret = null;
        byte[] keyBytes = key.getBytes();
        byte[] equalKeyBytes = new byte[keyBytes.length + 1];
        byte[] lineBufs = new byte[this.getDataSize];
        boolean matchFlg = true;

        // マッチング用配列作成
        for (int idx = 0; idx < keyBytes.length; idx++) {
            equalKeyBytes[idx] = keyBytes[idx];
        }

        equalKeyBytes[equalKeyBytes.length - 1] = 38;

        try {

            File file = dataFileList[hashCode % numberOfDataFiles];

            synchronized (innerCache.syncObj) {
                CacheContainer accessor = (CacheContainer)innerCache.get(file.getAbsolutePath());
                RandomAccessFile raf = null;
                BufferedWriter wr = null;

                if (accessor == null || accessor.isClosed) {

                    raf = new RandomAccessFile(file, "rwd");
                    wr = new BufferedWriter(new FileWriter(file, true));
                    accessor = new CacheContainer();
                    accessor.raf = raf;
                    accessor.wr = wr;
                    accessor.file = file;
                    innerCache.put(file.getAbsolutePath(), accessor);
                } else {

                    raf = accessor.raf;
                }

                raf.seek(0);
                int readLen = -1;
                while((readLen = raf.read(lineBufs)) != -1) {

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

                        // マッチした場合のみ配列化
                        if (matchFlg) {

                            tmpBytes = new byte[lineDataSize];

                            for (int i = 0; i < lineDataSize; i++) {

                                tmpBytes[i] = lineBufs[assist + i];
                            }
                            break;
                        }
                    }
                    if (matchFlg) break;
                }
            }

            if (tmpBytes != null) {

                if (tmpBytes[keyDataLength] != 38) {

                    int i = keyDataLength;
                    int counter = 0;

                    for (; i < tmpBytes.length; i++) {

                        if (tmpBytes[i] == 38) break;
                        counter++;
                    }

                    ret = new String(tmpBytes, keyDataLength, counter, "UTF-8");
                }
            }
        } catch (Exception e) {

            e.printStackTrace();
        }

        return ret;
    }


    /**
     * remove
     *
     * @param key 
     * @param hashCode
     */
    public String remove(String key, int hashCode) {
        String ret = null;
        synchronized (innerCache.syncObj) {

            ret = this.get(key, hashCode);
            if(ret != null) {

                this.put(key, "&&&&&&&&&&&&&&&&", hashCode);

                // The size of an decrement
                this.totalSize.getAndDecrement();
            }
        }
        return ret;
    }



    /**
     * 指定の文字を指定の桁数で特定文字列で埋める.<br>
     *
     * @param data
     * @param fixSize
     */
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
        return innerCache.getSize();
    }


    protected static int createHashCode(String key) {
        
        int hashCode = new String(DigestUtils.sha(key.getBytes())).hashCode();

        if (hashCode < 0) {
            hashCode = hashCode - hashCode - hashCode;
        }

        return hashCode;
    } 
}


/**
 * ファイルアクセッサー周りのキャッシュ用コンテナ.<br>
 */
class CacheContainer {
    public RandomAccessFile raf = null;
    public BufferedWriter wr = null;
    public File file = null;
    public boolean isClosed = false;
}


/**
 * ファイルアクセッサーのキャッシュ群.<br>
 */
class InnerCache extends LinkedHashMap {

    private boolean fileWrite = false;

    private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
    private final Lock r = rwl.readLock();
    private final Lock w = rwl.writeLock();

    private int maxCacheSize = -1;

    public Object syncObj = new Object();

    // コンストラクタ
    public InnerCache() {
        super(512, 0.75f, true);
        maxCacheSize = 512;
    }


    // コンストラクタ
    public InnerCache(int maxCacheCapacity) {
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
        boolean ret = false;
        if (size() > maxCacheSize) {
            CacheContainer accessor= (CacheContainer)eldest.getValue();
            try {
                if (accessor != null) {

                    synchronized (syncObj) {

                        if (accessor.raf != null) {
                            accessor.raf.close();
                            accessor.raf = null;
                        }

                        if (accessor.wr != null) {
                            accessor.wr.close();
                            accessor.wr = null;
                        }
                        accessor.isClosed = true;
                        eldest.setValue(accessor);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            ret = true;
        }
        return ret;
    }


    public int getSize() {
        return size();
    }
}
