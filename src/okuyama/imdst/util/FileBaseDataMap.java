package okuyama.imdst.util;

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
 * TODO:Iterator部分の実装がまだ。164行目から!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class FileBaseDataMap extends AbstractMap {

    CoreFileBaseKeyMap[] coreFileBaseKeyMaps = null;

    String[] dirs = null;

    int numberOfCoreMap = 0;

    // Using a single cache 25 KB per
    int innerCacheSizeTotal = 1024 * 4;


    /**
     * コンストラクタ.<br>
     *
     * @param baseDirs
     * @param numberOfKeyData
     * @return 
     * @throws
     */
    public FileBaseDataMap(String[] baseDirs, int numberOfKeyData) {
        this.dirs = baseDirs;
        this.numberOfCoreMap = baseDirs.length;
        this.coreFileBaseKeyMaps = new CoreFileBaseKeyMap[baseDirs.length];
        int oneCacheSizePer = innerCacheSizeTotal / numberOfCoreMap;
        int oneMapSizePer = numberOfKeyData / numberOfCoreMap;

        for (int idx = 0; idx < baseDirs.length; idx++) {
            String[] dir = {baseDirs[idx]};
            this.coreFileBaseKeyMaps[idx] = new CoreFileBaseKeyMap(dir, oneCacheSizePer, oneMapSizePer);
        }
    }


    /**
     * put.<br>
     * 
     * @param key
     * @param value
     */
    public Object put(Object key, Object value) {
        int hashCode = CoreFileBaseKeyMap.createHashCode((String)key);

        this.coreFileBaseKeyMaps[hashCode % this.numberOfCoreMap].put((String)key, ((Long)value).toString(), hashCode);
        return null;
    }


    /**
     * get.<br>
     * 
     * @param key
     */
    public Object get(Object key) {
        int hashCode = CoreFileBaseKeyMap.createHashCode((String)key);

        return this.coreFileBaseKeyMaps[hashCode % this.numberOfCoreMap].get((String)key, hashCode);
    }


    /**
     * remove.<br>
     * 
     * @param key
     */
    public Object remove(Object key) {
        int hashCode = CoreFileBaseKeyMap.createHashCode((String)key);

        return this.coreFileBaseKeyMaps[hashCode % this.numberOfCoreMap].remove((String)key, hashCode);
    }


    /**
     * containsKey.<br>
     *
     * @param key 
     */
    public boolean containsKey(Object key) {
        boolean ret = true;
        int hashCode = CoreFileBaseKeyMap.createHashCode((String)key);

        if (this.coreFileBaseKeyMaps[hashCode % this.numberOfCoreMap].get((String)key, hashCode) == null) {
            ret = false;
        }
        return ret;
    }


    /**
     * size.<br>
     *
     * @param
     * @return 
     * @throws
     */
    public int size() {
        int ret = 0;

        for (int idx = 0; idx < this.coreFileBaseKeyMaps.length; idx++) {
            ret = ret + this.coreFileBaseKeyMaps[idx].totalSize.intValue();
        }
        return ret;
    }


    /**
     * clear.<br>
     *
     * @param
     * @return 
     * @throws
     */
    public void clear() {
        for (int i = 0; i < this.coreFileBaseKeyMaps.length; i++) {
            this.coreFileBaseKeyMaps[i].clear();
            this.coreFileBaseKeyMaps[i].init();
        }
    }


    /**
     * entrySet.<br>
     *
     * @param
     * @return 
     * @throws
     */
    public Set entrySet() {
        Set ret = new FileBaseDataMapSet(this);
        return ret;
    }


    /**
     * イテレータを初期化.<br>
     * スレッドセーフではない.<br>
     *
     * @param
     * @return 
     * @throws
     */
    public void setIteratorInit() {
    
    }


    /**
     * イテレータの次の値の存在確認.<br>
     * スレッドセーフではない.<br>
     *
     * @param
     * @return boolean 
     * @throws
     */
    public boolean hasIteratorNext() {
        boolean ret = true;
        return ret;
    }


    /**
     *
     *
     * @param
     * @return 
     * @throws
     */
    public Object nextIteratorKey() {
        Object ret = null;
        return ret;
    }
}



/**
 * Managing Key.<br>
 * Inner Class.<br>
 *
 *
 */
class CoreFileBaseKeyMap {

    // Create a data directory(Base directory)
    private String[] baseFileDirs = null;

    // Data File Name
    private String[] fileDirs = null;

    // The Maximum Length Key
    private int keyDataLength = ImdstDefine.saveKeyMaxSize;

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
    private File[] dataFileList = null;

    // アクセススピード向上の為に、Openしたファイルストリームを一定数キャッシュする
    private InnerCache innerCache = null;

    // Total Size
    protected AtomicInteger totalSize = null;

    private int innerCacheSize = 128;

    /**
     * コンストラクタ.<br>
     *
     * @param dirs
     * @param innerCacheSize
     * @param numberOfKeyData
     * @return 
     * @throws
     */
    public CoreFileBaseKeyMap(String[] dirs, int innerCacheSize, int numberOfKeyData) {
        try {
            this.baseFileDirs = dirs;
            this.innerCacheSize = innerCacheSize;
            this.numberOfDataFiles = numberOfKeyData / 100;

            this.init();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * 初期化
     *
     * @param dirs
     * @param innerCacheSize
     * @param numberOfKeyData
     * @return 
     * @throws
     */
    public void init() {

        this.innerCache = new InnerCache(this.innerCacheSize);
        this.totalSize = new AtomicInteger(0);
        this.dataFileList = new File[numberOfDataFiles];

        try {
            this.fileDirs = new String[this.baseFileDirs.length * this.dataDirsFactor];
            int counter = 0;
            for (int idx = 0; idx < this.baseFileDirs.length; idx++) {
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
     * clearメソッド.<br>
     *
     * @param
     * @return 
     * @throws
     */
    public void clear() {
        if (this.innerCache != null) this.innerCache.clear();
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
     * @return 
     * @throws
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


class FileBaseDataMapEntry implements Map.Entry {

    private Object key = null;
    private Object value = null;
    private FileBaseDataMap fileBaseDataMap = null;

    // コンストラクタ
    public FileBaseDataMapEntry(Object key, Object value, FileBaseDataMap fileBaseDataMap) {
        this.key = key;
        this.value = value;
        this.fileBaseDataMap = fileBaseDataMap;
    }


    /**
     * equals<br>
     *
     */
    public boolean equals(Object o) {
        return this.key.equals(o);
    }


    /**
     * getKey<br>
     *
     */
    public Object getKey() {
        return this.key;
    }


    /**
     * getValue<br>
     *
     */
    public Object getValue() {
        return this.value;
    }

    /**
     * hashCode<br>
     *
     */
    public int hashCode() {
        return this.key.hashCode();
    }

    /**
     * setValue<br>
     *
     */
    public Object setValue(Object o) {
        return this.fileBaseDataMap.put(this.key, o);
    }
}


class FileBaseDataMapSet extends AbstractSet implements Set {

    private FileBaseDataMap fileBaseDataMap = null;

    // コンストラクタ
    public FileBaseDataMapSet(FileBaseDataMap fileBaseDataMap) {
        this.fileBaseDataMap = fileBaseDataMap;
    }


    public int size() {
        return this.fileBaseDataMap.size();
    }


    public Iterator iterator() {
        return new FileBaseDataMapIterator(this.fileBaseDataMap);
    }
}


class FileBaseDataMapIterator implements Iterator {

    private FileBaseDataMap fileBaseDataMap = null;

    private Object nowPositionKey = null;


    // コンストラクタ
    public FileBaseDataMapIterator(FileBaseDataMap fileBaseDataMap) {
        this.fileBaseDataMap = fileBaseDataMap;
        this.fileBaseDataMap.setIteratorInit();
    }


    /**
     * hasNext<br>
     *
     */
    public boolean hasNext() {
        return this.fileBaseDataMap.hasIteratorNext();
    }


    /**
     * next<br>
     *
     */
    public Map.Entry next() {

        Object key = null;
        Object value = null;

        while((key = this.fileBaseDataMap.nextIteratorKey()) == null) {};
        this.nowPositionKey = key;
        value = this.fileBaseDataMap.get(key);

        Map.Entry fileBaseDataMapEntry = new FileBaseDataMapEntry(key, value, this.fileBaseDataMap);
        return fileBaseDataMapEntry;
    }


    /**
     * remove<br>
     *
     */
    public void remove() {
        this.fileBaseDataMap.remove(this.nowPositionKey);
    }
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

            Set workEntrySet = this.entrySet();
            Iterator workEntryIte = workEntrySet.iterator();
            String workKey = null;

            while(workEntryIte.hasNext()) {
                Map.Entry obj = (Map.Entry)workEntryIte.next();

                CacheContainer accessor= (CacheContainer)obj.getValue();
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
                            obj.setValue(accessor);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
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
