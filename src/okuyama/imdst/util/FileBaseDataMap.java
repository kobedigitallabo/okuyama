package okuyama.imdst.util;

import java.io.*;
import java.util.concurrent.locks.*;
import java.util.*;

import org.apache.commons.codec.digest.DigestUtils;

/**
 * Fileベースでデータを管理する.<br>
 * ディスクベースなのでメモリをあまり利用しないので大量のデータを管理できる.<br>
 *
 */
public class FileBaseDataMap {

    // データファイル作成ディレクトリ(ベース)
    private String[] baseFileDirs = null;

    // データファイル作成ディレクトリ
    private String[] fileDirs = null;

    // Key値の最大長
    private int keyDataLength = 129;

    // Valueの最大長
    private int oneDataLength = 16;

    // Key + Value の長さ
    private int lineDataSize =  keyDataLength + oneDataLength;

    // 一度に取得するデータサイズ(byteサイズ)
    private int getDataSize = lineDataSize * 56;

    // データファイルの数
    //private int numberOfDataFiles = 1024 * 10;
    private int numberOfDataFiles = 256;


    // データファイルを格納するディレクトリ分散係数
    private int dataDirsFactor = 2;
    //private int dataDirsFactor = 20;

    // Fileオブジェクト格納用
    private File[] dataFileList = new File[numberOfDataFiles];

    // アクセススピード向上の為に、Openしたファイルストリームを一定数キャッシュする
    //private InnerCache innerCache = new InnerCache(1024);
    private InnerCache innerCache = new InnerCache(256);


    public FileBaseDataMap(String[] dirs) {
        this.baseFileDirs = dirs;
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

                dataFileList[i] = file;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * putメソッド.<br>
     * 
     * @param key
     * @param value
     */
    public void put(String key, String value) {
        try {

            // Create HashCode 
            int index = createHashCode(key);

            File file = dataFileList[index % numberOfDataFiles];

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
                } else {

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
     */
    public String get(String key) {
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

            // Create HashCode
            int index = createHashCode(key);

            File file = dataFileList[index % numberOfDataFiles];

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
     *
     *
     */
    public String remove(String key) {
        String ret = this.get(key);
        if(ret != null) this.put(key, "&&&&&&&&&&&&&&&&");
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


    public int createHashCode(String key) {
        
        int index = new String(DigestUtils.sha(key.getBytes())).hashCode();

        if (index < 0) {
            index = index - index - index;
        }

        return index;
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
