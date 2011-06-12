package okuyama.imdst.util;

import java.io.*;
import java.util.concurrent.locks.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.codec.digest.DigestUtils;
import com.sun.mail.util.BASE64DecoderStream;
;

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
public class FileBaseDataMap extends AbstractMap {

    private CoreFileBaseKeyMap[] coreFileBaseKeyMaps = null;
    private CoreFileBaseKeyMap coreFileBaseKeyMap4BigData = null;
    private CoreFileBaseKeyMap coreFileBaseKeyMap4MiddleData = null;
    private int coreMapType = 0; //0:Map1つのみ、1:Map2つ、2:Map3つ
    private int regularSizeLimit = 0;
    private int middleSize = 0;

    private String[] dirs = null;

    private int numberOfCoreMap = 0;

    // Using a single cache 25 KB per
    private int innerCacheSizeTotal = 1024;

    // Sync Object
    private Object syncObj = null;

    private int iteratorIndex = 0;

    private List iteratorNowDataList = null;
    private int iteratorNowDataListIdx = 0;

    protected static int paddingSymbol = 38;
    protected static byte[] paddingSymbolSet = {38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,
                                                38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,
                                                38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,
                                                38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,
                                                38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,
                                                38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,
                                                38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,
                                                38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,
                                                38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,
                                                38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,
                                                38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,
                                                38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,
                                                38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,
                                                38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,
                                                38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,
                                                38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,
                                                38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,
                                                38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,
                                                38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,
                                                38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,
                                                38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,
                                                38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,
                                                38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,
                                                38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,
                                                38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,
                                                38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,
                                                38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,
                                                38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,38,
                                                38,38,38,38,38,38,38,38};

    protected static String paddingSymbolSetString = new String(paddingSymbolSet);

    protected static ByteArrayOutputStream fillStream = null;



    /**
     * コンストラクタ.<br>
     *
     * @param baseDirs
     * @param numberOfKeyData
     * @return 
     * @throws
     */
    public FileBaseDataMap(String[] baseDirs, int numberOfKeyData) {
        this(baseDirs, numberOfKeyData, 0.40);
    }


    /**
     * コンストラクタ.<br>
     *
     * @param baseDirs
     * @param numberOfKeyData
     * @param cacheMemPercent
     * @return 
     * @throws
     */
    public FileBaseDataMap(String[] baseDirs, int numberOfKeyData, double cacheMemPercent) {
        this(baseDirs, numberOfKeyData, cacheMemPercent, 0);
    }

    /**
     * コンストラクタ.<br>
     *
     * @param baseDirs
     * @param numberOfKeyData
     * @param cacheMemPercent
     * @param numberOfValueLength
     * @return 
     * @throws
     */
    public FileBaseDataMap(String[] baseDirs, int numberOfKeyData, double cacheMemPercent, int numberOfValueLength) {
        this(baseDirs, numberOfKeyData, cacheMemPercent, numberOfValueLength, 1024, 1024*101);
    }

    /**
     * コンストラクタ.<br>
     *
     * @param baseDirs
     * @param numberOfKeyData
     * @param cacheMemPercent
     * @param numberOfValueLength
     * @return 
     * @throws
     */
    public FileBaseDataMap(String[] baseDirs, int numberOfKeyData, double cacheMemPercent, int numberOfValueLength, int regularSizeLimit, int middleSizeLimit) {
        this.regularSizeLimit = regularSizeLimit;
        this.dirs = baseDirs;
        this.numberOfCoreMap = baseDirs.length;
        this.syncObj = new Object();

        // 最大メモリから指定値をキャッシュに割り当てる
        long maxMem = JavaSystemApi.getRuntimeMaxMem("K");
        // メモリの上限値をKBで取得してそれの指定した割合から割り出す
        long cacheMem = new Double(maxMem * cacheMemPercent).longValue();
        // 指定メモリ量に幾つのキャッシュが乗るか調べる(1キャッシュ当たりのサイズ(可変)(単位:KB))
        this.innerCacheSizeTotal = new Long(cacheMem).intValue() / 128;

        int oneCacheSizePer = innerCacheSizeTotal / numberOfCoreMap;
        int oneMapSizePer = numberOfKeyData / numberOfCoreMap;



        if (numberOfValueLength > 0) {

            if (numberOfValueLength > middleSizeLimit) {

                coreMapType = 2;
                // 最大サイズ用
                String[] bigDataDir = {baseDirs[0]+"/virtualbigdata1/", baseDirs[0]+"/virtualbigdata2/"};
                coreFileBaseKeyMap4BigData = new FixWriteCoreFileBaseKeyMap(bigDataDir, oneCacheSizePer / 3, oneMapSizePer / 3, numberOfValueLength);

                // ミドルサイズ用
                middleSize = middleSizeLimit;

                String[] middleDataDir = {baseDirs[0]+"/virtualmiddledata1/", baseDirs[0]+"/virtualmiddledata2/"};
                coreFileBaseKeyMap4MiddleData = new FixWriteCoreFileBaseKeyMap(middleDataDir, oneCacheSizePer / 3, oneMapSizePer / 3, middleSize);
            } else if (numberOfValueLength > regularSizeLimit){

                coreMapType = 1;
                // 最大サイズ用
                String[] bigDataDir = {baseDirs[0]+"/virtualbigdata1/", baseDirs[0]+"/virtualbigdata2/"};
                coreFileBaseKeyMap4BigData = new FixWriteCoreFileBaseKeyMap(bigDataDir, oneCacheSizePer / 2, oneMapSizePer / 2, numberOfValueLength);
            }
            this.coreFileBaseKeyMaps = new FixWriteCoreFileBaseKeyMap[baseDirs.length];
        } else {

            this.coreFileBaseKeyMaps = new DelayWriteCoreFileBaseKeyMap[baseDirs.length];
        }

        for (int idx = 0; idx < baseDirs.length; idx++) {

            String[] dir = {baseDirs[idx]};

            if (numberOfValueLength > 0) {

                if (numberOfValueLength > regularSizeLimit) {
                    this.coreFileBaseKeyMaps[idx] = new FixWriteCoreFileBaseKeyMap(dir, oneCacheSizePer, oneMapSizePer, regularSizeLimit);
                } else {
                    this.coreFileBaseKeyMaps[idx] = new FixWriteCoreFileBaseKeyMap(dir, oneCacheSizePer, oneMapSizePer, numberOfValueLength);
                }

                fillStream = new ByteArrayOutputStream(4096);
                for (int i = 0; i < 8; i++) {
                    fillStream.write(FileBaseDataMap.paddingSymbolSet, 0 ,512);
                }
            } else {

                this.coreFileBaseKeyMaps[idx] = new DelayWriteCoreFileBaseKeyMap(dir, oneCacheSizePer, oneMapSizePer);
            }
        }
    }


    /**
     * put.<br>
     * 
     * @param key
     * @param value
     */
    public Object put(Object key, Object value) {

        int hashCode = createHashCode((String)key);

        synchronized (this.syncObj) { 

            if (coreMapType != 0) {
                String valueStr = (String)value;
                byte[] valueStrBytes = valueStr.getBytes();

                if (coreMapType == 1) {
                    if (valueStrBytes.length > regularSizeLimit) {

                        this.coreFileBaseKeyMap4BigData.put((String)key, valueStr, hashCode);
                        // データが存在するマーカー"*"を設定
                        this.coreFileBaseKeyMaps[hashCode % this.numberOfCoreMap].put((String)key, "*", hashCode);
                    } else {

                        this.coreFileBaseKeyMaps[hashCode % this.numberOfCoreMap].put((String)key, valueStr, hashCode);
                        //this.coreFileBaseKeyMap4BigData.remove((String)key, valueStr, hashCode);
                    }
                } else if (coreMapType == 2) {
                    if (valueStrBytes.length > middleSize) {

                        this.coreFileBaseKeyMap4BigData.put((String)key, valueStr, hashCode);

                        // 設定前に古いデータがあるかもしれないので消す
                        this.coreFileBaseKeyMap4MiddleData.remove((String)key, hashCode);
                        // データが存在するマーカー"*"を設定
                        this.coreFileBaseKeyMaps[hashCode % this.numberOfCoreMap].put((String)key, "*", hashCode);
                    } else if (valueStrBytes.length > regularSizeLimit) {

                        this.coreFileBaseKeyMap4MiddleData.put((String)key, valueStr, hashCode);

                        // 設定前に古いデータがあるかもしれないので消す
                        this.coreFileBaseKeyMap4BigData.remove((String)key, hashCode);
                        // データが存在するマーカー"**"を設定
                        this.coreFileBaseKeyMaps[hashCode % this.numberOfCoreMap].put((String)key, "**", hashCode);
                    } else {

                        this.coreFileBaseKeyMaps[hashCode % this.numberOfCoreMap].put((String)key, valueStr, hashCode);
                        //this.coreFileBaseKeyMap4BigData.remove((String)key, valueStr, hashCode);
                    }
                }
            } else {

                this.coreFileBaseKeyMaps[hashCode % this.numberOfCoreMap].put((String)key, (String)value, hashCode);
            }
        }
        return null;
    }


    /**
     * get.<br>
     * 
     * @param key
     */
    public Object get(Object key) {

        Object ret = null;
        int hashCode = createHashCode((String)key);

        synchronized (this.syncObj) { 
            ret = this.coreFileBaseKeyMaps[hashCode % this.numberOfCoreMap].get((String)key, hashCode);
            if (coreMapType != 0) {
                if (ret != null && ret.equals("*")) {
                    ret = this.coreFileBaseKeyMap4BigData.get((String)key, hashCode);
                } else if (ret != null && ret.equals("**")){
                    ret = this.coreFileBaseKeyMap4MiddleData.get((String)key, hashCode);
                }
            }
        }
        return ret;
    }


    /**
     * remove.<br>
     * 
     * @param key
     */
    public Object remove(Object key) {
        Object ret = null;
        int hashCode = createHashCode((String)key);

        synchronized (this.syncObj) { 
            ret = this.coreFileBaseKeyMaps[hashCode % this.numberOfCoreMap].remove((String)key, hashCode);
            if (coreMapType != 0 && ret != null && ret.equals("*")) {
                ret = this.coreFileBaseKeyMap4BigData.remove((String)key, hashCode);
            } else if (coreMapType != 0 && ret != null && ret.equals("**")) {
                ret = this.coreFileBaseKeyMap4MiddleData.remove((String)key, hashCode);
            }
        }
        return ret;
    }


    /**
     * containsKey.<br>
     *
     * @param key 
     */
    public boolean containsKey(Object key) {
        boolean ret = true;
        int hashCode = createHashCode((String)key);

        synchronized (this.syncObj) { 
            if (this.coreFileBaseKeyMaps[hashCode % this.numberOfCoreMap].get((String)key, hashCode) == null) {
                ret = false;
            }
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
            ret = ret + this.coreFileBaseKeyMaps[idx].getTotalSize().intValue();
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

            synchronized (this.syncObj) { 
                this.coreFileBaseKeyMaps[i].clear();
                this.coreFileBaseKeyMaps[i].init();
                if (coreMapType > 0) {
                    this.coreFileBaseKeyMap4BigData.clear();
                    this.coreFileBaseKeyMap4BigData.init();
                } 

                if (coreMapType > 1) {
                    this.coreFileBaseKeyMap4MiddleData.clear();
                    this.coreFileBaseKeyMap4MiddleData.init();
                } 
            }
        }
    }


    /**
     * finishClear.<br>
     *
     * @param
     * @return 
     * @throws
     */
    public void finishClear() {
        for (int i = 0; i < this.coreFileBaseKeyMaps.length; i++) {

            synchronized (this.syncObj) { 
                this.coreFileBaseKeyMaps[i].clear();
                if (coreMapType > 0) {
                    this.coreFileBaseKeyMap4BigData.clear();
                }
                if (coreMapType > 1) {
                    this.coreFileBaseKeyMap4MiddleData.clear();
                }
            }
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
    public void iteratorInit() {

        this.iteratorIndex = 0;
        this.iteratorNowDataList = new ArrayList();
        this.iteratorNowDataListIdx = 0;

        for (int i = 0; i < this.coreFileBaseKeyMaps.length; i++) {

            this.coreFileBaseKeyMaps[i].startKeyIteration();
        }

        if (this.iteratorIndex < this.coreFileBaseKeyMaps.length) {
            while (true) {
                this.iteratorNowDataList = this.coreFileBaseKeyMaps[this.iteratorIndex].getAllOneFileInKeys();
                if(this.iteratorNowDataList == null) break;
                if(this.iteratorNowDataList.size() > 0) break;
            }
        }
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
        if(this.iteratorNowDataList == null) return false;
        if(this.iteratorNowDataList.size() > this.iteratorNowDataListIdx) return true;
        return false;
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

        ret = (String)this.iteratorNowDataList.get(this.iteratorNowDataListIdx);
        this.iteratorNowDataListIdx++;

        if (this.iteratorNowDataList.size() == this.iteratorNowDataListIdx) {

            if (this.iteratorIndex < this.coreFileBaseKeyMaps.length) {

                while (true) {

                    this.iteratorNowDataList = this.coreFileBaseKeyMaps[this.iteratorIndex].getAllOneFileInKeys();
                    this.iteratorNowDataListIdx = 0;

                    if (this.iteratorNowDataList == null && this.iteratorIndex < this.coreFileBaseKeyMaps.length) {

                        this.iteratorIndex++;
                        if (this.iteratorIndex == this.coreFileBaseKeyMaps.length) break;
                        continue;
                    }

                    if (this.iteratorNowDataList == null) {

                        this.iteratorNowDataList = null;
                        this.iteratorNowDataListIdx = 0;
                        this.iteratorIndex++;
                    }

                    if (this.iteratorNowDataList.size() > 0) {

                        this.iteratorNowDataListIdx = 0;
                        break;
                    }
                }
            } else {

                this.iteratorNowDataList = null;
                this.iteratorNowDataListIdx = 0;
            }
        }

        return ret;
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
 * Interface Of CoreFileBaseKeyMap
 *
 */
interface CoreFileBaseKeyMap {

    /**
     * put.
     *
     * @param key
     * @param value
     * @param hashCode
     */
    public void put(String key, String value, int hashCode);


    /**
     * get.
     *
     * @param key
     * @param hashCode
     * @return value
     */
    public String get(String key, int hashCode);


    /**
     * remove.
     *
     * @param key
     * @param hashCode
     * @return value
     */
    public String remove(String key, int hashCode);


    /**
     * getTotalSize.
     *
     * @return size
     */
    public AtomicInteger getTotalSize();


    /**
     * clear.
     *
     */
    public void clear();


    /**
     * init.
     *
     */
    public void init();


    /**
     * startKeyIteration.
     *
     */
    public void startKeyIteration();


    /**
     * getAllOneFileInKeys.
     *
     */
    public List getAllOneFileInKeys();


}



/**
 * Managing Key.<br>
 * Inner Class.<br>
 * DelayMode.<br>
 *
 *
 */
class DelayWriteCoreFileBaseKeyMap extends Thread implements CoreFileBaseKeyMap {

    // Create a data directory(Base directory)
    private String[] baseFileDirs = null;

    // Data File Name
    private String[] fileDirs = null;

    // The Maximum Length Key
    private int keyDataLength = new Double(ImdstDefine.saveKeyMaxSize * 1.33).intValue() + 1;

    // The Maximun Length Value
    private int oneDataLength = 11;

    // The total length of the Key and Value
    private int lineDataSize =  keyDataLength + oneDataLength;

    // The length of the data read from the file stream at a time(In bytes)
    private int getDataSize = (4096 / lineDataSize) * lineDataSize;

    // The number of data files created
    private int numberOfDataFiles = 1024;

    // データファイルを格納するディレクトリ分散係数
    private int dataDirsFactor = 20;

    // Fileオブジェクト格納用
    private File[] dataFileList = null;

    // アクセススピード向上の為に、Openしたファイルストリームを一定数キャッシュする
    private InnerCache innerCache = null;

    // Total Size
    private AtomicInteger totalSize = null;

    private int innerCacheSize = 128;

    // 1ファイルに対してどの程度のキー数を保存するかの目安
    private int numberOfOneFileKey = 7000;

    // 全キー取得時の現在ファイルのインデックス
    private int nowIterationFileIndex = 0;

    // 全キー取得時の現在のファイル内でのFPの位置
    private long nowIterationFpPosition = 0;

    // 遅延書き込み依頼用のQueue
    private ArrayBlockingQueue delayWriteQueue = new ArrayBlockingQueue(8000);

    // 遅延書き込み前のデータを補完するMap
    private ConcurrentHashMap delayWriteDifferenceMap = new ConcurrentHashMap(8000, 7900, 32);

    // 遅延書き込みを依頼した回数
    private long delayWriteRequestCount = 0L;

    // 遅延書き込みを実行した回数
    private long delayWriteExecCount = 0L;



    /**
     * コンストラクタ.<br>
     *
     * @param dirs
     * @param innerCacheSize
     * @param numberOfKeyData
     * @return 
     * @throws
     */
    public DelayWriteCoreFileBaseKeyMap(String[] dirs, int innerCacheSize, int numberOfKeyData) {
        try {
            this.baseFileDirs = dirs;
            this.innerCacheSize = innerCacheSize;
            if (numberOfKeyData <=  this.numberOfOneFileKey) numberOfKeyData =  this.numberOfOneFileKey * 2;
            this.numberOfDataFiles = numberOfKeyData / this.numberOfOneFileKey;

            this.init();
            this.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * コンストラクタ.<br>
     * Valueサイズ指定有り.<br>
     *
     * @param dirs
     * @param innerCacheSize
     * @param numberOfKeyData
     * @param numberOfValueSize
     * @return 
     * @throws
     */
    public DelayWriteCoreFileBaseKeyMap(String[] dirs, int innerCacheSize, int numberOfKeyData, int numberOfValueSize) {
        try {
            this.oneDataLength = numberOfValueSize;
            this.lineDataSize =  this.keyDataLength + this.oneDataLength;
            if (8192 > this.lineDataSize) {
                this.getDataSize = this.lineDataSize * (8192 / this.lineDataSize) * 5;
            } else {
                this.getDataSize = this.lineDataSize * 1 * 2;
            }

            this.baseFileDirs = dirs;
            this.innerCacheSize = innerCacheSize;
            if (numberOfKeyData <=  this.numberOfOneFileKey) numberOfKeyData =  this.numberOfOneFileKey * 2;
            this.numberOfDataFiles = numberOfKeyData / this.numberOfOneFileKey;

            this.init();
            this.start();
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

                // Keyファイルのディレクトリ範囲ないで適当に記録ファイルを分散させる
                File file = new File(this.fileDirs[i % this.fileDirs.length] + i + ".data");

                file.delete();
                dataFileList[i] = file;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    // 遅延書き込み用
    public void run() {
        while (true) {
            
            try {
                Object[] instructionObj = (Object[])this.delayWriteQueue.take();


                String key = (String)instructionObj[0];
                String value = (String)instructionObj[1];
                int hashCode = ((Integer)instructionObj[2]).intValue();
                StringBuilder buf = new StringBuilder(this.lineDataSize);
                CacheContainer accessor = null;
                RandomAccessFile raf = null;
                BufferedWriter wr = null;

                buf.append(this.fillCharacter(key, keyDataLength));
                buf.append(this.fillCharacter(value, oneDataLength));

                File file = this.dataFileList[hashCode % numberOfDataFiles];

                if (this.innerCache != null)
                    accessor = (CacheContainer)this.innerCache.get(file.getAbsolutePath());


                synchronized (this.dataFileList[hashCode % numberOfDataFiles]) {
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


                    // KeyData Write File
                    for (int tryIdx = 0; tryIdx < 2; tryIdx++) {
                        try {

                            // Key値の場所を特定する
                            long[] dataLineNoRet = this.getLinePoint(key, raf);

                            if (dataLineNoRet[0] == -1) {

                                wr.write(buf.toString());
                                SystemUtil.diskAccessSync(wr);

                                // The size of an increment
                                this.totalSize.getAndIncrement();
                            } else {

                                // 過去に存在したデータなら1増分
                                boolean increMentFlg = false;
                                if (dataLineNoRet[1] == -1) increMentFlg = true;
                                //if (this.get(key, hashCode) == null) increMentFlg = true;

                                raf.seek(dataLineNoRet[0] * (lineDataSize));

                                raf.write(buf.toString().getBytes(), 0, lineDataSize);

                                if (increMentFlg) this.totalSize.getAndIncrement();
                            }

                            break;
                        } catch (IOException ie) {

                            // IOExceptionの場合は1回のみファイルを再度開く
                            if (tryIdx == 1) throw ie;
                            try {

                                if (raf != null) raf.close();
                                if (wr != null) wr.close();

                                raf = new RandomAccessFile(file, "rwd");
                                wr = new BufferedWriter(new FileWriter(file, true));
                                accessor = new CacheContainer();
                                accessor.raf = raf;
                                accessor.wr = wr;
                                accessor.file = file;
                                innerCache.put(file.getAbsolutePath(), accessor);
                            } catch (Exception e) {
                                throw e;
                            }
                        }
                    }
                }

                // 削除処理の場合
                if (value.indexOf("&&&&&&&&&&&") == 0) {
                    // The size of an decrement
                    this.totalSize.getAndDecrement();
                }

                synchronized (this.delayWriteDifferenceMap) {

                    String removeChcek = (String)this.delayWriteDifferenceMap.get(key);
                    if (removeChcek != null && removeChcek.equals(value))
                        this.delayWriteDifferenceMap.remove(key);
                }
                this.delayWriteExecCount++;
            } catch (Exception e2) {
                e2.printStackTrace();
            }
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

        Object[] instructionObj = new Object[3];
        instructionObj[0] = key;
        instructionObj[1] = value;
        instructionObj[2] = new Integer(hashCode);
        try {
long start1 = 0L;
long start2 = 0L;

long end1 = 0L;
long end2 = 0L;

start1 = System.nanoTime();
            synchronized (this.delayWriteDifferenceMap) {
                this.delayWriteDifferenceMap.put(key, value);
            }
end1 = System.nanoTime();
start2 = System.nanoTime();
            this.delayWriteQueue.put(instructionObj);
end2 = System.nanoTime();
            this.delayWriteRequestCount++;

if (ImdstDefine.fileBaseMapTimeDebug) {
    System.out.println("Set 1="+(end1 - start1) + " 2="+(end2 - start2));
}
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    // 指定のキー値が指定のファイル内でどこにあるかを調べる
    private long[] getLinePoint(String key, RandomAccessFile raf) throws Exception {
        long[] ret = {-1, 0};
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

        equalKeyBytes[equalKeyBytes.length - 1] = new Integer(FileBaseDataMap.paddingSymbol).byteValue();

        try {

            raf.seek(0);
            int readLen = -1;
            while((readLen = SystemUtil.diskAccessSync(raf, lineBufs)) != -1) {

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
                        // 削除データか確かめる
                        if (lineBufs[assist + keyDataLength] == FileBaseDataMap.paddingSymbol) ret[1] = -1;
                        break;
                    }

                    lineCount++;
                }
                if (matchFlg) break;
            }

        } catch (IOException ie) {
            throw ie;
        } catch (Exception e) {
            throw e;
        }
        ret[0] = line;
        return ret;
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
long start1 = 0L;
long start2 = 0L;
long start3 = 0L;

long end1 = 0L;
long end2 = 0L;
long end3 = 0L;

start1 = System.nanoTime();

        if (this.delayWriteDifferenceMap.containsKey(key)) {
            String retStr = (String)this.delayWriteDifferenceMap.get(key);
            if (retStr ==null) return null;
            if (retStr.equals("&&&&&&&&&&&")) return null;
            return retStr;
        }

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

        equalKeyBytes[equalKeyBytes.length - 1] = new Integer(FileBaseDataMap.paddingSymbol).byteValue();

end1 = System.nanoTime();
        try {
start2 = System.nanoTime();
            File file = this.dataFileList[hashCode % numberOfDataFiles];
            CacheContainer accessor = null;
            RandomAccessFile raf = null;
            BufferedWriter wr = null;

            if (innerCache != null)
                accessor = (CacheContainer)this.innerCache.get(file.getAbsolutePath());

            synchronized (this.dataFileList[hashCode % numberOfDataFiles]) {
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
end2 = System.nanoTime();
start3 = System.nanoTime();
                for (int tryIdx = 0; tryIdx < 2; tryIdx++) {

                    try {

                        raf.seek(0);
                        int readLen = -1;
                        while((readLen = SystemUtil.diskAccessSync(raf, lineBufs)) != -1) {

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
                        break;
                    } catch (IOException ie) {

                        // IOExceptionの場合は1回のみファイルをサイド開く
                        if (tryIdx == 1) throw ie;

                        try {

                            if (raf != null) raf.close();
                            if (wr != null) wr.close();

                            raf = new RandomAccessFile(file, "rwd");
                            wr = new BufferedWriter(new FileWriter(file, true));
                            accessor = new CacheContainer();
                            accessor.raf = raf;
                            accessor.wr = wr;
                            accessor.file = file;
                            innerCache.put(file.getAbsolutePath(), accessor);
                        } catch (Exception e) {
                            throw e;
                        }
                    }
                }
            }

            // 取得データを文字列化
            if (tmpBytes != null) {

                if (tmpBytes[keyDataLength] != FileBaseDataMap.paddingSymbol) {

                    int i = keyDataLength;
                    int counter = 0;

                    for (; i < tmpBytes.length; i++) {

                        if (tmpBytes[i] == FileBaseDataMap.paddingSymbol) break;
                        counter++;
                    }

                    ret = new String(tmpBytes, keyDataLength, counter, "UTF-8");
                }
            }
end3 = System.nanoTime();

if (ImdstDefine.fileBaseMapTimeDebug) {
    System.out.println("Get 1="+(end1 - start1) + " 2="+(end2 - start2) + " 3="+(end3 - start3));
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

        ret = this.get(key, hashCode);
        if(ret != null) {

            this.put(key, "&&&&&&&&&&&", hashCode);
        }
        return ret;
    }


    /**
     * return of TotalSize
     *
     * @return size
     */
    public AtomicInteger getTotalSize() {
        return this.totalSize;
    }


    /**
     * 指定の文字を指定の桁数で特定文字列で埋める.<br>
     * 足りない文字列は固定の"&"で補う(38).<br>
     *
     * @param data
     * @param fixSize
     */
    private String fillCharacter(String data, int fixSize) {
        return SystemUtil.fillCharacter(data, fixSize, FileBaseDataMap.paddingSymbol);
    }

    public int getCacheSize() {
        return innerCache.getSize();
    }


    /**
     * キー値を連続して取得する準備を行う.<br>
     *
     * @param
     * @return 
     * @throws
     */
    public void startKeyIteration() {
        this.nowIterationFileIndex =  0;
        this.nowIterationFpPosition =  0L;

        long nowDelayRequestCount = this.delayWriteRequestCount;
        while(nowDelayRequestCount > this.delayWriteExecCount) {
            try {
                Thread.sleep(20);
            } catch(Exception e) {
            }
        }
    }


    /**
     * 自身が保持するキー値を返す.<br>
     * 最終的にキー値を全て返すとnullを返す.<br>
     *
     * @param
     * @return 
     * @throws
     */
    public List getAllOneFileInKeys() {

        List keys = null;
        byte[] datas = null;
        StringBuilder keysBuf = null;
        RandomAccessFile raf = null;

        try {
            if (this.nowIterationFileIndex < this.dataFileList.length) {

                keys = new ArrayList();
                datas = new byte[new Long(this.dataFileList[this.nowIterationFileIndex].length()).intValue()];

                raf = new RandomAccessFile(this.dataFileList[this.nowIterationFileIndex], "rwd");

                raf.seek(0);
                int readLen = -1;
                readLen = SystemUtil.diskAccessSync(raf, datas);

                if (readLen > 0) {

                    int loop = readLen / lineDataSize;

                    for (int loopIdx = 0; loopIdx < loop; loopIdx++) {

                        int assist = (lineDataSize * loopIdx);
                        keysBuf = new StringBuilder(ImdstDefine.stringBufferLarge_3Size);

                        int idx = 0;

                        while (true) {

                            if (datas[assist + idx] != FileBaseDataMap.paddingSymbol) {
                                keysBuf.append(new String(datas, assist + idx, 1));
                            } else {
                                break;
                            }
                            idx++;
                        }
                        keys.add(keysBuf.toString());
                        keysBuf = null;
                    }
                }
            }
            this.nowIterationFileIndex++;
        } catch(Exception e) {

            e.printStackTrace();
        } finally {

            try {
                if(raf != null) raf.close();

                raf = null;
                datas = null;
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
        return keys;
    }
}


/**
 * Managing Key.<br>
 * Inner Class.<br>
 *
 *
 */
class FixWriteCoreFileBaseKeyMap implements CoreFileBaseKeyMap{

    // Create a data directory(Base directory)
    private String[] baseFileDirs = null;

    // Data File Name
    private String[] fileDirs = null;

    // The Maximum Length Key
    private int keyDataLength = new Double(ImdstDefine.saveKeyMaxSize * 1.33).intValue() + 1;

    // The Maximun Length Value
    private int oneDataLength = 11;

    // The total length of the Key and Value
    private int lineDataSize =  keyDataLength + oneDataLength;

    // The length of the data read from the file stream at a time(In bytes)
    private int getDataSize = lineDataSize * (8192 / lineDataSize) * 5;

    // The number of data files created
    private int numberOfDataFiles = 1024;

    // データファイルを格納するディレクトリ分散係数
    private int dataDirsFactor = 40;

    // Fileオブジェクト格納用
    private File[] dataFileList = null;

    // アクセススピード向上の為に、Openしたファイルストリームを一定数キャッシュする
    private InnerCache innerCache = null;

    // Total Size
    private AtomicInteger totalSize = null;

    private int innerCacheSize = 128;

    // 1ファイルに対してどの程度のキー数を保存するかの目安
    private int numberOfOneFileKey = 7000;

    // 全キー取得時の現在ファイルのインデックス
    private int nowIterationFileIndex = 0;

    // 全キー取得時の現在のファイル内でのFPの位置
    private long nowIterationFpPosition = 0;



    /**
     * コンストラクタ.<br>
     *
     * @param dirs
     * @param innerCacheSize
     * @param numberOfKeyData
     * @return 
     * @throws
     */
    public FixWriteCoreFileBaseKeyMap(String[] dirs, int innerCacheSize, int numberOfKeyData) {
        try {
            this.baseFileDirs = dirs;
            this.innerCacheSize = innerCacheSize;
            if (numberOfKeyData <=  this.numberOfOneFileKey) numberOfKeyData =  this.numberOfOneFileKey * 2;
            this.numberOfDataFiles = numberOfKeyData / this.numberOfOneFileKey;

            this.init();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * コンストラクタ.<br>
     * Valueサイズ指定有り.<br>
     *
     * @param dirs
     * @param innerCacheSize
     * @param numberOfKeyData
     * @param numberOfValueSize
     * @return 
     * @throws
     */

    public FixWriteCoreFileBaseKeyMap(String[] dirs, int innerCacheSize, int numberOfKeyData, int numberOfValueSize) {
        try {
            this.oneDataLength = numberOfValueSize;
            this.lineDataSize =  this.keyDataLength + this.oneDataLength;
            if (8192 > this.lineDataSize) {
                this.getDataSize = this.lineDataSize * (8192 / this.lineDataSize) * 5;
            } else {
//              this.getDataSize = this.lineDataSize * 1 * 2;
                this.getDataSize = this.lineDataSize;
                this.numberOfOneFileKey = 100;
            }

            this.baseFileDirs = dirs;
            this.innerCacheSize = innerCacheSize;
            if (numberOfKeyData <=  this.numberOfOneFileKey) numberOfKeyData =  this.numberOfOneFileKey * 2;
            this.numberOfDataFiles = numberOfKeyData / this.numberOfOneFileKey;

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

                // Keyファイルのディレクトリ範囲ないで適当に記録ファイルを分散させる
                File file = new File(this.fileDirs[i % this.fileDirs.length] + i + ".data");
                if (file.exists()) {
                    file.delete();
                }

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
/*
long start1 = 0L;
long start2 = 0L;
long start3 = 0L;
long start4 = 0L;

long end1 = 0L;
long end2 = 0L;
long end3 = 0L;
long end4 = 0L;
*/
        try {

//start1 = System.nanoTime();
//start2 = System.nanoTime();
            File file = dataFileList[hashCode % numberOfDataFiles];

            StringBuilder buf = new StringBuilder(this.keyDataLength);

            //TODO:ここ直す
            buf.append(this.fillCharacter(key, keyDataLength));
            //buf.append(this.fillCharacter(value, oneDataLength));


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
//end2 = System.nanoTime();
//start3 = System.nanoTime();
            // KeyData Write File
            for (int tryIdx = 0; tryIdx < 2; tryIdx++) {
                try {

                    // Key値の場所を特定する

                    long[] dataLineNoRet = this.getLinePoint(key, raf);
//end3 = System.nanoTime();
//start4 = System.nanoTime();
                    if (dataLineNoRet[0] == -1) {

                        wr.write(buf.toString());
                        SystemUtil.diskAccessSync(wr);
                        wr.write(value);
                        SystemUtil.diskAccessSync(wr);

                        // 渡されたデータが固定の長さ分ない場合は足りない部分を補う
                        int valueSize = value.length();
                        byte[] fillByte = new byte[1];
                        fillByte[0] = new Integer(FileBaseDataMap.paddingSymbol).byteValue();
                        int paddingSize = (oneDataLength - valueSize);

                        int writeSetCount = paddingSize / 512;
                        int singleWriteCount = paddingSize % 512;


                        for (int i = 0; i < writeSetCount; i++) {

                            wr.write(FileBaseDataMap.paddingSymbolSetString);
                            if ((i % 14) == 0) SystemUtil.diskAccessSync(wr);
                        }
                        SystemUtil.diskAccessSync(wr);

                        byte[] fillBytes = new byte[singleWriteCount];
                        for (int i = 0; i < singleWriteCount; i++) {
                            fillBytes[i] = fillByte[0];
                        }

                        wr.write(new String(fillBytes));
                        SystemUtil.diskAccessSync(wr);

                        // The size of an increment
                        this.totalSize.getAndIncrement();
                    } else {

                        // 過去に存在したデータなら1増分
                        boolean increMentFlg = false;
                        if (dataLineNoRet[1] == -1) increMentFlg = true;
                        //if (this.get(key, hashCode) == null) increMentFlg = true;

                        raf.seek(dataLineNoRet[0] * (lineDataSize));
                        raf.write(buf.toString().getBytes(), 0, keyDataLength);
                        raf.write(value.getBytes());


                        // 渡されたデータが固定の長さ分ない場合は足りない部分を補う
                        int valueSize = value.length();
                        byte[] fillByte = new byte[1];
                        fillByte[0] = new Integer(FileBaseDataMap.paddingSymbol).byteValue();

                        int paddingSize = (oneDataLength - valueSize);

                        int writeSetCount = paddingSize / (4096);
                        int singleWriteCount = paddingSize % (4096);

                        for (int i = 0; i < writeSetCount; i++) {
                            raf.write(FileBaseDataMap.fillStream.toByteArray());
                        }


                        byte[] remainderPaddingBytes = new byte[singleWriteCount];

                        for (int i = 0; i < singleWriteCount; i++) {
                            remainderPaddingBytes[i] = fillByte[0];
                        }
                        if (remainderPaddingBytes.length > 0) raf.write(remainderPaddingBytes);


                        if (increMentFlg) this.totalSize.getAndIncrement();

                    }
//end4 = System.nanoTime();
                    break;
                } catch (IOException ie) {

                    // IOExceptionの場合は1回のみファイルを再度開く
                    if (tryIdx == 1) throw ie;
                    try {

                        if (raf != null) raf.close();
                        if (wr != null) wr.close();

                        raf = new RandomAccessFile(file, "rwd");
                        wr = new BufferedWriter(new FileWriter(file, true));
                        accessor = new CacheContainer();
                        accessor.raf = raf;
                        accessor.wr = wr;
                        accessor.file = file;
                        innerCache.put(file.getAbsolutePath(), accessor);
                    } catch (Exception e) {
                        throw e;
                    }
                }
            }
        } catch (Exception e2) {
            e2.printStackTrace();
        }
//end1 = System.nanoTime();
//if (ImdstDefine.fileBaseMapTimeDebug) {
//  System.out.println("1="+(end1 - start1) + " 2="+(end2 - start2) + " 3="+(end3 - start3) + " 4="+(end4 - start4));
//}

    }


    // 指定のキー値が指定のファイル内でどこにあるかを調べる
    private long[] getLinePoint(String key, RandomAccessFile raf) throws Exception {
        long[] ret = {-1, 0};
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

        equalKeyBytes[equalKeyBytes.length - 1] = new Integer(FileBaseDataMap.paddingSymbol).byteValue();

        try {

            raf.seek(0);
            int readLen = -1;
            while((readLen = SystemUtil.diskAccessSync(raf, lineBufs)) != -1) {

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
                        // 削除データか確かめる
                        if (lineBufs[assist + keyDataLength] == FileBaseDataMap.paddingSymbol) ret[1] = -1;
                        break;
                    }

                    lineCount++;
                }
                if (matchFlg) break;
            }

        } catch (IOException ie) {
            throw ie;
        } catch (Exception e) {
            throw e;
        }
        ret[0] = line;
        return ret;
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

        equalKeyBytes[equalKeyBytes.length - 1] = new Integer(FileBaseDataMap.paddingSymbol).byteValue();

        try {

            File file = dataFileList[hashCode % numberOfDataFiles];


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


            for (int tryIdx = 0; tryIdx < 2; tryIdx++) {

                try {
                    raf.seek(0);
                    int readLen = -1;
                    while((readLen = SystemUtil.diskAccessSync(raf, lineBufs)) != -1) {

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
                    break;
                } catch (IOException ie) {

                    // IOExceptionの場合は1回のみファイルをサイド開く
                    if (tryIdx == 1) throw ie;

                    try {
                        if (raf != null) raf.close();
                        if (wr != null) wr.close();

                        raf = new RandomAccessFile(file, "rwd");
                        wr = new BufferedWriter(new FileWriter(file, true));
                        accessor = new CacheContainer();
                        accessor.raf = raf;
                        accessor.wr = wr;
                        accessor.file = file;
                        innerCache.put(file.getAbsolutePath(), accessor);
                    } catch (Exception e) {
                        throw e;
                    }
                }
            }


            // 取得データを文字列化
            if (tmpBytes != null) {

                if (tmpBytes[keyDataLength] != FileBaseDataMap.paddingSymbol) {

                    int i = keyDataLength;
                    int counter = 0;

                    for (; i < tmpBytes.length; i++) {

                        if (tmpBytes[i] == FileBaseDataMap.paddingSymbol) break;
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

        ret = this.get(key, hashCode);
        if(ret != null) {

            this.put(key, "&&&&&&&&&&&", hashCode);

            // The size of an decrement
            this.totalSize.getAndDecrement();
        }
        return ret;
    }


    /**
     * return of TotalSize
     *
     * @return size
     */
    public AtomicInteger getTotalSize() {
        return this.totalSize;
    }


    /**
     * 指定の文字を指定の桁数で特定文字列で埋める.<br>
     * 足りない文字列は固定の"&"で補う(38).<br>
     *
     * @param data
     * @param fixSize
     */
    private String fillCharacter(String data, int fixSize) {
        return SystemUtil.fillCharacter(data, fixSize, FileBaseDataMap.paddingSymbol);
    }


    public int getCacheSize() {
        return innerCache.getSize();
    }


    /**
     * キー値を連続して取得する準備を行う.<br>
     *
     * @param
     * @return 
     * @throws
     */
    public void startKeyIteration() {
        this.nowIterationFileIndex =  0;
        this.nowIterationFpPosition =  0L;
    }


    /**
     * 自身が保持するキー値を返す.<br>
     * 最終的にキー値を全て返すとnullを返す.<br>
     *
     * @param
     * @return 
     * @throws
     */
    public List getAllOneFileInKeys() {

        List keys = null;
        byte[] datas = null;
        StringBuilder keysBuf = null;
        RandomAccessFile raf = null;

        try {
            if (this.nowIterationFileIndex < this.dataFileList.length) {

                keys = new ArrayList();

                long oneFileLength = new Long(this.dataFileList[this.nowIterationFileIndex].length()).longValue();

                long readSize = lineDataSize * 10;
                int readLoop = new Long(oneFileLength / readSize).intValue();
                if ((oneFileLength % readSize) > 0) readLoop++;
                raf = new RandomAccessFile(this.dataFileList[this.nowIterationFileIndex], "rwd");
                raf.seek(0);

                for (int readLoopIdx = 0; readLoopIdx < readLoop; readLoopIdx++) {
                    datas = new byte[new Long(readSize).intValue()];

                    int readLen = -1;
                    readLen = SystemUtil.diskAccessSync(raf, datas);

                    if (readLen > 0) {

                        int loop = readLen / lineDataSize;

                        for (int loopIdx = 0; loopIdx < loop; loopIdx++) {

                            int assist = (lineDataSize * loopIdx);
                            keysBuf = new StringBuilder(ImdstDefine.stringBufferLarge_3Size);

                            int idx = 0;

                            while (true) {

                                if (datas[assist + idx] != FileBaseDataMap.paddingSymbol) {
                                    keysBuf.append(new String(datas, assist + idx, 1));
                                } else {
                                    break;
                                }
                                idx++;
                            }
                            keys.add(keysBuf.toString());
                            keysBuf = null;
                        }
                    }
                }
            }
            this.nowIterationFileIndex++;
        } catch(Exception e) {

            e.printStackTrace();
        } finally {

            try {
                if(raf != null) raf.close();

                raf = null;
                datas = null;
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
        return keys;
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
        this.fileBaseDataMap.iteratorInit();
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

        if (value == null) return null;

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
        maxCacheSize = 64;
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
