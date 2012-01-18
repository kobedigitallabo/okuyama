package okuyama.imdst.util.persistentmap;


import java.lang.reflect.Constructor;
import java.util.*;
import java.io.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;
import java.util.concurrent.ConcurrentHashMap;

import java.util.zip.*;

import okuyama.imdst.util.*;


/**
 * データ格納Map.<br>
 * 格納されるKeyとValueはKey値のHash値から導き出された、<br>
 * 特定の集合のHashMapに格納される。そしてそのHashMapはSerializeされさらに圧縮されて<br>
 * byte配列として、1つのMapに格納される。<br>
 * Serializeと圧縮を使うことと、全てのKeyとValueを格納するMapの要素数を増やさないことで<br>
 * メモリ使用量を減らす.<br>
 * スレッドセーフに並列アクセスが可能なように実装されている<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class PersistentMap extends AbstractMap implements Cloneable, Serializable {

    private AtomicInteger nowSize = new AtomicInteger(0);

    int parallelControl = 19999;

    private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
    final Lock r = rwl.readLock();
    final Lock w = rwl.writeLock();

    private Integer[] syncObjs = null;

    private Map baseMap = null;


    private boolean classFix = false;
    private Class keyClass = null;
    private Class valueClass = null;

    private static HashMap uniqueNameMap = new HashMap(10);

    public static long bucketJvm1MBMemoryFactor = ImdstDefine.serializeMapBucketSizeMemoryFactor;


    /**
     * コンストラクタ
     *
     *
     * @param size 予想格納最大数(現在内部的には利用しない)
     * @param upper 格納上限拡張閾値(現在内部的には利用しない)
     * @param multi 実際に格納に使用する集合バケット数 (現在の検証で1GBのJVMへのメモリ割当で40万程度、1MBで400件程度が適正値)
     */
    public PersistentMap(int size, int upper, int multi, String serializeClassName) {
        System.out.println("PersistentMap BucketSize= " + multi);
        System.out.println("PersistentMap SerializerClassName= " + serializeClassName);
        String uniqueName = createUniqueName();
        System.out.println("PersistentMap UniqueName = " + uniqueName);


    }


    private int hashPointCalc(int hash) {
        return ((hash << 1) >>> 1) % parallelControl;
    }



    /**
     * set<br>
     *
     * @param key
     * @param value
     */
    public Object put(Object key, Object value) {

        r.lock();

        try { 

        } finally {
            r.unlock(); 
        }
        return null;
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
            return null;
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


        r.lock();

        try { 

        } finally {
            r.unlock(); 
        }
        return null;
    }


    /**
     * containsKey<br>
     *
     * @param key
     * @return boolean
     */
    public boolean containsKey(Object key) {
        boolean ret = false;
        r.lock();
        try { 
        } finally {
            r.unlock(); 
        }
        return ret;
    }


    /**
     * clear<br>
     *
     */
    public void clear() {
        w.lock();
        try { 
        } finally {
            w.unlock(); 
        }

    }


    /**
     * size.<br>
     *
     * @param
     * @return int
     * @throws
     */
    public int size() {
        return -1;
    }


    /**
     * entrySet<br>
     *
     * @return Set
     */
    public Set entrySet() {
        return new PersistentMapSet(baseMap.entrySet(), this);
    }


    private static String createUniqueName() {
        String retStr = null;
        synchronized(uniqueNameMap) {
            Random rnd = new Random();

            while(true) {
                int rndInt = rnd.nextInt(19999);
                String uniqueKey = "PersistentMapName_" + rndInt;
                if (!uniqueNameMap.containsKey(uniqueKey)) {
                    retStr = uniqueKey;
                    uniqueNameMap.put(retStr, null);
                    break;
                }
            }
        }
        return retStr;
    }
}
