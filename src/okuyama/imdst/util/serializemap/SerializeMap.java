package okuyama.imdst.util.serializemap;


import java.util.*;
import java.io.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;
import java.util.concurrent.ConcurrentHashMap;

import java.util.concurrent.ConcurrentLinkedQueue;
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
public class SerializeMap extends AbstractMap implements Cloneable, Serializable {

    private AtomicInteger nowSize = new AtomicInteger(0);

    int parallelControl = 19999;

    private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
    final Lock r = rwl.readLock();
    final Lock w = rwl.writeLock();

    private Integer[] syncObjs = null;

    private Map baseMap = null;


    /**
     * コンストラクタ
     *
     *
     * @param size 予想格納最大数(現在内部的には利用しない)
     * @param upper 格納上限拡張閾値(現在内部的には利用しない)
     * @param multi 実際に格納に使用する集合バケット数
     */
    public SerializeMap(int size, int upper, int multi) {
        System.out.println("SerializeMap = " + multi);
        parallelControl = multi;
        syncObjs = new Integer[multi];
        for (int i = 0; i < parallelControl; i++) {
            syncObjs[i] = new Integer(i);
        }
        baseMap = new ConcurrentHashMap(multi, (multi - 1), 64);
    }


    private int hashPointCalc(int hash) {
        return ((hash << 1) >>> 1) % parallelControl;
    }


    public static byte[] dataSerialize(Map data) {
        return SystemUtil.defaultSerializeMap(data);
    }

    public static Map dataDeserialize(byte[] data) {
        return SystemUtil.defaultDeserializeMap(data);
    }
    

    /**
     * set<br>
     *
     * @param key
     * @param value
     */
    public Object put(Object key, Object value) {

        boolean incrFlg = false;
        r.lock();
        try { 

            int poitnInt = hashPointCalc(key.hashCode());
            Integer point = new Integer(poitnInt);
            byte[] target = null;

            synchronized(syncObjs[poitnInt]) {

               target = (byte[])baseMap.get(point);

                if (target != null)
                   target = SystemUtil.dataDecompress(target);

                if (target != null) {

                    Map targetMap = dataDeserialize(target);

                    // sizeを加算
                    if (!targetMap.containsKey(key)) incrFlg = true;

                    targetMap.put(key, value);
                    target = dataSerialize(targetMap);
                } else {

                    Map targetMap = new HashMap();
                    targetMap.put(key, value);
                    target = dataSerialize(targetMap);

                    // sizeを加算
                    incrFlg = true;
                }
                baseMap.put(point, SystemUtil.dataCompress(target));
            }

            // sizeを加算
            if (incrFlg)nowSize.incrementAndGet();
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

            int poitnInt = hashPointCalc(key.hashCode());
            Integer point = new Integer(poitnInt);
            byte[] target = null;

            synchronized(syncObjs[poitnInt]) {

                target = (byte[])baseMap.get(point);

            }

            if (target != null){
                target = SystemUtil.dataDecompress(target);
            } else {
                return null;
            }

            Map targetMap = dataDeserialize(target);
            return targetMap.get(key);
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
        boolean decrFlg = false;
        Object ret = null;
        r.lock();

        try { 
            int poitnInt = hashPointCalc(key.hashCode());
            Integer point = new Integer(poitnInt);
            byte[] target = null;

            synchronized(syncObjs[poitnInt]) {

                target = (byte[])baseMap.get(point);

                if (target != null){
                    target = SystemUtil.dataDecompress(target);
                } else {
                    return null;
                }

                Map targetMap = dataDeserialize(target);
                ret = targetMap.remove(key);

                if (ret != null) {
                    target = dataSerialize(targetMap);

                    // sizeを減算
                    decrFlg = true;
                }

                baseMap.put(point, SystemUtil.dataCompress(target));
            }

            if (decrFlg) nowSize.decrementAndGet();
        } finally {
            r.unlock(); 
        }
        return ret;
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

            int poitnInt = hashPointCalc(key.hashCode());
            Integer point = new Integer(poitnInt);
            byte[] target = null;

            synchronized(syncObjs[poitnInt]) {

                target = (byte[])baseMap.get(point);

                if (target != null){
                    target = SystemUtil.dataDecompress(target);
                } else {
                    return false;
                }

                Map targetMap = dataDeserialize(target);
                ret = targetMap.containsKey(key);
            }
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
            baseMap.clear();
            nowSize = new AtomicInteger(0);
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
        return nowSize.intValue();
    }


    /**
     * entrySet<br>
     *
     * @return Set
     */
    public Set entrySet() {
        return new SerializeMapSet(baseMap.entrySet(), this);
    }
}
