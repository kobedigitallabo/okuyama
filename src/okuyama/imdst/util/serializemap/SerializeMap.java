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


    // コンストラクタ
    public SerializeMap(int size, int upper, int multi) {

        parallelControl = multi;
        syncObjs = new Integer[multi];
        for (int i = 0; i < parallelControl; i++) {
            syncObjs[i] = new Integer(i);
        }
        baseMap = new ConcurrentHashMap((size / multi), (upper / multi), multi);
    }


    private int hashPointCalc(int hash) {
        return ((hash << 1) >>> 1) % parallelControl;
    }

    public static byte[] dataSerialize(Map data) {
        ByteArrayOutputStream bao = null;
        ObjectOutput oo = null;
        try {
            bao = new ByteArrayOutputStream(1000);
            oo = new ObjectOutputStream(bao);

            oo.writeObject(data);
            oo.flush();

            oo.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return bao.toByteArray();
    }

    public static Map dataDeserialize(byte[] data) {
        Map retData = null;
        ByteArrayInputStream bio = null;
        ObjectInputStream ois = null;
        try {
            bio = new ByteArrayInputStream(data);
            ois = new ObjectInputStream(bio);

            retData = (Map)ois.readObject();
            ois.close();
            bio.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return retData;
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

            int poitnInt = hashPointCalc(key.hashCode());

            synchronized(syncObjs[poitnInt]) {

                Integer point = new Integer(poitnInt);

                byte[] target = (byte[])baseMap.get(point);

                if (target != null)
                   target = SystemUtil.dataDecompress(target);

                if (target != null) {

                    Map targetMap = dataDeserialize(target);
                    targetMap.put(key, value);
                    target = dataSerialize(targetMap);
                } else {

                    Map targetMap = new HashMap();
                    targetMap.put(key, value);
                    target = dataSerialize(targetMap);
                }

                baseMap.put(point, SystemUtil.dataCompress(target));
                nowSize.incrementAndGet();
            }
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

            synchronized(syncObjs[poitnInt]) {

                Integer point = new Integer(poitnInt);

                byte[] target = (byte[])baseMap.get(point);

                if (target != null){
                    target = SystemUtil.dataDecompress(target);
                } else {
                    return null;
                }

                Map targetMap = dataDeserialize(target);
                return targetMap.get(key);
            }

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
        Object ret = null;
        r.lock();

        try { 
            int poitnInt = hashPointCalc(key.hashCode());

            synchronized(syncObjs[poitnInt]) {

                Integer point = new Integer(poitnInt);

                byte[] target = (byte[])baseMap.get(point);

                if (target != null){
                    target = SystemUtil.dataDecompress(target);
                } else {
                    return null;
                }

                Map targetMap = dataDeserialize(target);
                ret = targetMap.remove(key);

                if (ret != null) {
                    target = dataSerialize(targetMap);
                }

                baseMap.put(point, SystemUtil.dataCompress(target));
                nowSize.decrementAndGet();
            }
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

            synchronized(syncObjs[poitnInt]) {

                Integer point = new Integer(poitnInt);

                byte[] target = (byte[])baseMap.get(point);

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
            nowSize = new AtomicInteger();
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
