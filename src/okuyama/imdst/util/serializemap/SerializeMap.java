package okuyama.imdst.util.serializemap;


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
public class SerializeMap extends AbstractMap implements Cloneable, Serializable, ICoreStorage {

    private AtomicInteger nowSize = new AtomicInteger(0);

    int parallelControl = 19999;

    private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
    final Lock r = rwl.readLock();
    final Lock w = rwl.writeLock();

    private Integer[] syncObjs = null;

    private Map baseMap = null;

    private ISerializer serializer = null;

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
    public SerializeMap(int size, int upper, int multi, String serializeClassName) {
        System.out.println("SerializeMap BucketSize= " + multi);
        System.out.println("SerializeMap SerializerClassName= " + serializeClassName);
        String uniqueName = createUniqueName();
        System.out.println("SerializeMap UniqueName = " + uniqueName);


        parallelControl = multi;
        syncObjs = new Integer[multi];
        for (int i = 0; i < parallelControl; i++) {
            syncObjs[i] = new Integer(i);
        }
        baseMap = new ConcurrentHashMap(multi, (multi - 1), 64);

        // シリアライザインスタンス化
        try {
            if (serializeClassName.indexOf(";") == -1) {

                this.serializer = (ISerializer)((Class)Class.forName(serializeClassName)).newInstance();
                this.serializer.setInstanceCreateMapName(uniqueName);
            } else {

                Class[] constructorTypes = {String.class};
                Object[] constructorArgs = new Object[1];
                String[] classCreateWork = serializeClassName.split(";");
                String className = classCreateWork[0];
                String  constructorParam = classCreateWork[1];
                constructorArgs[0] = constructorParam;

                Class serializeClazz = (Class)Class.forName(className);
                Constructor constructor = serializeClazz.getConstructor(constructorTypes);
                this.serializer = (ISerializer)constructor.newInstance(constructorArgs);
                this.serializer.setInstanceCreateMapName(uniqueName);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private int hashPointCalc(int hash) {
        return ((hash << 1) >>> 1) % parallelControl;
    }


    public byte[] dataSerialize(Map data, Class keyClazz, Class valueClazz, Object key, int uniqueNo) {

        return this.serializer.serialize(data, keyClazz, valueClazz, key, uniqueNo);
    }

    public Map dataDeserialize(byte[] data, Object key, int uniqueNo) {

        return this.serializer.deSerialize(data, key, uniqueNo);
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
        if (!this.classFix) {
            this.classFix = true;
            this.keyClass = key.getClass();
            this.valueClass = value.getClass();
        }

        try { 

            int poitnInt = hashPointCalc(key.hashCode());
            Integer point = new Integer(poitnInt);
            byte[] target = null;

            synchronized(syncObjs[poitnInt]) {

               target = (byte[])baseMap.get(point);


                if (target != null) {

                    Map targetMap = dataDeserialize(target, key, poitnInt);

                    // sizeを加算
                    if (!targetMap.containsKey(key)) incrFlg = true;

                    targetMap.put(key, value);
                    target = dataSerialize(targetMap, this.keyClass, this.valueClass, key, poitnInt);
                } else {

                    Map targetMap = new HashMap();
                    targetMap.put(key, value);
                    target = dataSerialize(targetMap, this.keyClass, this.valueClass, key, poitnInt);

                    // sizeを加算
                    incrFlg = true;
                }
                baseMap.put(point, target);
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

            if (target == null) {
                return null;
            }

            Map targetMap = dataDeserialize(target, key, poitnInt);
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

                if (target == null){
                    return null;
                }

                Map targetMap = dataDeserialize(target, key, poitnInt);
                ret = targetMap.remove(key);

                if (ret != null) {
                    target = dataSerialize(targetMap, this.keyClass, this.valueClass, key, poitnInt);

                    // sizeを減算
                    decrFlg = true;
                } 

                baseMap.put(point, target);
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
                if (target == null) {
                    return false;
                }

                Map targetMap = dataDeserialize(target, key, poitnInt);
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
            this.serializer.clearParentMap();
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


    private static String createUniqueName() {
        String retStr = null;
        synchronized(uniqueNameMap) {
            Random rnd = new Random();

            while(true) {
                int rndInt = rnd.nextInt(19999);
                String uniqueKey = "SerializeMapName_" + rndInt;
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
