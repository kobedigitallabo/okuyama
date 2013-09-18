package fuse.okuyamafs;



import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.TimeUnit;

import okuyama.imdst.client.*;



/**
 * OkuyamaFuse.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class CacheStoreDaemon extends Thread {


    private ArrayBlockingQueue storeQueue = null;

    private Map tmpDataMap = null;

    public OkuyamaFsMap okuyamaFs = null;

    int maxParallel = 64;
    public Object[] parallelAccessSync = new Object[maxParallel];


    public CacheStoreDaemon(int cacheSize, OkuyamaFsMap okuyamaFs) {
        this.storeQueue = new ArrayBlockingQueue(cacheSize);
        this.okuyamaFs = okuyamaFs;
        this.tmpDataMap = new LRUCacheMap(cacheSize);
        for (int i = 0; i < maxParallel; i++) {
            parallelAccessSync[i] = new Object();
        }
    }

    public void run() {
            System.out.println("CacheStoreDaemon-start");
        while (true) {

            try {
                Object[] requestData = null;
                while (true) {
                    requestData = (Object[])this.storeQueue.poll(1000, TimeUnit.MILLISECONDS);
                    if (requestData != null) break;
                    if (OkuyamaFilesystem.jvmShutdownStatus == true) break;
                }
                if (OkuyamaFilesystem.jvmShutdownStatus == true) break;

                if (tmpDataMap.containsKey(requestData[1])) {

                    Object registData = requestData[2];

                    Object tmpDataObj = tmpDataMap.get(requestData[1]);


                    if (requestData[0].equals("String")) {
                        synchronized(parallelAccessSync[((requestData[1].hashCode() << 1) >>> 1) % maxParallel]) {

                            if (!((String)registData).equals((String)tmpDataObj)) continue;
                            this.okuyamaFs.putString(requestData[1], (String)requestData[2]);
                        }
                    } else if (requestData[0].equals("Map")) {

                        synchronized(parallelAccessSync[((requestData[1].hashCode() << 1) >>> 1) % maxParallel]) {

                            if (!((Map)registData).equals((Map)tmpDataObj)) continue;
                            this.okuyamaFs.putMap(requestData[1], (Map)requestData[2]);
                        }
                    } else if (requestData[0].equals("bytes")) {

                        if (!Arrays.equals((byte[])registData, (byte[])tmpDataObj)) continue;
                        this.okuyamaFs.putBytes(requestData[1], (byte[])requestData[2]);
                    }
                }
            } catch(Exception e) {
                e.printStackTrace();
            }
        }
        System.out.println("CacheStoreDaemon-end");
    }


    public boolean containsKey(Object key) {
        synchronized(parallelAccessSync[((key.hashCode() << 1) >>> 1) % maxParallel]) {
            if (tmpDataMap.containsKey(key)) {
                return true;
            } else {
                return okuyamaFs.containsKey(key);
            }
        }
    }

    public Object putString(Object key, String value) {
        synchronized(parallelAccessSync[((key.hashCode() << 1) >>> 1) % maxParallel]) {
            return this.putStoreRequest("String", key, value);
        }
    }

    public Object putMap(Object key, Map value) {
        synchronized(parallelAccessSync[((key.hashCode() << 1) >>> 1) % maxParallel]) {
            return this.putStoreRequest("Map", key, value);
        }
    }

    public Object putBytes(Object key, byte[] value) {
        synchronized(parallelAccessSync[((key.hashCode() << 1) >>> 1) % maxParallel]) {
            return this.putStoreRequest("bytes", key, value);
        }
    }


    private Object putStoreRequest(String type, Object key, Object value) {
        Object[] storeRequest = new Object[3];
        storeRequest[0] = type;
        storeRequest[1] = key;
        storeRequest[2] = value;

        try {
            synchronized(parallelAccessSync[((key.hashCode() << 1) >>> 1) % maxParallel]) {

                tmpDataMap.put(key, value);
                storeQueue.put(storeRequest);
            }
        } catch(Exception e) {
            e.printStackTrace();
        }

        return null;
    }



    public String getString(Object key) {
        synchronized(parallelAccessSync[((key.hashCode() << 1) >>> 1) % maxParallel]) {
            if (tmpDataMap.containsKey(key)) {
                String value = (String)tmpDataMap.get(key);
                if (value == null) value = okuyamaFs.getString(key);
                return value;
            } else {

                String val = okuyamaFs.getString(key);
                if (OkuyamaClientWrapper.singleMode == true && val != null) {
                    tmpDataMap.put(key, val);
                }

                return val;
            }
        }
    }

    public Map getMap(Object key) {
        synchronized(parallelAccessSync[((key.hashCode() << 1) >>> 1) % maxParallel]) {
            if (tmpDataMap.containsKey(key)) {

                Map value = (Map)tmpDataMap.get(key);
                if (value == null) value = okuyamaFs.getMap(key);
                return value;

            } else {
                Map val = okuyamaFs.getMap(key);
                if (OkuyamaClientWrapper.singleMode == true && val != null) {
                    tmpDataMap.put(key, val);
                }

                return val;
            }
        }
    }

    public byte[] getBytes(Object key) {
        synchronized(parallelAccessSync[((key.hashCode() << 1) >>> 1) % maxParallel]) {
            if (tmpDataMap.containsKey(key)) {

                byte[] value = (byte[])tmpDataMap.get(key);
                if (value == null) value = okuyamaFs.getBytes(key);
                return value;

            } else {
                byte[] val = okuyamaFs.getBytes(key);
                if (OkuyamaClientWrapper.singleMode == true && val != null) {
                    tmpDataMap.put(key, val);
                }

                return val;
            }
        }
    }


    public Object remove(Object key) {
        synchronized(parallelAccessSync[((key.hashCode() << 1) >>> 1) % maxParallel]) {
            tmpDataMap.remove(key);
            return okuyamaFs.remove(key);
        }
    }
}
