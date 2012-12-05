package fuse.okuyamafs;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.*;

import okuyama.imdst.client.*;
import okuyama.imdst.util.*;

/**
 * OkuyamaFuse.<br>
 * okuyama上で全てのデータを管理するコアのMap.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class OkuyamaFsMap implements IFsMap {

    public int type = -1;


    private String[] masterNodeList = null;

    private Object putSync = new Object();
    private Object delSync = new Object();

    private DelayStoreDaemon[] delayStoreDaemon = null;
    public static int delayStoreDaemonSize = 60;
    public static int allDelaySJobSize = 100000;

    private ArrayBlockingQueue responseCheckDaemonQueue = new ArrayBlockingQueue(130);

    private ArrayBlockingQueue requestCheckDaemonQueue = new ArrayBlockingQueue(130);


    private static boolean preFetchFlg = true;
    private static int maxPreFetchDaemon = 3;
    private ArrayBlockingQueue preFetchDataDaemonQueue = new ArrayBlockingQueue(maxPreFetchDaemon);
    private ConcurrentHashMap preFetchRequestMarker = null;


    private ExpireCacheMap dataCache = null;
    //static Map dataCache = new MemcacheMap();
    static boolean getCache = true;

    public OkuyamaClientFactory factory = null;


    /**
     * コンストラクタ
     */
    public OkuyamaFsMap(int type, String[] masterNodeInfos) {
        this.type = type;
        this.masterNodeList = masterNodeInfos;
        try {
            this.factory = OkuyamaClientFactory.getFactory(this.masterNodeList, 350);

            if (type == 1) {
                this.dataCache = new ExpireCacheMap(OkuyamaFsMap.allDelaySJobSize, 100*1000, this.factory);
            } else {
                this.dataCache = new ExpireCacheMap(OkuyamaFsMap.allDelaySJobSize, 5*1000);
            }
            /*this.delayStoreDaemon = new DelayStoreDaemon[delayStoreDaemonSize];
            for (int idx = 0; idx < delayStoreDaemonSize; idx++) {
                this.delayStoreDaemon[idx] = new DelayStoreDaemon(masterNodeInfos, (allDelaySJobSize / delayStoreDaemonSize), dataCache, this.factory);
                this.delayStoreDaemon[idx].start();
            }*/

            for (int idx = 0; idx < 40; idx++) {

                ResponseCheckDaemon responseCheckDaemon = new ResponseCheckDaemon(this.factory);
                responseCheckDaemon.start();
                this.responseCheckDaemonQueue.put(responseCheckDaemon);
            }

            for (int idx = 0; idx < 60; idx++) {

                RequestCheckDaemon requestCheckDaemon = new RequestCheckDaemon(this.factory);
                requestCheckDaemon.start();
                this.requestCheckDaemonQueue.put(requestCheckDaemon);
            }

            if (preFetchFlg) {
                this.preFetchRequestMarker = new ConcurrentHashMap();
                for (int idx = 0; idx < maxPreFetchDaemon; idx++) {
                    PreFetchDaemon preFetchDaemon = new PreFetchDaemon(this.dataCache, this.factory, this.preFetchDataDaemonQueue, this.preFetchRequestMarker);
                    preFetchDaemon.start();
                    this.preFetchDataDaemonQueue.put(preFetchDaemon);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public OkuyamaClient createClient() {
        OkuyamaClient client = null;
        try {
            client = factory.getClient();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return client;
    }


    public boolean putNewString(Object key, String value) {

       OkuyamaClient client = createClient();
        try {

            Object[] setRet = client.setNewValue(type + "\t" + (String)key, value);
            if (!setRet[0].equals("true")) {
                return false;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                client.close();
            } catch (Exception ee) {
            }
        }
        return true;
    }

    public boolean putNewMap(Object key, Map value) {

        OkuyamaClient client = createClient();
        try {

            Object[] setRet = client.setNewObjectValue(type + "\t" + (String)key, value);

            if (!setRet[0].equals("true")) {
                return false;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        } finally {
            try {
                client.close();
            } catch (Exception ee) {
            }

        }
        return true;
    }

    public boolean putNewBytes(Object key, byte[] value) {
        OkuyamaClient client = createClient();
        try {

            synchronized(putSync) {

                Object[] checkRet = client.readByteValue(type + "\t" + (String)key);
                if (((String)checkRet[0]).equals("true"))  return false;

                client.sendByteValue(type + "\t" + (String)key, value);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                client.close();
            } catch (Exception ee) {
            }
        }

        return true;
    }


    public Object putBytes(Object key, byte[] value) {
        OkuyamaClient client = createClient();

        try {
            String keyStr = type + "\t" + (String)key;
            client.sendByteValue(keyStr, value);
            //this.delayStoreDaemon[(((keyStr.hashCode() << 1) >>> 1) % delayStoreDaemonSize)].putBytes(keyStr, putBytes);
            //dataCache.put(keyStr, putBytes);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    public Object putMultiBytes(Object[] dataList) {


        try {
            List useDaemonList = new ArrayList();
            //long start = System.nanoTime();
            // 暫定実装
            for (int idx = 0; idx < dataList.length; idx++){

                int maxParallel = 50;
                if (useDaemonList.size() == maxParallel) {
                    for (int useDaemonListIdx = 0; useDaemonListIdx < maxParallel; useDaemonListIdx++) {

                        RequestCheckDaemon daemon = (RequestCheckDaemon)useDaemonList.remove(0);
                        Integer ret = (Integer)daemon.takeResponse();
                        if (ret.intValue() == 1) {
                            throw new Exception("putMultiBytes - error");
                        }
                        this.requestCheckDaemonQueue.put(daemon);
                    }
                }
                RequestCheckDaemon daemon = (RequestCheckDaemon)this.requestCheckDaemonQueue.poll();

                Object[] putData = (Object[])dataList[idx];
                String keyStr = type + "\t" + (String)putData[0];
                byte[] data = (byte[])putData[1];

                if (daemon != null) {

                    daemon.putRequest(keyStr, data);
                    useDaemonList.add(daemon);
                } else {

                    OkuyamaClient client = createClient();
                    client.sendByteValue(keyStr, data);
                    client.close();
                }
            }
            int daemonListSize = useDaemonList.size();
            if (daemonListSize > 0) {
                
                for (int idx = 0; idx < daemonListSize; idx++) {

                    RequestCheckDaemon daemon = (RequestCheckDaemon)useDaemonList.get(idx);
                    Integer ret = (Integer)daemon.takeResponse();
                    if (ret.intValue() == 1) {
                        throw new Exception("putMultiBytes - error");
                    }
                    this.requestCheckDaemonQueue.put(daemon);
                }
            }
            //long end = System.nanoTime();
            //System.out.println("putMulti:DataCnt=" + dataList.length + " time=" + (end - start));

        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }


    public Object putMap(Object key, Map value) {
        OkuyamaClient client = createClient();
        try {

            client.setObjectValue(type + "\t" + (String)key, value);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                client.close();
            } catch (Exception ee) {
            }

        }

        return null;
    }


    public Object putString(Object key, String value) {
        OkuyamaClient client = createClient();
        try {

            String keyStr = type + "\t" + (String)key;
            client.setValue(keyStr, value);
            //this.delayStoreDaemon[(((keyStr.hashCode() << 1) >>> 1) % delayStoreDaemonSize)].putString(keyStr, value);
            dataCache.put(keyStr, value);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                client.close();
            } catch (Exception ee) {
            }

        }
        return null;
    }


    public String getString(Object key) {
        OkuyamaClient client = createClient();
        try {
            String keyStr = type + "\t" + (String)key;
            String cacheRetStr = (String)dataCache.get(keyStr);

            if (cacheRetStr != null) return cacheRetStr;

            String[] ret = client.getValue(type + "\t" + (String)key);
            if (ret[0].equals("true")) {
                // データ有り
                return ret[1];
            } else if (ret[0].equals("false")) {
                return null;
            } else if (ret[0].equals("error")) {
                return null;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                client.close();
            } catch (Exception ee) {
            }

        }
        return null;
    }

    public Map getMap(Object key) {
        OkuyamaClient client = createClient();
        try {
            Object[] ret = client.getObjectValue(type + "\t" + (String)key);
            if (ret[0].equals("true")) {
                // データ有り
                return (Map)ret[1];
            } else if (ret[0].equals("false")) {
                return null;
            } else if (ret[0].equals("error")) {
                return null;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                client.close();
            } catch (Exception ee) {
            }

        }
        return null;
    }

    public byte[] getBytes(Object key) {
    //return (byte[])dumm.get(type + "\t" + (String)key);
        OkuyamaClient client = createClient();
        String realKey = type + "\t" + (String)key;

        try {
            byte[] data = (byte[])dataCache.get(realKey);
            if (data == null) {
                Object[] ret = client.readByteValue(realKey);
            
                if (ret[0].equals("true")) {
                    // データ有り
                    byte[] retBytes = (byte[])ret[1];
                    if (retBytes != null) {
                        return retBytes;
                    }
                    return retBytes;
                }
            } else {
                return data;
            }
               
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                client.close();
            } catch (Exception ee) {
            }

        }
        return null;
    }

    public Map getMultiBytes(Object[] keyList) {
    //return (byte[])dumm.get(type + "\t" + (String)key);
        Map retMap = new HashMap();
        Map okuyamaDataMap = new HashMap();

        try {
            List tmpKeyList = new ArrayList();
            List realTmpKeyList = new ArrayList();
            String[] keyStrList = null;
            
            for (int idx = 0; idx < keyList.length; idx++) {
                String key = type + "\t" + (String)keyList[idx];
                byte[] data = (byte[])dataCache.get(key);

                if (data == null) {
                    tmpKeyList.add(key);
                    realTmpKeyList.add((String)keyList[idx]);
                } else {
                    retMap.put((String)keyList[idx], data);
                }
            }
            if (tmpKeyList.size() > 0) {
                keyStrList = (String[])tmpKeyList.toArray(new String[0]);

                if (preFetchFlg) {
                    String[] preFetchCheck = keyStrList[0].split("\t");

                    if (!preFetchRequestMarker.containsKey(preFetchCheck[0] + "\t" + preFetchCheck[1])) {

                        PreFetchDaemon preFetchDaemon = (PreFetchDaemon)this.preFetchDataDaemonQueue.poll();
                        if (preFetchDaemon != null) {

                            preFetchDaemon.putRequest(keyStrList[keyStrList.length - 1]);
                        }
                    }
                }

                int maxLimit = 35;
                int limit = 0;

                OkuyamaClient client = null;

                Map daemonMap = new HashMap(40);
                for (int idx = 0; idx < keyStrList.length; idx++) {

                    String key = (String)keyStrList[idx];
                    ResponseCheckDaemon daemon = (ResponseCheckDaemon)this.responseCheckDaemonQueue.poll();
                    if (daemon == null) {
                        daemon = new ResponseCheckDaemon(this.factory);
                        daemon.start();
                    }
                    daemon.putRequest(key);
                    daemonMap.put(key, daemon);
                }
                for (int i = 0; i < keyStrList.length; i++) {

                    ResponseCheckDaemon daemon = (ResponseCheckDaemon)daemonMap.get(keyStrList[i]);
                    Object[] responseObj = daemon.takeResponse();
                    if (responseObj.length > 0) {

                        //if (!dataCache.containsKey(keyStrList[i])) dataCache.put(keyStrList[i], (byte[])responseObj[1]);
                        retMap.put(realTmpKeyList.get(i), (byte[])responseObj[1]);
                    } 
                    if (!this.responseCheckDaemonQueue.offer(daemon)) daemon.endRequest();
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        } 
        return retMap;
    }

    public Object remove(Object key) {
        OkuyamaClient client = createClient();
        try {
            String cnvKey = type + "\t" + (String)key;
            Object[] ret = client.removeValue(cnvKey);
            dataCache.remove(cnvKey);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                client.close();
            } catch (Exception ee) {
            }

        }
        return null;
    }

    public boolean removeMulti(Object[] keyList) {

        boolean ret = true;

        try {

            //List clientList = new ArrayList(20);
            Object[] clientList = new Object[keyList.length];
            for (int idx = 0; idx < keyList.length; idx++) {

                if (keyList[idx] != null) {
                    OkuyamaClient client = createClient();
                    Object key = keyList[idx];

                    String removeKey = type + "\t" + (String)key;
                    client.requestRemoveValue(removeKey);

                    Object[] checkSt = new Object[2];
                    checkSt[0] = removeKey;
                    checkSt[1] = client;
                    clientList[idx] = checkSt;
                }
            }


            for (int idx = 0; idx < clientList.length; idx++) {

                Object[] checkSt = (Object[])clientList[idx];

                String removeKey = (String)checkSt[0];
                OkuyamaClient client = (OkuyamaClient)checkSt[1];

                String[] removeRet = client.responseRemoveValue(removeKey);
                client.close();

                if(!removeRet[0].equals("true")) {
                    ret = false;
                }
            }
            clientList = null;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ret;
    }

    public boolean removeExistObject(Object key) {
        OkuyamaClient client = createClient();
        try {

            synchronized(delSync) {
                Object[] ret = client.getValue(type + "\t" + (String)key);
                if (!ret[0].equals("true")) {
                    return false;
                }
                client.removeValue(type + "\t" + (String)key);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                client.close();
            } catch (Exception ee) {
            }

        }
        return true;
    }

    public boolean containsKey(Object key) {
        //boolean retDumm =  dumm.containsKey(type + "\t" + (String)key);
        //if (retDumm == true) return true;
        OkuyamaClient client = createClient();
        try {

            Object[] ret = client.getValue(type + "\t" + (String)key);
            if (!ret[0].equals("true")) {
                return false;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                client.close();
            } catch (Exception ee) {
            }
        }
        return true;
    }
}

class ResponseCheckDaemon extends Thread {

    private ArrayBlockingQueue requestBox = new ArrayBlockingQueue(1);
    private ArrayBlockingQueue responseBox = new ArrayBlockingQueue(1);

    private OkuyamaClientFactory factory = null;
    private OkuyamaClient client = null;

    private boolean endFlg = false;

    public ResponseCheckDaemon(OkuyamaClientFactory factory) {
        this.factory = factory;
    }

    public void run() {

        while (true) {
            try {
                String key = null;
                while (true) {
                    key = (String)this.requestBox.poll(1000, TimeUnit.MILLISECONDS);
                    if (OkuyamaFilesystem.jvmShutdownStatus == true) break;
                    if (key != null) break;
                    
                }
                if (OkuyamaFilesystem.jvmShutdownStatus == true) break;

                if (this.endFlg) {

                    if (client != null ) {
                        client.close();
                        client = null;
                    }
                    break;
                }

                client = this.factory.getClient();
                Object[] responseSet = client.readByteValue(key);

                if (responseSet[0].equals("true")) {
                    Object[] retObj = new Object[2];
                    retObj[0] = key;
                    retObj[1] = responseSet[1];


                    this.responseBox.put(retObj);
                } else {

                    responseBox.put(new Object[0]);
                }
            } catch (Exception e) {
                e.printStackTrace();
                try {
                    if (client != null ) {
                        client.close();
                        client = null;
                    }
                } catch (Exception ee) {}

                if (this.endFlg) return;
                try {
                    responseBox.put(new Object[0]);
                } catch (Exception ee) {}
            } finally {
                try {
                    if (client != null ) {
                        client.close();
                        client = null;
                    }
                } catch (Exception ee) {}
            }
        }
    }

    public void putRequest(String key) throws Exception {
        try {
            this.requestBox.put(key);
        } catch (Exception ee) {
            throw ee;
        }
    }
    
    public Object[] takeResponse() {
        try {
            return (Object[])this.responseBox.take();
        } catch (Exception ee) {
            ee.printStackTrace();
            return new Object[0];
        }
    }

    public void endRequest() throws Exception {
        try {
            this.endFlg = true;
            this.requestBox.put("");
        } catch (Exception ee) {
            throw ee;
        }
    }
}


class RequestCheckDaemon extends Thread {

    private ArrayBlockingQueue requestBox = new ArrayBlockingQueue(1);
    private ArrayBlockingQueue responseBox = new ArrayBlockingQueue(1);

    private OkuyamaClientFactory factory = null;
    private OkuyamaClient client = null;
    private int clientUseCount = 0;
    private int maxClientUseCount = 5000;

    private boolean endFlg = false;

    public RequestCheckDaemon(OkuyamaClientFactory factory) {
        this.factory = factory;
    }

    public void run() {

        while (true) {
            try {
                Object[] request = null;
                while (true) {
                    request = (Object[])this.requestBox.poll(1000, TimeUnit.MILLISECONDS);
                    if (OkuyamaFilesystem.jvmShutdownStatus == true) break;
                    if (request != null) break;
                    
                }
                if (OkuyamaFilesystem.jvmShutdownStatus == true) break;

                if (this.endFlg) {

                    if (client != null ) {
                        client.close();
                        client = null;
                    }
                    break;
                }

                if (clientUseCount > maxClientUseCount) {
                    if (client != null ) {
                        client.close();
                        client = null;
                    }
                    clientUseCount = 0;
                }
                if (client == null) client = this.factory.getClient();

                clientUseCount++;
                //long start = System.nanoTime();
                boolean ret = client.sendByteValue((String)request[0], (byte[])request[1]);
                //long end = System.nanoTime();
                //System.out.println("put=" + (end - start) / 1000 / 1000);
                if (ret) {
                    this.responseBox.put(new Integer(0));
                } else {
                    this.responseBox.put(new Integer(1));
                }
            } catch (Exception e) {
                e.printStackTrace();
                try {
                    if (client != null ) {
                        client.close();
                        client = null;
                    }
                } catch (Exception ee) {}

                if (this.endFlg) return;
                try {
                    responseBox.put(new Object[0]);
                } catch (Exception ee) {}
            }
        }
    }

    public void putRequest(String key, byte[] value) throws Exception {
        try {
            Object[] request = new Object[2];
            request[0] = key;
            request[1] = value;
            this.requestBox.put(request);
        } catch (Exception ee) {
            throw ee;
        }
    }
    
    public Integer takeResponse() {
        try {
            return (Integer)this.responseBox.take();
        } catch (Exception ee) {
            ee.printStackTrace();
            return new Integer(1);
        }
    }

    public void endRequest() throws Exception {
        try {
            this.endFlg = true;
            this.requestBox.put("");
        } catch (Exception ee) {
            throw ee;
        }
    }
}

class PreFetchDaemon extends Thread {


    private ExpireCacheMap storeCache = null;
    private OkuyamaClientFactory factory =null;

    private ArrayBlockingQueue requestBox = new ArrayBlockingQueue(1);

    private ArrayBlockingQueue myPool =null;

    private boolean endFlg = false;

    private ResponseCheckDaemon[] dataReadDaemon = null;

    private ConcurrentHashMap nowPreFetchMarker = null;

    public PreFetchDaemon(ExpireCacheMap dataCache, OkuyamaClientFactory factory, ArrayBlockingQueue myPool, ConcurrentHashMap nowPreFetchMarker) {
        this.storeCache = dataCache;
        this.factory = factory;
        this.myPool = myPool;
        this.nowPreFetchMarker = nowPreFetchMarker;
        try {
            this.dataReadDaemon = new ResponseCheckDaemon[40];
            for (int idx = 0; idx < 40; idx++) {
                this.dataReadDaemon[idx] = new ResponseCheckDaemon(this.factory);
                this.dataReadDaemon[idx].start();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    
    public void run() {
        OkuyamaClient client = null;
        String[] startKeyIndexSplit = null;
        String dataKey = null;

        while (true) {
            try {
                String startKey = null;
                while (true) {
                    startKey = (String)this.requestBox.poll(1000, TimeUnit.MILLISECONDS);
                    if (OkuyamaFilesystem.jvmShutdownStatus == true) break;
                    if (startKey != null) break;
                }
                if (OkuyamaFilesystem.jvmShutdownStatus == true) break;

                if (this.endFlg) {
                    break;
                }


                startKeyIndexSplit = startKey.split("\t");

                dataKey = startKeyIndexSplit[0] + "\t" + startKeyIndexSplit[1];
                this.nowPreFetchMarker.put(dataKey, 1);

                long keyIdx = Long.parseLong(startKeyIndexSplit[2]);
                for (int idx = 0; idx < 10000; idx=idx+40) {

                    String[] getKeys = new String[40];
                    List requestSendGrpIdxList = new ArrayList(40);
                    Map tmp = new HashMap();

                    for (int grpIdx = 0; grpIdx < 40; grpIdx++) {
                        String preFetchRealKey = dataKey + "\t" + (keyIdx + 3 + idx + grpIdx);
                        if (!this.storeCache.containsKey(preFetchRealKey)) {

                            this.dataReadDaemon[grpIdx].putRequest(preFetchRealKey);
                            getKeys[grpIdx] = preFetchRealKey;
                            requestSendGrpIdxList.add(grpIdx);
                        }
                    }

                    boolean breakLoop = false;
                    int requestSendGrpIdxListSize = requestSendGrpIdxList.size();
                    boolean lastDataNull = false;

                    for (int grpIdx = 0; grpIdx < requestSendGrpIdxListSize; grpIdx++) {

                        int requestGrpIdx = ((Integer)requestSendGrpIdxList.get(grpIdx)).intValue();
                        if (getKeys[requestGrpIdx] != null) {
                            Object[] responseObj = this.dataReadDaemon[requestGrpIdx].takeResponse();
                            if (responseObj.length > 0) {

                                lastDataNull = false;
                                if (!this.storeCache.containsKey(getKeys[requestGrpIdx])) {

                                    this.storeCache.put(getKeys[requestGrpIdx], (byte[])responseObj[1]);
                                }
                            } else {
                                lastDataNull = true;
                            }
                        }
                    }

                    if (lastDataNull) break;
                }
                //long end = System.nanoTime();
                //System.out.println("Time=" + (end - start));
            } catch (Exception e) {
                e.printStackTrace();

                if (this.endFlg) return;
            } finally {
                try {
                    this.myPool.put(this);
                    if (startKeyIndexSplit != null && startKeyIndexSplit.length > 0) {

                        this.nowPreFetchMarker.remove(dataKey);
                    }
                } catch (Exception e2) {
                }
            }
        }

    }

    public void putRequest(String key) throws Exception {
        try {
            this.requestBox.put(key);
        } catch (Exception ee) {
            throw ee;
        }
    }

    public void endRequest() throws Exception {
        try {
            this.endFlg = true;
            this.requestBox.put("");
        } catch (Exception ee) {
            throw ee;
        }
    }
}
