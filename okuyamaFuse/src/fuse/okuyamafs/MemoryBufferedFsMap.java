// a
package fuse.okuyamafs;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.*;

import okuyama.imdst.client.*;
import okuyama.imdst.util.*;

/**
 * OkuyamaFuse.<br>
 * okuyama上で全てのデータを管理するコアのMap.<br>
 * ！！1つのブロックを大きく取って、送る際に圧縮して送ることで速度が出る。!!
 * 
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class MemoryBufferedFsMap extends OkuyamaFsMap implements IFsMap  {

    public int type = -1;


    private String[] masterNodeList = null;

    private Object putSync = new Object();
    private Object delSync = new Object();

    public static int delayStoreDaemonSize = 60;
    public static int allDelaySJobSize = 100000;

    private ArrayBlockingQueue responseCheckDaemonQueue = null;

    private ArrayBlockingQueue requestCheckDaemonQueue = null;

    private Map testDataMap = new ConcurrentHashMap(50000);

    static boolean getCache = true;

    public OkuyamaClientFactory factory = null;

    private String prefix = "fd1";

    static {
        if (OkuyamaFilesystem.blockSize > (1024*24)) {
            // Fsが扱うデータがBlockサイズが24KBを超える場合はOnとなる
            OkuyamaFsMapUtil.setLargeDataMode(true);
        }
    }


    /**
     * コンストラクタ
     */
    public MemoryBufferedFsMap(int type, String[] masterNodeInfos) {
        super();
        this.type = type;
        this.masterNodeList = masterNodeInfos;
    }

    private void initData() {
//        String tagName = prefix "-" + type;

        // Okuyamaに格納されている全データを取得する
        
    }

    public OkuyamaClient createClient() {
        OkuyamaClient client = null;
        try {
            client = new BufferedOkuyamaClient(factory.getClient(300*1000));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return client;
    }


    public boolean putNewString(Object key, String value) {
        printDebug("1");
        //OkuyamaClient client = createClient();
        try {

            //Object[] setRet = client.setNewValue(type + "\t" + (String)key, value);
            testDataMap.put(type + "\t" + (String)key, value);
/*            if (!setRet[0].equals("true")) {
                return false;
            }*/

        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }

    public boolean putNewMap(Object key, Map value) {
        printDebug("2");
        try {
            testDataMap.put(type + "\t" + (String)key, value);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }        return true;
    }

    public boolean putNewBytes(Object key, byte[] value) {
        printDebug("3");
        try {

            synchronized(putSync) {
                Object a = testDataMap.get(type + "\t" + (String)key);
                if (a != null) return false;
                testDataMap.put(type + "\t" + (String)key, OkuyamaFsMapUtil.dataCompress(value));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }


    public Object putBytes(Object key, byte[] value) {
        printDebug("4");

        String keyStr = type + "\t" + (String)key;
        try {
            testDataMap.put(keyStr, OkuyamaFsMapUtil.dataCompress(value));
        } catch (Exception e) {}
        return null;
    }

    public Object putMultiBytes(Object[] dataList) {
        printDebug("4.5");

        try {
            List useDaemonList = new ArrayList();
            //long start = System.nanoTime();
            // 暫定実装
            for (int idx = 0; idx < dataList.length; idx++){
                Object[] putData = (Object[])dataList[idx];
                String keyStr = type + "\t" + (String)putData[0];
                byte[] data = (byte[])putData[1];
                testDataMap.put(keyStr, OkuyamaFsMapUtil.dataCompress(data));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }


    public Object putMap(Object key, Map value) {
        printDebug("5");
        try {

            testDataMap.put(type + "\t" + (String)key, value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }


    public Object putString(Object key, String value) {
        printDebug("5.5");
        try {

            String keyStr = type + "\t" + (String)key;
            testDataMap.put(keyStr, value);
        } catch (Exception e) {
            e.printStackTrace();
        }        return null;
    }


    public String getString(Object key) {
        printDebug("6");
        try {
            String keyStr = type + "\t" + (String)key;
            String cacheRetStr = (String)testDataMap.get(keyStr);
            return cacheRetStr ;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public Map getMap(Object key) {
        printDebug("7");
        try {
            Map ret = (Map)testDataMap.get(type + "\t" + (String)key);
            return ret;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public byte[] getBytes(Object key) {
        printDebug("8");
        String realKey = type + "\t" + (String)key;

        try {
            byte[] data = null;
            if (data == null) {
                data = (byte[])testDataMap.get(realKey);
                if (data == null) return null;
                return OkuyamaFsMapUtil.dataDecompress(data);
            } else {
                return data;
            }
            
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public Map getMultiBytes(Object[] keyList) {
        printDebug("9");
        Map retMap = new HashMap();
        Map okuyamaDataMap = new HashMap();

        try {
            List tmpKeyList = new ArrayList();
            List realTmpKeyList = new ArrayList();
            String[] keyStrList = null;
            
            for (int idx = 0; idx < keyList.length; idx++) {
                String key = type + "\t" + (String)keyList[idx];
                byte[] data = (byte[])testDataMap.get(key);

                if (data != null) {
                    retMap.put((String)keyList[idx], OkuyamaFsMapUtil.dataDecompress(data));
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        //long end = System.nanoTime();
        //printDebug("Key count=" + keyList.length + " Time=" + ((end - start) / 1000));
        return retMap;
    }

    public Object remove(Object key) {
        printDebug("10");
        try {
            String cnvKey = type + "\t" + (String)key;
            testDataMap.remove(cnvKey);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public boolean removeMulti(Object[] keyList) {
        printDebug("11");
        boolean ret = true;

        try {

            //List clientList = new ArrayList(20);
            for (int idx = 0; idx < keyList.length; idx++) {

                if (keyList[idx] != null) {

                    Object key = keyList[idx];

                    String removeKey = type + "\t" + (String)key;
                    Object rmRet = testDataMap.remove(removeKey);
                    if (rmRet == null) return false;
                    
/*                    String[] removeRet = client.removeValue(removeKey);
                    if(!removeRet[0].equals("true")) {
                        ret = false;
                    }*/
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return ret;
    }
/*
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
                    dataCache.remove(removeKey);
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
*/
    public boolean removeExistObject(Object key) {
        printDebug("12");
        //OkuyamaClient client = createClient();
        try {

            synchronized(delSync) {
                String cnvKey = type + "\t" + (String)key;
                /*Object[] ret = client.getValue(cnvKey);
                if (!ret[0].equals("true")) {
                    return false;
                }*/
                Object rmRet = testDataMap.remove(cnvKey);
                if (rmRet == null) return false;
/*
                client.removeValue(cnvKey);*/
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }

    public boolean containsKey(Object key) {
        printDebug("13");
        return testDataMap.containsKey(type + "\t" + (String)key);
    }

    private void printDebug(String str) {
        //System.out.println(new Date() + " " + str);
    }
}


