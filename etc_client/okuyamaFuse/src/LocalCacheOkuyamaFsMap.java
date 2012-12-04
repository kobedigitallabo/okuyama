package fuse.okuyamafs;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import okuyama.imdst.client.*;


/**
 * メモリ上のMapにて全てのデータを管理するコアのMap.<br>
 * OkuyamaFuse.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class LocalCacheOkuyamaFsMap implements IFsMap {


    private int type = -1;

    private String masterNodeInfo = null;

    private String[] masterNodeList = null;

    private Object putSync = new Object();
    private Object delSync = new Object();

    public static OkuyamaClientFactory factory = null;

    public CacheStoreDaemon daemon = null;


    public static Object daemonSync = new Object();

    public static OkuyamaFsMap okuyamaFs = null;


    /**
     * コンストラクタ
     */
    public LocalCacheOkuyamaFsMap(int type, String[] masterNodeInfos) {
        this.type = type;
        this.masterNodeList = masterNodeInfos;
        try {

            if (daemon == null) {

                daemon = new CacheStoreDaemon(200000, new OkuyamaFsMap(type, this.masterNodeList));
                daemon.start();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



    public boolean putNewString(Object key, String value) {
        synchronized(putSync) {
            if(daemon.containsKey(key) == true) return false;
                daemon.putString(key, value);
        }
        return true;
    }

    public boolean putNewMap(Object key, Map value) {
        synchronized(putSync) {
            if(daemon.containsKey(key) == true) return false;
                daemon.putMap(key, value);
        }
        return true;
    }

    public boolean putNewBytes(Object key, byte[] value) {
        synchronized(putSync) {
            if(daemon.containsKey(key) == true) return false;
                daemon.putBytes(key, value);
        }
        return true;
    }

    public Object putBytes(Object key, byte[] value) {
        return daemon.putBytes(key, value);
    }

    public Object putMap(Object key, Map value) {
        return daemon.putMap(key, value);
    }

    public Object putString(Object key, String value) {
        return daemon.putString(key, value);
    }


    public String getString(Object key) {
        return (String)daemon.getString(key);
    }

    public Map getMap(Object key) {
        return (Map)daemon.getMap(key);
    }

    public byte[] getBytes(Object key) {
        return (byte[])daemon.getBytes(key);
    }

    public Object remove(Object key) {
        return daemon.remove(key);
    }

    public boolean removeExistObject(Object key) {
        synchronized(delSync) {
            if(daemon.containsKey(key) == false) return false;
                daemon.remove(key);
        }
        return true;
    }

    public boolean containsKey(Object key) {
        return daemon.containsKey(key);
    }
}

