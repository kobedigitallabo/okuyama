package fuse.okuyamafs;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import okuyama.imdst.util.*;

/**
 * メモリ上のMapにて全てのデータを管理するコアのMap.<br>
 * OkuyamaFuse.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class NativeFsMap implements IFsMap {

    private int type = -1;

    private Object putSync = new Object();
    private Object delSync = new Object();

    private Map dataMap = null;

    private boolean complessMode = true;


    /**
     * コンストラクタ
     */
    public NativeFsMap(int type, String[] masterNodeInfos) {
        this.type = type;
        this.dataMap = new ConcurrentHashMap(1000000, 9800000, 64);

    }


    public boolean putNewString(Object key, String value) {
        synchronized(putSync) {
            if(this.dataMap.containsKey(key) == true) return false;
            this.dataMap.put(key, value);
        }
        return true;
    }

    public boolean putNewMap(Object key, Map value) {
        synchronized(putSync) {
            if(this.dataMap.containsKey(key) == true) return false;
            this.dataMap.put(key, value);
        }
        return true;
    }

    public boolean putNewBytes(Object key, byte[] value) {
        synchronized(putSync) {
            if(this.dataMap.containsKey(key) == true) return false;
            if (complessMode == true) {
                this.dataMap.put(key, SystemUtil.dataCompress(value));
            } else {
                this.dataMap.put(key, value);
            }
        }
        return true;
    }

    public Object putBytes(Object key, byte[] value) {
        if (complessMode == true) {

            return this.dataMap.put(key, SystemUtil.dataCompress(value));
        } else {
            this.dataMap.put(key, value);
            return null;
        }
    }

    public Object putMap(Object key, Map value) {
        return this.dataMap.put(key, value);
    }

    public Object putString(Object key, String value) {
        return this.dataMap.put(key, value);
    }


    public String getString(Object key) {
        return (String)this.dataMap.get(key);
    }

    public Map getMap(Object key) {
        return (Map)this.dataMap.get(key);
    }

    public byte[] getBytes(Object key) {
        byte[] ret = (byte[])this.dataMap.get(key);
        if (complessMode == true) {

            if (ret != null) return SystemUtil.dataDecompress(ret);
        } else {
            if (ret != null) return ret;
        }
        return null;
    }

    public Object remove(Object key) {
        return this.dataMap.remove(key);
    }

    public boolean removeExistObject(Object key) {
        synchronized(delSync) {
            if(this.dataMap.containsKey(key) == false) return false;
            this.dataMap.remove(key);
        }
        return true;
    }

    public boolean containsKey(Object key) {
        return this.dataMap.containsKey(key);
    }
}
