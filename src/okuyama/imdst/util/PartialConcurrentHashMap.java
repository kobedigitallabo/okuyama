package okuyama.imdst.util;


import java.util.*;
import java.io.*;
import java.util.concurrent.ConcurrentHashMap;

import okuyama.base.util.ILogger;
import okuyama.base.util.LoggerFactory;
import okuyama.base.lang.BatchException;
import okuyama.imdst.util.StatusUtil;

import com.sun.mail.util.BASE64DecoderStream;
import com.sun.mail.util.BASE64EncoderStream;

/**
 * ConcurrentHashMap拡張.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class PartialConcurrentHashMap extends ConcurrentHashMap implements Cloneable, Serializable {

    private boolean fullMemory = true;

    private FileBaseDataMap bigValueStoreMap = null;

    static byte[] flg = new byte[1];

    public PartialConcurrentHashMap(int size, int upper, int multi, String[] bigValueStoreDirs) {
        super(size, upper, multi);

        if (ImdstDefine.bigValueFileStoreUse == true && bigValueStoreDirs != null) {
            fullMemory = false;
            String[] bigValueStoreDir = {bigValueStoreDirs[0] + "/partialbigdata1/", 
                                         bigValueStoreDirs[0] + "/partialbigdata2/", 
                                         bigValueStoreDirs[0] + "/partialbigdata3/", 
                                         bigValueStoreDirs[0] + "/partialbigdata4/", 
                                         bigValueStoreDirs[0] + "/partialbigdata5/"};
            this.bigValueStoreMap = new FileBaseDataMap(bigValueStoreDir, 100000, 0.01, ImdstDefine.saveDataMaxSize, ImdstDefine.memoryStoreLimitSize*2, ImdstDefine.memoryStoreLimitSize*6);
        }

    }


    public Object put(Object key, Object value) {
        // 規定サイズを超える場合でかつ、VirtualStoreのディレクトリが指定してある場合はFileBaseMapに格納

        byte[] valueBytes = (byte[])value;
        //System.out.println("Partial= " + valueBytes.length);
        if (fullMemory == false && valueBytes.length > ImdstDefine.memoryStoreLimitSize) {

            this.bigValueStoreMap.put(new String(((CoreMapKey)key).getDatas()), new String(BASE64EncoderStream.encode(valueBytes)));
            // 符号を登録
            super.put(key, PartialConcurrentHashMap.flg);
        } else {

            super.put(key, valueBytes);
        }
        return null;
    }

    public Object get(Object key) {
        // 規定サイズを超える場合でかつ、VirtualStoreのディレクトリが指定してある場合はFileBaseMapに格納
        Object ret = super.get(key);
        if (ret == null || fullMemory == true || ((byte[])ret).length > 1) return ret;

        String valueStr = (String)this.bigValueStoreMap.get(new String(((CoreMapKey)key).getDatas()));

        if (valueStr == null) return null;

        byte[] retBytes = valueStr.getBytes();
        return BASE64DecoderStream.decode(retBytes);
    }

    public Object remove(Object key) {
        // 規定サイズを超える場合でかつ、VirtualStoreのディレクトリが指定してある場合はFileBaseMapに格納
        Object ret = super.remove(key);
        if (ret == null || fullMemory == true || ((byte[])ret).length > 1) return ret;

        String valueStr = (String)this.bigValueStoreMap.remove(new String(((CoreMapKey)key).getDatas()));

        if (valueStr == null) return null;

        byte[] retBytes = valueStr.getBytes();
        return BASE64DecoderStream.decode(retBytes);
    }

}