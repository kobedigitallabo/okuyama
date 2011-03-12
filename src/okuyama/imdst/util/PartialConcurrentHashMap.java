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

    private static byte[] flg = new byte[1];

    public PartialConcurrentHashMap(int size, int upper, int multi, String[] bigValueStoreDirs) {
        super(size, upper, multi);

        if (bigValueStoreDirs != null) {
            fullMemory = false;
            String[] bigValueStoreDir = {bigValueStoreDirs[0] + "/partialBigData2/", bigValueStoreDirs[0] + "/partialBigData2/", bigValueStoreDirs[0] + "/partialBigData3/", bigValueStoreDirs[0] + "/partialBigData4/", bigValueStoreDirs[0] + "/partialBigData5/"};
            this.bigValueStoreMap = new FileBaseDataMap(bigValueStoreDir, 100000, 0.01, ImdstDefine.saveDataMaxSize, ImdstDefine.dataFileWriteMaxSize * 2);
        }
    }


    public Object put(Object key, Object value) {
        // 規定サイズを超える場合でかつ、VirtualStoreのディレクトリが指定してある場合はFileBaseMapに格納


        if (fullMemory == false && ((byte[])value).length > ImdstDefine.dataFileWriteMaxSize) {
            this.bigValueStoreMap.put(new String(((CoreMapKey)key).getDatas()), new String(BASE64EncoderStream.encode((byte[])value)));
            // 符号を登録
            super.put(key, PartialConcurrentHashMap.flg);
        } else {

            super.put(key, value);
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