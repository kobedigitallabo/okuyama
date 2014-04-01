package fuse.okuyamafs;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.*;

import okuyama.imdst.client.*;
import okuyama.imdst.util.*;

/**
 * OkuyamaFuse.<br>
 * 
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class OkuyamaFsMapUtil  {


    public static boolean largeDataMode = false;

    public static int multiDataAccessDaemons = 50;
    public static int multiDataAccessDaemonsQueue = 130;

    public static int okuyamaClientPoolSize = 15000;
    public static int okuyamaFsMaxCacheTime = 100000; // ミリ秒 
    public static int okuyamaFsMaxCacheLimit = 100000; // キャッシュ数



    public static void setLargeDataMode(boolean type) {
        largeDataMode = type;
        multiDataAccessDaemons = 25;
        multiDataAccessDaemonsQueue = 50;
        okuyamaClientPoolSize = 50;
        okuyamaFsMaxCacheLimit = 250;
    }

    public static byte[] dataCompress(byte[] data) throws Exception {
        if (largeDataMode) return SystemUtil.dataCompress(data);
        return data;
    }

    public static byte[] dataDecompress(byte[] data) throws Exception {
        if (largeDataMode) return SystemUtil.dataDecompress(data);
        return data;
    }
}
