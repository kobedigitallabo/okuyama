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

import okuyama.imdst.util.serializemap.*;
/**
 * データ格納Map.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class CoreValueMap extends AbstractMap implements Cloneable, Serializable {

    private boolean fileWrite = false;

    private ICoreValueConverter converter = null;

    private AbstractMap mainMap = null;

    private boolean allDataMemory = false;



    // メモリ救済用
    // メモリ領域が枯渇した場合に使用する仮想領域
    private boolean urgentSaveMode = false;
    private AbstractMap urgentSaveMap = null;
    private ICoreValueConverter urgentSaveMapConverter = null;
    private String[] virtualStoreDirs = null;
    private Object syncObj = new Object();


    // コンストラクタ
    public CoreValueMap(int size, int upper, int multi, boolean memoryMode, String[] virtualStoreDirs) {

        if (memoryMode) {

            //mainMap  = new ConcurrentHashMap(size, upper, multi);
            if (!ImdstDefine.useSerializeMap) {

                System.out.println("PartialConcurrentHashMap Use");
                mainMap  = new PartialConcurrentHashMap(size, upper, multi, virtualStoreDirs);
            } else {

                long jvmMaxMemory = JavaSystemApi.getRuntimeMaxMem("M");
                long bucketSize = jvmMaxMemory * SerializeMap.bucketJvm1MBMemoryFactor;
                /*
                if (size > 100000000) {
                    multi = new Double(size * 0.1).intValue();
                } else if (size > 59999999) {
                    multi = 4000000;
                } else if (size > 19999999) {
                    multi = 2000000;
                } else if (size > 9999999) {
                    multi = 1000000;
                } else if (size > 5999999) {
                    multi = 600000;
                } else if (size > 2999999) {
                    multi = 400000;
                } else if (size > 999999) {
                    multi = 200000;
                } else if (size > 599999) {
                    multi = 100000;
                } else {
                    size = 200000;
                    upper =190000;
                    multi = 50000;
                }*/

                System.out.println("PartialSerializeMap Use");
                MemoryModeCoreValueCnv.compressUnderLimitSize = 1024 * 1024 * 1024;
                mainMap  = new PartialSerializeMap(size, upper, new Long(bucketSize).intValue(), virtualStoreDirs);
            }

            converter = new MemoryModeCoreValueCnv();
            this.allDataMemory = true;
        } else {

            //mainMap  = new ConcurrentHashMap(size, upper, multi);

            if (!ImdstDefine.useSerializeMap) {

                System.out.println("ConcurrentHashMap Use");
                mainMap  = new ConcurrentHashMap(size, upper, multi);
            } else {


                long jvmMaxMemory = JavaSystemApi.getRuntimeMaxMem("M");
                long bucketSize = jvmMaxMemory * SerializeMap.bucketJvm1MBMemoryFactor;
/*
                if (size > 100000000) {
                    multi = new Double(size * 0.1).intValue();
                } else if (size > 59999999) {
                    multi = 4000000;
                } else if (size > 19999999) {
                    multi = 2000000;
                } else if (size > 9999999) {
                    multi = 1000000;
                } else if (size > 5999999) {
                    multi = 600000;
                } else if (size > 2999999) {
                    multi = 400000;
                } else if (size > 999999) {
                    multi = 200000;
                } else if (size > 599999) {
                    multi = 100000;
                } else {

                    size = 200000;
                    upper =190000;
                    multi = 50000;
                }
*/
                System.out.println("SerializeMap Use");
                mainMap = new SerializeMap(size, upper, new Long(bucketSize).intValue(), ImdstDefine.serializerClassName);
            }

            converter = new PartialFileModeCoreValueCnv();
        }

        this.virtualStoreDirs = virtualStoreDirs;
    }


    // コンストラクタ
    public CoreValueMap(String[] dirs, int numberOfDataSize, boolean renewFlg) {

        mainMap  = new FileBaseDataMap(dirs, numberOfDataSize, 0.2, 15, renewFlg);
        converter = new AllFileModeCoreValueCnv();
    }


    /**
     * set<br>
     *
     * @param key
     * @param value
     */
    public Object put(Object key, Object value) {

        // メモリファイル共有Mapモードが起動しているかを確認
        if (!this.isUrgentSaveMode()) {
            return mainMap.put(converter.convertEncodeKey(key), converter.convertEncodeValue(value));
        } else {
            // メモリファイル共有Mapモードへ移行している
            if (mainMap.containsKey(converter.convertEncodeKey(key))) {

                // 既存ストレージ
                return mainMap.put(converter.convertEncodeKey(key), converter.convertEncodeValue(value));
            } else {

                // メモリファイル共有Map
                //System.out.println("Virtual[ " + ((String)value).length());
                return urgentSaveMap.put(urgentSaveMapConverter.convertEncodeKey(key), urgentSaveMapConverter.convertEncodeValue(value));
            }
        }
    }


    /**
     * get<br>
     *
     * @param key
     * @return Object
     */
    public Object get(Object key) {

        // メモリファイル共有Mapモードが起動しているかを確認
        if (!this.isUrgentSaveMode()) {
            return converter.convertDecodeValue(mainMap.get(converter.convertEncodeKey(key)));
        } else {

            // メモリファイル共有Mapモードへ移行している
            if (mainMap.containsKey(converter.convertEncodeKey(key))) {

                // 既存ストレージ
                return converter.convertDecodeValue(mainMap.get(converter.convertEncodeKey(key)));
            } else {

                // メモリファイル共有Map
                return urgentSaveMapConverter.convertDecodeValue(urgentSaveMap.get(urgentSaveMapConverter.convertEncodeKey(key)));
            }
        }
    }


    /**
     * remove<br>
     *
     * @param key
     * @return Object
     */
    public Object remove(Object key) {
        // メモリファイル共有Mapモードが起動しているかを確認
        if (!this.isUrgentSaveMode()) {
            return converter.convertDecodeValue(mainMap.remove(converter.convertEncodeKey(key)));
        } else {
            // メモリファイル共有Mapモードへ移行している
            if (mainMap.containsKey(converter.convertEncodeKey(key))) {

                // 既存ストレージ
                return converter.convertDecodeValue(mainMap.remove(converter.convertEncodeKey(key)));
            } else {

                // メモリファイル共有Map
                return urgentSaveMapConverter.convertDecodeValue(urgentSaveMap.remove(urgentSaveMapConverter.convertEncodeKey(key)));
            }
        }
    }


    /**
     * containsKey<br>
     *
     * @param key
     * @return boolean
     */
    public boolean containsKey(Object key) {

        // メモリファイル共有Mapモードが起動しているかを確認
        if (!this.isUrgentSaveMode()) {
            return mainMap.containsKey(converter.convertEncodeKey(key));
        } else {

            // メモリファイル共有Mapモードへ移行している
            if (mainMap.containsKey(converter.convertEncodeKey(key))) {
                return true;
            } else {

                return urgentSaveMap.containsKey(urgentSaveMapConverter.convertEncodeKey(key));
            }
        }
    }


    /**
     * clear<br>
     *
     */
    public void clear() {
        this.mainMap.clear();

        // メモリファイル共有Mapモードが起動しているかを確認
        if (this.isUrgentSaveMode()) 
            urgentSaveMap.clear();
    }


    /**
     * size.<br>
     *
     * @param
     * @return int
     * @throws
     */
    public int size() {

        // メモリファイル共有Mapモードが起動しているかを確認
        if (!this.isUrgentSaveMode()) {
            return this.mainMap.size();
        } else {
            int retSize = this.mainMap.size();
            retSize = retSize + urgentSaveMap.size();
            return retSize;
        }
    }
    
    
    /**
     * entrySet<br>
     *
     * @return Set
     */
    public Set entrySet() {

        // メモリファイル共有Mapモードが起動しているかを確認
        if (!this.isUrgentSaveMode()) {
            return new CoreValueMapSet(mainMap.entrySet(), converter);
        } else {
            return new CoreValueMapSet(mainMap.entrySet(), urgentSaveMap.entrySet(), converter, urgentSaveMapConverter);
        }
    }


    /**
     * isUseMemoryLimitOver
     */
    private boolean isUrgentSaveMode() {
        if (this.virtualStoreDirs == null) return false;
        if (this.urgentSaveMode) return true;
        if (!StatusUtil.isUseMemoryLimitOver()) return false;

        if (this.urgentSaveMap != null) return true;

        synchronized (this.syncObj) {

            if (this.urgentSaveMap != null) return true;

            this.urgentSaveMapConverter = new AllFileModeCoreValueCnv();

            // memoryモード判定
            if (this.allDataMemory) {

                // memoryモードの場合はリアルValueをFileMapのValueに入れる
                this.urgentSaveMap  = new FileBaseDataMap(this.virtualStoreDirs, 100000, 0.05, (new Double(ImdstDefine.saveDataMaxSize * 1.38).intValue() + 1), 512, 1024*124);
            } else {

                // 非memoryモードの場合をValueの位置をFileMapのValueに入れる
                this.urgentSaveMap  = new FileBaseDataMap(this.virtualStoreDirs, 10000000, 0.1, 15);
            }
            this.urgentSaveMode = true;
        }
        return true;
    }
}
