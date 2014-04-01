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

    public long useStorageObjectTime = 0L;

    protected Map dataSizeMap = new ConcurrentHashMap(20, 16, 16);

    // メモリ救済用
    // メモリ領域が枯渇した場合に使用する仮想領域
    private boolean urgentSaveMode = false;
    private AbstractMap urgentSaveMap = null;
    private ICoreValueConverter urgentSaveMapConverter = null;
    private String[] virtualStoreDirs = null;
    private Object syncObj = new Object();

    // コンストラクタ
    public CoreValueMap(int size, int upper, int multi, boolean memoryMode, String[] virtualStoreDirs, boolean renewFlg, File bkupObjectDataFile) {
        System.out.println("Core Storage initialize start - " + new Date().toString());
        if (memoryMode) {

            if (!ImdstDefine.useSerializeMap) {

                if (ImdstDefine.saveValueCompress == true) {

                    // Value圧縮あり
                    System.out.println(" PartialConcurrentHashMap Use");
                    if (ImdstDefine.bigValueFileStoreUse == true && virtualStoreDirs != null) {
                        ImdstDefine.dataFullMemory = false;
                    }
                    if (renewFlg) {
                        mainMap = new PartialConcurrentHashMap(size, upper, multi, virtualStoreDirs);
                    } else {
                        File file = bkupObjectDataFile;
                        if (file != null && file.exists()) {
                            try {
                                FileInputStream fis = new FileInputStream(file);
                                ObjectInputStream ois = new ObjectInputStream(fis);
                                CoreStorageContainer container = (CoreStorageContainer)ois.readObject();
                                this.useStorageObjectTime = container.storeTime;
                                mainMap = (PartialConcurrentHashMap)container.storeObject;
                                this.dataSizeMap = container.dataSizeMap;
                            } catch(Exception e) {
                                e.printStackTrace();
                                mainMap = new PartialConcurrentHashMap(size, upper, multi, virtualStoreDirs);
                            }
                        } else {
                            mainMap = new PartialConcurrentHashMap(size, upper, multi, virtualStoreDirs);
                        }
                    }
                } else {

                    // Value圧縮なし
                    System.out.println(" ConcurrentHashMap Use");
                    if (renewFlg) {
                        mainMap = new NativeConcurrentHashMap(size, upper, multi);
                    } else {
                        File file = bkupObjectDataFile;
                        if (file != null && file.exists()) {
                            try {
                                FileInputStream fis = new FileInputStream(file);
                                ObjectInputStream ois = new ObjectInputStream(fis);
                                CoreStorageContainer container = (CoreStorageContainer)ois.readObject();
                                this.useStorageObjectTime = container.storeTime;
                                mainMap = (NativeConcurrentHashMap)container.storeObject;
                                this.dataSizeMap = container.dataSizeMap;
                            } catch(Exception e) {
                                e.printStackTrace();
                                mainMap = new NativeConcurrentHashMap(size, upper, multi);
                            }
                        } else {
                            mainMap = new NativeConcurrentHashMap(size, upper, multi);
                        }
                    }
                    // 圧縮なしのメモリ用のコンバータ
                    converter = new OriginalValueMemoryModeCoreValueCnv();
                }
            } else {

                long jvmMaxMemory = JavaSystemApi.getRuntimeMaxMem("M");
                long bucketSize = jvmMaxMemory * SerializeMap.bucketJvm1MBMemoryFactor;

                System.out.println(" PartialSerializeMap Use");
                if (ImdstDefine.bigValueFileStoreUse == true && virtualStoreDirs != null) {
                    ImdstDefine.dataFullMemory = false;
                }

                MemoryModeCoreValueCnv.compressUnderLimitSize = 1024 * 1024 * 1024;

                if (renewFlg) {
                    mainMap = new PartialSerializeMap(size, upper, new Long(bucketSize).intValue(), virtualStoreDirs);
                } else {
                    File file = bkupObjectDataFile;
                    if (file != null && file.exists()) {
                        try {
                            FileInputStream fis = new FileInputStream(file);
                            ObjectInputStream ois = new ObjectInputStream(fis);
                            CoreStorageContainer container = (CoreStorageContainer)ois.readObject();
                            this.useStorageObjectTime = container.storeTime;
                            mainMap = (PartialSerializeMap)container.storeObject;
                            this.dataSizeMap = container.dataSizeMap;
                        } catch(Exception e) {
                            mainMap = new PartialSerializeMap(size, upper, new Long(bucketSize).intValue(), virtualStoreDirs);
                        }
                    } else {
                        mainMap = new PartialSerializeMap(size, upper, new Long(bucketSize).intValue(), virtualStoreDirs);
                    }
                }
            }

            if (converter == null) converter = new MemoryModeCoreValueCnv();
            this.allDataMemory = true;
        } else {

            if (!ImdstDefine.useSerializeMap) {

                System.out.println(" ConcurrentHashMap Use");
                
                if (renewFlg) {
                    mainMap = new NativeConcurrentHashMap(size, upper, multi);
                } else {
                    File file = bkupObjectDataFile;
                    if (file != null && file.exists()) {
                        try {
                            FileInputStream fis = new FileInputStream(file);
                            ObjectInputStream ois = new ObjectInputStream(fis);
                            CoreStorageContainer container = (CoreStorageContainer)ois.readObject();
                            this.useStorageObjectTime = container.storeTime;
                            mainMap = (NativeConcurrentHashMap)container.storeObject;
                            this.dataSizeMap = container.dataSizeMap;
                        } catch(Exception e) {
                            mainMap = new NativeConcurrentHashMap(size, upper, multi);
                        }
                    } else {
                        mainMap = new NativeConcurrentHashMap(size, upper, multi);
                    }
                }
            } else {


                long jvmMaxMemory = JavaSystemApi.getRuntimeMaxMem("M");
                long bucketSize = jvmMaxMemory * SerializeMap.bucketJvm1MBMemoryFactor;

                System.out.println(" SerializeMap Use");

                if (renewFlg) {
                    mainMap = new SerializeMap(size, upper, new Long(bucketSize).intValue(), ImdstDefine.serializerClassName);
                } else {
                    File file = bkupObjectDataFile;
                    if (file != null && file.exists()) {
                        try {
                            FileInputStream fis = new FileInputStream(file);
                            ObjectInputStream ois = new ObjectInputStream(fis);
                            CoreStorageContainer container = (CoreStorageContainer)ois.readObject();
                            this.useStorageObjectTime = container.storeTime;
                            mainMap = (SerializeMap)container.storeObject;
                            this.dataSizeMap = container.dataSizeMap;
                        } catch(Exception e) {
                            mainMap = new SerializeMap(size, upper, new Long(bucketSize).intValue(), ImdstDefine.serializerClassName);
                        }
                    } else {
                        mainMap = new SerializeMap(size, upper, new Long(bucketSize).intValue(), ImdstDefine.serializerClassName);
                    }
                }
            }

            converter = new PartialFileModeCoreValueCnv();
        }

        this.virtualStoreDirs = virtualStoreDirs;
        if (useStorageObjectTime != 0L) {
            System.out.println(" ->Storage data is restored from backup");
            System.out.println(" ->Backup data create time = " + new Date(useStorageObjectTime).toString());
        }
        System.out.println("Core Storage initialize end   - " + new Date().toString());
    }


    // コンストラクタ
    public CoreValueMap(String[] dirs, int numberOfDataSize, boolean renewFlg) {

        mainMap = new FileBaseDataMap(dirs, numberOfDataSize, 0.2, 15, renewFlg);
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
                this.urgentSaveMap  = new FileBaseDataMap(this.virtualStoreDirs, 100000, 0.05, (new Double(ImdstDefine.saveDataMaxSize * 1.4).intValue() + 1), 512, 1024*124);
            } else {

                // 非memoryモードの場合をValueの位置をFileMapのValueに入れる
                this.urgentSaveMap  = new FileBaseDataMap(this.virtualStoreDirs, 10000000, 0.1, 15);
            }
            this.urgentSaveMode = true;
        }
        return true;
    }


    public void fileStoreMapObject(File file, Map dataSizeMap) throws Exception {
        try {
            String fileName = file.getAbsolutePath();
            File createFile = new File(fileName + ".tmp");

            FileOutputStream fos = new FileOutputStream(createFile, false);
            ObjectOutputStream oos = new ObjectOutputStream(fos);

            //System.out.println("  Execute - fileStoreMapObject - Start" + new Date());

            CoreStorageContainer container = new CoreStorageContainer();
            container.storeTime = System.currentTimeMillis();
            container.storeObject = (ICoreStorage)this.mainMap;
            container.dataSizeMap = dataSizeMap;

            oos.writeObject(container);
            //System.out.println("  Execute - fileStoreMapObject - End" + new Date());
            oos.close();

            if (file.exists()) {

                file.delete();
            }

            file = new File(fileName);
            if(!createFile.renameTo(file)) throw new Exception("Obj file create error");
        } catch(Exception e) {
            throw e;
        }
    }
}
