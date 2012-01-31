package okuyama.imdst.util;


import java.util.*;
import java.io.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;

import okuyama.base.util.ILogger;
import okuyama.base.util.LoggerFactory;
import okuyama.base.lang.BatchException;
import okuyama.imdst.util.StatusUtil;
import okuyama.imdst.util.io.*;


import org.apache.commons.codec.digest.DigestUtils;


/**
 * KeyとValueを管理する独自Mapクラス.<br>
 * メモリモードとファイルモードで動きが異なる.<br>
 * メモリモード:KeyとValueを親クラスであるHashMapで管理する.<br>
 * ファイルモード:Keyは親クラスのMapに、Valueはファイルに記録する<br>
 *                KeyとValueが格納させている行数を記録している.<br>
 *                行数から、ファイル内からValueを取り出す.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class KeyManagerValueMap extends CoreValueMap implements Cloneable, Serializable {

    // 完全にメモリのみでKeyとValueを管理する場合のみtrue
    public boolean memoryMode = true;

    // 完全にディスクのみでKeyとValueを管理する場合のみtrue
    private boolean fullDiskMode = false;


    private transient BufferedWriter bw = null;
    private transient AtomicInteger dataFileBufferUseCount = null;
    private transient AbstractDataRandomAccess raf = null;

    private transient FileBaseDataMap overSizeDataStore = null;

    private transient Object sync = new Object();

    private transient boolean vacuumExecFlg = false;
    private transient List vacuumDiffDataList = null;

    private ConcurrentHashMap dataSizeMap = new ConcurrentHashMap(20, 16, 16);
    private ArrayBlockingQueue deletedDataPointList = null;

    private String lineFile = null;
    private String tmpVacuumeLineFile = null;
    private String[] tmpVacuumeCopyMapDirs = null;

    private int lineCount = 0;
    private int oneDataLength = ImdstDefine.dataFileWriteMaxSize;
    private int seekOneDataLength = ImdstDefine.dataFileWriteMaxSize + 1;
    private long lastDataChangeTime = 0L;
    private int nowKeySize = 0;

    private transient boolean readObjectFlg = false;

    private boolean mapValueInSize = false;


    // コンストラクタ
    public KeyManagerValueMap(int size, boolean memoryMode, String[] virtualStoreDirs, boolean renewFlg, File bkupObjFile) {
        super(size, new Double(size * 0.9).intValue(), 512, memoryMode, virtualStoreDirs, renewFlg, bkupObjFile);

        this.memoryMode = memoryMode;
        if (!this.memoryMode) this.mapValueInSize = true;
    }


    // コンストラクタ
    public KeyManagerValueMap(String[] dirs, int numberOfDataSize, boolean renewFlg) {
        super(dirs, numberOfDataSize, renewFlg);
        this.memoryMode = false;
        this.fullDiskMode = true;
    }


    /**
     * 本メソッドは使用前に必ず呼び出す<br>
     * Objectに書き出した後でも必須
     */
    public void initNoMemoryModeSetting(String lineFile) {
        try {
            if (sync == null) 
                sync = new Object();


            readObjectFlg  = true;

            this.tmpVacuumeLineFile = lineFile + ".vacuumtmp";
            this.tmpVacuumeCopyMapDirs = new String[5];
            this.tmpVacuumeCopyMapDirs[0] = lineFile + ".cpmapdir1/";
            this.tmpVacuumeCopyMapDirs[1] = lineFile + ".cpmapdir2/";
            this.tmpVacuumeCopyMapDirs[2] = lineFile + ".cpmapdir3/";
            this.tmpVacuumeCopyMapDirs[3] = lineFile + ".cpmapdir4/";
            this.tmpVacuumeCopyMapDirs[4] = lineFile + ".cpmapdir5/";

            // 共有データファイルサイズオーバーのValueを格納するMapを初期化
            String[] overSizeDataStoreDirs = new String[1];
            for (int dirIdx = 0; dirIdx < 1; dirIdx++) {
                overSizeDataStoreDirs[dirIdx] = lineFile + "_" + dirIdx + "/";
            }

            if (this.overSizeDataStore == null)
                this.overSizeDataStore = new FileBaseDataMap(overSizeDataStoreDirs, 100000, 0.01, ImdstDefine.saveDataMaxSize, ImdstDefine.dataFileWriteMaxSize*5, ImdstDefine.dataFileWriteMaxSize*15);


            // データ操作記録ファイル用のBufferedWriter
            this.bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(lineFile), true), ImdstDefine.keyWorkFileEncoding), 1024*256);
            this.dataFileBufferUseCount = new AtomicInteger(0);

            // 共有データファイルの再書き込み遅延指定
            if (ImdstDefine.dataFileWriteDelayFlg) {
                // 遅延あり
                this.raf = new CustomRandomAccess(new File(lineFile) , "rw");
            } else {
                // 遅延なし
                //this.raf = new RandomAccessFile(new File(lineFile) , "rw");
                this.raf = new SortedSchedulingRandomAccess(new File(lineFile) , "rw");
            }
            // 自身のインスタンスをファイルアクセッサに渡す
            this.raf.setDataPointMap(this);
            
            // 削除済みデータ位置保持領域構築
            this.deletedDataPointList = new ArrayBlockingQueue(ImdstDefine.numberOfDeletedDataPoint);

            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(lineFile)) , ImdstDefine.keyWorkFileEncoding));
            this.lineFile = lineFile;
            int counter = 0;

            // 現在のファイルの終端を探す
            // 終端を探すまでに壊れてしまっているデータは無効データ(ブランクデータ)に置き換える
            String readDataLine = null;
            while((readDataLine = br.readLine()) != null){
                if (!readDataLine.trim().equals("")) {
                    counter++;
                    if (readDataLine.getBytes().length < this.oneDataLength) {
                        int shiftByteSize = 0;
                        if (readDataLine.length() < "(B)!0".length()) {
                            int shift = "(B)!0".length() - readDataLine.length();
                            shiftByteSize = shift; 
                        }
                        readDataLine = "(B)!0";
                        StringBuilder updateBuf = new StringBuilder(readDataLine);
                        for (int i = 0; i < (this.oneDataLength - readDataLine.length()); i++) {
                            updateBuf.append("&");
                            shiftByteSize++;
                        }
                        updateBuf.append("\n");
                        shiftByteSize++;

                        this.raf.seek(this.convertLineToSeekPoint(counter));
                        this.raf.write(updateBuf.toString().getBytes(), 0, this.oneDataLength+1);
                        for (int i = 0; i < shiftByteSize; i++) {
                            br.read();
                        }
                    }
                }
            }

            this.lineCount = counter;
            br.close();

            // 現在のサイズ格納
            this.nowKeySize = super.size();
        } catch(Exception e) {
            e.printStackTrace();
            // 致命的
            StatusUtil.setStatusAndMessage(1, "KeyManagerValueMap - init - Error [" + e.getMessage() + "]");
        }
    }


    /**
     * データを無加工で取り出す.<br>
     * 無加工とは、valueをファイルで保持する場合はpadding文字列をが付加されているが、それを削除せずに返す.<br>
     *
     * @param key
     * @return Object
     * @throw
     */
    public Object getNoCnv(Object key) {
        Object ret = null;

        if (this.memoryMode) {
            ret = super.get(key);
        } else {
            try {

                int i = 0;
                long seekPoint = 0L;
                byte[] buf = new byte[this.oneDataLength];

                // seek値取得
                if ((seekPoint = this.calcSeekDataPoint(key)) == -1) return null;

                synchronized (sync) {
                    // Vacuum中の場合はデータの格納先が変更されている可能性があるので、
                    // ここでチェック
                    if (vacuumExecFlg) {
                        if(seekPoint != this.calcSeekDataPoint(key)) {
                            // 再起呼び出し
                            return get(key);
                        }
                    }

                    this.readDataFile(buf, seekPoint, this.oneDataLength, key);
                }

                ret = new String(buf, ImdstDefine.keyWorkFileEncoding);
            } catch (Exception e) {
                e.printStackTrace();
                // 致命的
                StatusUtil.setStatusAndMessage(1, "KeyManagerValueMap - get - Error [" + e.getMessage() + "]");
                
            }
        }
        return ret;
    }

    /**
     * getをオーバーライド.<br>
     * MemoryモードとFileモードで取得方法が異なる.<br>
     *
     * @param key 登録kye値(全てStringとなる)
     * @return Object 返却値(全てStringとなる)
     */
    public Object get(Object key) {
        Object ret = null;
        if (this.memoryMode) {
            ret = super.get(key);
        } else {
            try {

                // Vacuum中はsyncを呼び出す
                if (vacuumExecFlg) {
                    ret = syncGet(key);
                } else {

                    int i = 0;
                    int readRet = 0;
                    long seekPoint = 0L;
                    byte[] buf = new byte[this.oneDataLength];

                    // seek値取得
                    if ((seekPoint = this.calcSeekDataPoint(key)) == -1) {

                        return null;
                    }

                    synchronized (sync) {
                        readRet = this.readDataFile(buf, seekPoint, this.oneDataLength, key);
                        if (readRet == -1) {

                            return null;
                        }

                        boolean overSizeData = false;
                        if (buf[this.oneDataLength -1] != 38 || readRet > this.oneDataLength) {

                            overSizeData = true;
                            if (!overSizeDataStore.containsKey(key)) {

                                // 共有ファイルの上限と同じ長さの可能性がある
                                overSizeData = false;
                            }
                        }

                        if (overSizeData) {
                            // データ長が共有データファイルの1データ上限を超えている
                            // その場合はキー名でファイルが存在するはず
                            ret = this.readOverSizeData(key, buf);
                        } else {

                            for (; i < buf.length; i++) {
                                if (buf[i] == 38) break;
                            }
                            ret = new String(buf, 0, i, ImdstDefine.keyWorkFileEncoding);
                        }
                        buf = null;
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                // 致命的
                StatusUtil.setStatusAndMessage(1, "KeyManagerValueMap - get - Error [" + e.getMessage() + "]");
            }
        }

        return ret;
    }


    // Vacuum中に使用するsynchronizedしながらGetする
    private Object syncGet(Object key) {

        Object ret = null;

        try{
            int i = 0;
            byte[] buf = new byte[this.oneDataLength];
            long seekPoint = 0L;
            int readRet = 0;

            synchronized (sync) {

                // Vacuum中はMap内の行数指定と実際のデータファイルでの位置が異なる場合があるため、
                // Vacuum中で且つ、Mapを更新中の場合はここで同期化する。
                // seek値取得
                if ((seekPoint = this.calcSeekDataPoint(key)) == -1) return null;

                readRet = this.readDataFile(buf, seekPoint, this.oneDataLength, key);

                boolean overSizeData = false;
                if (buf[this.oneDataLength -1] != 38 || readRet > this.oneDataLength) {
                    overSizeData = true;
                    if (!overSizeDataStore.containsKey(key)) {
                        // 共有ファイルの上限と同じ長さの可能性がある
                        overSizeData = false;
                    }
                }

                if (overSizeData) {

                    // データ長が共有データファイルの1データ上限を超えている
                    // その場合はキー名でファイルが存在するはず
                    ret = this.readOverSizeData(key, buf);
                } else {

                    for (; i < buf.length; i++) {
                        if (buf[i] == 38) break;
                    }
                    ret = new String(buf, 0, i, ImdstDefine.keyWorkFileEncoding);
                }

                buf = null;
            }
        } catch (Exception e) {
            e.printStackTrace();
            // 致命的
            StatusUtil.setStatusAndMessage(1, "KeyManagerValueMap - syncGet - Error [" + e.getMessage() + "]");
            
        }
        return ret;
    }


    /**
     * Valueがファイルにある場合の位置を取得する
     *
     */
    public long dataPointGet(Object key) {
        long ret = -1;
        try {
            ret = this.calcSeekDataPoint(key, false);
        } catch (Exception e) {
            e.printStackTrace();
            // 致命的
            StatusUtil.setStatusAndMessage(1, "KeyManagerValueMap - dataPointGet - Error [" + e.getMessage() + "]");
            
        }
        return ret;
    }

    /**
     * putをオーバーライド.<br>
     * MemoryモードとFileモードで保存方法が異なる.<br>
     *
     * @param key 登録kye値(全てStringとなる)
     * @param value 登録value値(全てStringとなる)
     * @return Object 返却値(Fileモード時は返却値は常にnull)
     */
    public Object put(Object key, Object value) {

        Object ret = null;

        this.totalDataSizeCalc(key, value);

        if (this.memoryMode) {
            ret = super.put(key, value);
        } else {

            StringBuilder writeBuf = new StringBuilder(this.oneDataLength + 2);
            int valueSize = (value.toString()).length();
            int realValueSize = valueSize;
            try {

                if (readObjectFlg == true) {

                    long seekPoint = 0L;
                    boolean overSizeFlg = false;


                    if (((String)value).length() > this.oneDataLength) {
                        writeBuf.append(((String)value).substring(0, (this.oneDataLength)));
                        overSizeFlg = true;
                        valueSize = this.oneDataLength;
                    } else {
                        writeBuf.append((String)value);
                    }

                    // 渡されたデータが固定の長さ分ない場合は足りない部分を補う
                    // 足りない文字列は固定の"&"で補う(38)
                    byte[] appendDatas = new byte[this.oneDataLength - valueSize];

                    for (int i = 0; i < appendDatas.length; i++) {
                        appendDatas[i] = 38;
                    }

                    writeBuf.append(new String(appendDatas));
                    writeBuf.append("\n");


                    if ((this.fullDiskMode == true && ImdstDefine.reuseDataFileValuePositionFlg == false) || (seekPoint = this.calcSeekDataPoint(key, false)) == -1) {

                        // まだ存在しないデータ
                        // 書き込む行を決定
                        synchronized (sync) {

                            // 削除済みデータが使用していた場所をまずは調べる
                            Integer deletedLine = null;
                            if (mapValueInSize) {
                                String deletedLineStr = (String)this.deletedDataPointList.poll();
                                if (deletedLineStr != null) {
                                    deletedLine = new Integer(((String[])deletedLineStr.split(":"))[0]);
                                }
                            } else {
                                deletedLine = (Integer)this.deletedDataPointList.poll();
                            }

                            if (vacuumExecFlg) {
                                // Vacuum差分にデータを登録
                                Object[] diffObj = {"1", (String)key, (String)value};
                                this.vacuumDiffDataList.add(diffObj);
                            }

                            if (deletedLine == null) {

                                this.bw.write(writeBuf.toString());
                                SystemUtil.diskAccessSync(this.bw, false);

                                this.lineCount++;

                                if (mapValueInSize) {
                                    super.put(key, new Integer(this.lineCount) + ":" + realValueSize);
                                } else {
                                    super.put(key, new Integer(this.lineCount));
                                }

                                this.checkDataFileWriterLimit(this.dataFileBufferUseCount.incrementAndGet());

                            } else {

                                // 削除済みデータの場所を再利用する
                                raf.seek(this.convertLineToSeekPoint(deletedLine));
                                raf.write(writeBuf.toString().getBytes(), 0, this.oneDataLength);


                                if (mapValueInSize) {
                                    super.put(key, new Integer(deletedLine) + ":" + realValueSize);
                                } else {
                                    super.put(key, new Integer(deletedLine));
                                }
                            }

                            this.nowKeySize = super.size();
                            // サイズオーバーの場合
                            if (overSizeFlg) {

                                // データ長が共有データファイルの1データ上限を超えている
                                this.writeOverSizeData(key, value);

                            }

                        }
                    } else {

                        // すでにファイル上に存在する
                        synchronized (sync) {

                            if (vacuumExecFlg) {
                                // Vacuum差分にデータを登録
                                Object[] diffObj = {"1", key, value};
                                this.vacuumDiffDataList.add(diffObj);
                            }

                            if (raf != null) {

                                raf.seek(seekPoint);
                                raf.write(writeBuf.toString().getBytes(), 0, this.oneDataLength);
                            }
                            // サイズオーバーの場合
                            if (overSizeFlg) {
                                // データ長が共有データファイルの1データ上限を超えている
                                // その場合はキー名でファイルを作成する
                                this.writeOverSizeData(key, value);
                            }
                        }
                    }
                } else {

                    if (mapValueInSize) {
                        super.put(key, value + ":" + realValueSize);
                    } else {
                        super.put(key, value);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                // 致命的
                StatusUtil.setStatusAndMessage(1, "KeyManagerValueMap - put - Error [" + e.getMessage() + "]");
            }
        }
        return ret;
    }



    /**
     * removeをオーバーライド.<br>
     * MemoryモードとFileモード両方で同じ動きをする.<br>
     *
     * @param key 削除kye値(全てStringとなる)
     * @return Object 返却値
     */
    public Object remove(Object key) {
        Object ret = null;

        synchronized (sync) {
            this.totalDataSizeCalc(key, null);
            ret = super.remove(key);
            if(this.overSizeDataStore != null && this.overSizeDataStore.containsKey(key)) {
                this.overSizeDataStore.remove(key);
            } 

            this.nowKeySize = super.size();

            // 再利用可能なデータの場所を保持(ValueをFileに保存している場合のみ)
            if(ret != null) {
                if (!this.memoryMode) 
                    this.deletedDataPointList.offer(ret);
            }


            if (vacuumExecFlg) {

                Object diffObj[] = {"2", (String)key};
                this.vacuumDiffDataList.add(diffObj);
            }
        }

        return ret;
    }

    /**
     * containsKeyをオーバーライド.<br>
     * MemoryモードとFileモード両方で同じ動きをする.<br>
     *
     * @param key 登録kye値(全てStringとなる)
     * @return boolean 返却値
     */
    public boolean containsKey(Object key) {
        if (vacuumExecFlg) {
            synchronized (sync) {
                return super.containsKey(key);
            }
        }

        return super.containsKey(key);
    }


    private void totalDataSizeCalc(Object key, Object value) {
        if (!ImdstDefine.calcSizeFlg) return;

        long addSize = 0L;

        if (value != null) addSize = new Double((((String)key).length() + ((String)value).length()) * 0.8).longValue();

        if (addSize != 0L)
            addSize = addSize + 20;

        String unique = null;
        String keyStr = (String)key;
        int beforeSize = 0;
        AtomicLong size = null;
        int nowValLen = 0;


       if(keyStr.indexOf("#") == 0) {

            unique = keyStr.substring(0, 6);
        } else {
            unique = "all";
        }

        if (mapValueInSize) {
            String val = (String)super.get(key);

            if (val != null) {
                nowValLen = new Double((((String)key).length() + new Integer(((String[])val.split(":"))[1]).intValue()) * 0.8).intValue() + 20;
            }
        } else {

            Object val = this.get(key);

            if (val != null) {
                nowValLen = new Double((((String)key).length() + ((String)val).length()) * 0.8).intValue() + 20;
            }
        }
        if (nowValLen != 0) {
            beforeSize = nowValLen * -1;
        }


        if(!dataSizeMap.containsKey(unique)) {
            size = new AtomicLong(0L);
            dataSizeMap.put(unique, size);
        } else {
            size = (AtomicLong)dataSizeMap.get(unique);
        }

        size.getAndAdd(beforeSize);
        size.getAndAdd(addSize);
    }


    public long getDataUseSize(String unique) {

        AtomicLong size = new AtomicLong(0L);

        if (unique == null) unique = "all";

        if(dataSizeMap.containsKey(unique)) {

            size = (AtomicLong)dataSizeMap.get(unique);
        }

        return size.longValue();
    }


    public String[] getAllDataUseSize() {

        if (dataSizeMap == null || dataSizeMap.size() == 0) return null;

        String[] sizeList = new String[dataSizeMap.size()];
        Set entrySet = dataSizeMap.entrySet();
        Iterator entryIte = entrySet.iterator(); 
        int idx = 0;
        while(entryIte.hasNext()) {

            Map.Entry obj = (Map.Entry)entryIte.next();
            if (obj == null) continue;

            String key = (String)obj.getKey();
            AtomicLong size = (AtomicLong)obj.getValue();

            sizeList[idx] = key + "=" + size.toString();
            idx++;
        }

        return sizeList;
    }


    /**
     * データファイルの不要領域を掃除して新たなファイルを作りなおす.<br>
     * 方法はMapを一時Mapをつくり出して、現在のsuperクラスのMapから全てのKeyを取り出し、<br>
     * そのキー値を使用してvalueを自身から取得し、新しいDataファイルに書き出すと同時に、<br>
     * 一時MapにKeyと書き込んだファイル内の行数をputする.<br>
     * 全てのKeyの取得が終わったタイミングで新しいファイルには、フラグメントはないので、<br>
     * 次に、同期を実施し同期中に、superのMapをクリアし、一時Mapの内容をsuperのMapにputする.<br>
     * これでsuperのMapはフラグメントの存在しない新しいファイルの行数を保持できているので、
     * あとは元のデータファイルを削除し、新しく作成したフラグメントのないファイルをデータファイル名に
     * リネームすれば、Vacuum完了.<br>
     */
    public boolean vacuumData() {
        boolean ret = false;


        BufferedWriter tmpBw = null;
        RandomAccessFile raf = null;
        Map vacuumWorkMap = null;
        boolean userMap = false;
        String dataStr = null;
        Set entrySet = null;
        Iterator entryIte = null;
        String key = null;
        int putCounter = 0;


        synchronized (sync) {

            if (this.vacuumDiffDataList != null) {
                this.vacuumDiffDataList.clear();
                this.vacuumDiffDataList = null;
            }

            this.vacuumDiffDataList = new FileBaseDataList(this.tmpVacuumeLineFile);
            vacuumExecFlg = true;
        }

        //vacuumWorkMap = new ConcurrentHashMap(super.size());
        if (JavaSystemApi.getUseMemoryPercent() > 40) {
            userMap = true;
            vacuumWorkMap = new FileBaseDataMap(this.tmpVacuumeCopyMapDirs, super.size(), 0.20);
        } else {
            vacuumWorkMap = new HashMap(super.size());
        }

        try {

            tmpBw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(this.lineFile + ".tmp"), true), ImdstDefine.keyWorkFileEncoding), 1024*256);
            raf = new RandomAccessFile(new File(this.lineFile) , "r");

            entrySet = super.entrySet();
            entryIte = entrySet.iterator();

            while(entryIte.hasNext()) {

                Map.Entry obj = (Map.Entry)entryIte.next();
                key = (String)obj.getKey();
                if (key != null && (dataStr = (String)getNoCnv(key)) != null) {
                    tmpBw.write(dataStr);
                    tmpBw.write("\n");
                    putCounter++;
                    
                    if (mapValueInSize) {
                        vacuumWorkMap.put(key, new Integer(putCounter).toString() + ":" +  dataStr.length());
                    } else {
                        vacuumWorkMap.put(key, new Integer(putCounter).toString());
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            // 致命的
            StatusUtil.setStatusAndMessage(1, "KeyManagerValueMap - vacuumData - Error [" + e.getMessage() + "]");
        } finally {
            try {
                // 正常終了の場合のみ、ファイルを変更
                if (StatusUtil.getStatus() == 0)  {

                    // 新ファイルflush
                    SystemUtil.diskAccessSync(tmpBw);
                    // 新ファイルclose
                    tmpBw.close();

                    // 新ファイルに置き換え
                    synchronized (sync) {

                        raf.close();

                        if(this.raf != null) this.raf.close();
                        if(this.bw != null) this.bw.close();

                        File dataFile = new File(this.lineFile);
                        if (dataFile.exists()) {
                            dataFile.delete();
                        }
                        dataFile = null;
                        // 一時KeyMapファイルをKeyMapファイル名に変更
                        File tmpFile = new File(this.lineFile + ".tmp");
                        tmpFile.renameTo(new File(this.lineFile));

                        // superのMapを初期化
                        super.clear();

                        // workMapからデータコピー
                        Integer workMapData = null;
                        Set workEntrySet = vacuumWorkMap.entrySet();
                        Iterator workEntryIte = workEntrySet.iterator();
                        String workKey = null;

                        while(workEntryIte.hasNext()) {

                            Map.Entry obj = (Map.Entry)workEntryIte.next();
                            workKey = (String)obj.getKey();
                            if (workKey != null) {

                                if (mapValueInSize) {
                                    super.put(key, (String)vacuumWorkMap.get(workKey));
                                } else {
                                    super.put(workKey, new Integer((String)vacuumWorkMap.get(workKey)));
                                }
                            }
                        }

                        // サイズ格納
                        this.nowKeySize = super.size();
                        // ファイルポインタ初期化
                        this.initNoMemoryModeSetting(this.lineFile);


                        // Vacuum中でかつ、synchronized前に登録、削除されたデータを追加登録
                        int vacuumDiffDataSize = this.vacuumDiffDataList.size();

                        if (vacuumDiffDataSize > 0) {

                            Object[] diffObj = null;
                            for (int i = 0; i < vacuumDiffDataSize; i++) {

                                // 差分リストからデータを作成
                                diffObj = (Object[])this.vacuumDiffDataList.get(i);
                                if (diffObj[0].equals("1")) {
                                    // put
                                    put(diffObj[1], diffObj[2]);
                                } else if (diffObj[0].equals("2")) {
                                    // remove
                                    remove(diffObj[1]);
                                }
                            }
                        }

                        this.vacuumDiffDataList.clear();
                        this.vacuumDiffDataList = null;

                        if (userMap) {
                            ((FileBaseDataMap)vacuumWorkMap).finishClear();
                        }
                        vacuumWorkMap = null;

                        // Vacuum終了をマーク
                        vacuumExecFlg = false;
                        ret = true;
                    }
                }
            } catch(Exception e2) {
                e2.printStackTrace();
                try {
                    File tmpFile = new File(this.lineFile + ".tmp");
                    if (tmpFile.exists()) {
                        tmpFile.delete();
                    }
                } catch(Exception e3) {
                    e3.printStackTrace();
                    // 致命的
                    StatusUtil.setStatusAndMessage(1, "KeyManagerValueMap - vacuumData - Error [" + e3.getMessage() + e3.getMessage() + "]");
                }
            }
        }

        return ret;
    }


    private void checkDataFileWriterLimit(int nowCount) {
        if (nowCount > ImdstDefine.maxDataFileBufferUseCount) {
            synchronized (sync) {
                try {

                    this.bw.flush();
                    this.bw.close();
                    this.bw = null;
                } catch (Exception e) {
                    this.bw = null;
                } finally {
                    try {
                        this.bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(this.lineFile), true), ImdstDefine.keyWorkFileEncoding), 1024*256);
                        this.dataFileBufferUseCount = new AtomicInteger(0);

                    } catch (Exception e) {
                        this.bw = null;
                    }
                }
            }
        }
    }

    public void close() {
        try {
            synchronized (sync) {
                if (this.deletedDataPointList != null)
                    this.deletedDataPointList.clear();

                if(this.raf != null) this.raf.close();
                if(this.bw != null) this.bw.close();


            }
        } catch(Exception e3) {
        }
    }

    /**
     * Diskモード時にデータストリームを閉じて、データファイルを削除する.<br>
     *
     * @throw Exception
     */
    public void deleteMapDataFile() throws Exception{
        try {
            synchronized (sync) {
                if (this.deletedDataPointList != null)
                    this.deletedDataPointList.clear();

                if(this.raf != null) {
                    this.raf.close();
                    this.raf = null;
                }

                if(this.bw != null) {
                    this.bw.close();
                    this.bw = null;
                }

                File dataFile = new File(this.lineFile);
                if(dataFile.exists()) {
                    dataFile.delete();
                }

            }
        } catch(Exception e3) {
            e3.printStackTrace();
            throw e3;
        }
    }


    /**
     * 読み込み開始位置を渡すことでデータファイルからValue値を読み込む.<br>
     * 返却値は読み込んだデータ数.<br>
     * 1Valueが終端に達していない場合は、読み込み指定された値の+1を返す.<br>
     * 1データが限界に達していないかの判定用
     * 読み込みに失敗した場合は-1を返す.<br>
     *
     * @param buf 読み込み用Buffer
     * @param seekPoint seek値
     * @param readLength 読み込み指定地
     * @return 読み込んだデータサイズ
     */
    private int readDataFile(byte[] buf, long seekPoint, int readLength, Object key) throws Exception {
        int ret = readLength;

        if (raf != null) {

            if (!ImdstDefine.dataFileWriteDelayFlg) {
                ((SortedSchedulingRandomAccess)this.raf).seekAndRead(seekPoint, buf, 0, this.oneDataLength, key);
            } else {
                raf.seek(seekPoint);
                SystemUtil.diskAccessSync(raf, buf, 0, this.oneDataLength);
            }
        } else {
            return -1;
        }


        if (buf[buf.length - 1] != 38 && buf[buf.length - 2] != 33 && buf[buf.length - 1] != 48) ret++;

        return ret;
    }


    /**
     * 共有ファイルに書き出す上限サイズを超えているデータを書き出す.<br>
     */
    private void writeOverSizeData(Object key, Object value) {
        //File overDataFile = new File(this.lineFile + "_/" + (key.toString().hashCode() % 20) + "/" +  DigestUtils.md5Hex(key.toString().getBytes()));
        //BufferedWriter overBw = null;
        try {
            this.overSizeDataStore.put((String)key, ((String)value).substring(this.oneDataLength, ((String)value).length()));
            //overBw = new BufferedWriter (new OutputStreamWriter(new FileOutputStream(overDataFile, false), ImdstDefine.keyWorkFileEncoding));
            //overBw.write(((String)value).substring(this.oneDataLength, ((String)value).length()));
            //SystemUtil.diskAccessSync(overBw);
        } catch (Exception inE) {
            inE.printStackTrace();
            // 致命的
            StatusUtil.setStatusAndMessage(1, "KeyManagerValueMap - Inner File Write - Error [" + inE.getMessage() + "]");
        } finally {
            /*try {
                if (overBw != null) overBw.close();
            } catch (Exception inE2) {
            }*/
        }
    }


    /**
     * 共有ファイルに書き出す上限サイズを超えているデータを読み出す.<br>
     */
    private String readOverSizeData(Object key, byte[] buf) {
        String ret = null;
        try {

            String readStr = (String)this.overSizeDataStore.get((String)key);

            StringBuilder retTmpBuf = new StringBuilder(this.oneDataLength);
            retTmpBuf.append(new String(buf, 0, this.oneDataLength, ImdstDefine.keyWorkFileEncoding));
            retTmpBuf.append(readStr);

            ret = retTmpBuf.toString();
        } catch (Exception inE) {
            inE.printStackTrace();
            // 致命的
            StatusUtil.setStatusAndMessage(1, "KeyManagerValueMap - Inner File Read[get] - Error [" + inE.getMessage() + "]");
        }
        return ret;
    }



    /**
     * Key値を渡すことでそのKeyの対となるValueがデータファイルのどこにあるかを.<br>
     * データファイル中のバイト位置で返す.<br>
     *
     * @param key Key値
     * @return long ファイル中の開始位置 データが存在しない場合は-1が返却される
     */
    private long calcSeekDataPoint(Object key) {
        return this.calcSeekDataPoint(key, true);
    }
    /**
     * Key値を渡すことでそのKeyの対となるValueがデータファイルのどこにあるかを.<br>
     * データファイル中のバイト位置で返す.<br>
     *
     * @param key Key値
     * @return long ファイル中の開始位置 データが存在しない場合は-1が返却される
     */
    private long calcSeekDataPoint(Object key, boolean requestSeekPoint) {

        Integer lineInteger = null;
        if (mapValueInSize) {
            Object lineIntegerObj = (Object)super.get(key);
            if (lineIntegerObj != null) {
                String lineIntegerMix = (String)super.get(key);
                if (lineIntegerMix != null) {
                    lineInteger = new Integer(((String[])lineIntegerMix.split(":"))[0]);
                }
            }
        } else {
            lineInteger = (Integer)super.get(key);
        }
        long ret = this.convertLineToSeekPoint(lineInteger);
        if (ret != -1 && requestSeekPoint == true) {
            if (!ImdstDefine.dataFileWriteDelayFlg)
                ((SortedSchedulingRandomAccess)this.raf).requestSeekPoint(ret, 0, this.oneDataLength);
        }
        return ret;
    }


    /**
     * 共有データファイルの行数を渡すことでseek位置の値を計算して返す.<br>
     *
     * @param lineInteger 行数
     * @return long seek値
     */
    private long convertLineToSeekPoint(Integer lineInteger) {

        int line = 0;
        if (lineInteger != null) {
            line = lineInteger.intValue();
        } else {
            return -1;
        }

        // seek計算
        return new Long(this.seekOneDataLength).longValue() * new Long((line - 1)).longValue();
    }


    /**
     * getKeySize.<br>
     *
     * @param
     * @return int
     * @throws
     */
    public int getKeySize() {
        return this.nowKeySize;
    }


    /**
     * getAllDataCount.<br>
     *
     * @param
     * @return int
     * @throws
     */
    public int getAllDataCount() {
        return this.lineCount;
    }


    /**
     * データを変更した最終時間を記録する.<br>
     * @param time 変更時間
     */
    public void setKLastDataChangeTime(long time) {
        this.lastDataChangeTime = time;
    }

    /**
     * データを変更した最終時間を取得する.<br>
     * @return long 変更時間
     */
    public long getKLastDataChangeTime() {
        return this.lastDataChangeTime;
    }
}
