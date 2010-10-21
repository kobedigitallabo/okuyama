package okuyama.imdst.util;


import java.util.*;
import java.io.*;
import java.util.concurrent.ConcurrentHashMap;

import okuyama.base.util.ILogger;
import okuyama.base.util.LoggerFactory;
import okuyama.base.lang.BatchException;
import okuyama.imdst.util.StatusUtil;

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

    private boolean memoryMode = true;

    private transient FileOutputStream fos = null;
    private transient OutputStreamWriter osw = null;
    private transient BufferedWriter bw = null;
    private transient RandomAccessFile raf = null;
    private transient Object sync = new Object();
    private transient boolean vacuumExecFlg = false;
    private transient List vacuumDiffDataList = null;

    private String lineFile = null;
    private String tmpVacuumeLineFile = null;
    private String[] tmpVacuumeCopyMapDirs = null;

    private int lineCount = 0;
    private int oneDataLength = new Double(ImdstDefine.saveDataMaxSize * 1.38).intValue();
    private int seekOneDataLength = (new Double(ImdstDefine.saveDataMaxSize * 1.38).intValue() + 1);
    private long lastDataChangeTime = 0L;
    private int nowKeySize = 0;

    private transient boolean readObjectFlg = false;

    // キャッシュ
    private ValueCacheMap valueCacheMap = null;

    // 最大キャッシュサイズ
    private int maxCacheSize = -1;

    // コンストラクタ
    public KeyManagerValueMap(int size, boolean memoryMode) {

        super(size, new Double(size * 0.9).intValue(), 512, memoryMode);

        this.memoryMode = memoryMode;
    }

    // コンストラクタ
    public KeyManagerValueMap(String[] dirs, int numberOfDataSize) {

        super(dirs, numberOfDataSize);
        this.memoryMode = false;
    }

    /**
     * 本メソッドは使用前に必ず呼び出す<br>
     * Objectに書き出した後でも必須
     */
    public void initNoMemoryModeSetting(String lineFile) {
        try {
            if (sync == null) {
                sync = new Object();
            }

            if (this.valueCacheMap == null) {

                this.maxCacheSize = new Long((JavaSystemApi.getRuntimeFreeMem() / 10) / (ImdstDefine.saveDataMaxSize + ImdstDefine.saveKeyMaxSize)).intValue();
                this.valueCacheMap = new ValueCacheMap(this.maxCacheSize);
            } else {
                this.valueCacheMap.clear();
            }

            readObjectFlg  = true;

			this.tmpVacuumeLineFile = lineFile + ".vacuumtmp";
			this.tmpVacuumeCopyMapDirs = new String[5];
			this.tmpVacuumeCopyMapDirs[0] = lineFile + ".cpmapdir1/";
			this.tmpVacuumeCopyMapDirs[1] = lineFile + ".cpmapdir2/";
			this.tmpVacuumeCopyMapDirs[2] = lineFile + ".cpmapdir3/";
			this.tmpVacuumeCopyMapDirs[3] = lineFile + ".cpmapdir4/";
			this.tmpVacuumeCopyMapDirs[4] = lineFile + ".cpmapdir5/";



            this.fos = new FileOutputStream(new File(lineFile), true);
            this.osw = new OutputStreamWriter(this.fos, ImdstDefine.keyWorkFileEncoding);
            this.bw = new BufferedWriter (osw);
            this.raf = new RandomAccessFile(new File(lineFile) , "rw");

            FileInputStream fis = new FileInputStream(new File(lineFile));
            InputStreamReader isr = new InputStreamReader(fis , ImdstDefine.keyWorkFileEncoding);
            BufferedReader br = new BufferedReader(isr);
            this.lineFile = lineFile;
            int counter = 0;

            // 現在のファイルの終端
            while(br.readLine() != null){
                counter++;
            }
            this.lineCount = counter;
            br.close();
            isr.close();
            fis.close();
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
                byte[] buf = new byte[oneDataLength];

                Integer lineInteger = (Integer)super.get(key);
                int line = 0;
                if (lineInteger != null) {
                    line = lineInteger.intValue();
                } else {
                    return null;
                }
                

                // seek計算
                long seekPoint = new Long(seekOneDataLength).longValue() * new Long((line - 1)).longValue();

                synchronized (sync) {
                    // Vacuum中の場合はデータの格納先が変更されている可能性があるので、
                    // ここでチェック
                    if (vacuumExecFlg) {
                        if(!lineInteger.equals((Integer)super.get(key))) {
                            // 再起呼び出し
                            return get(key);
                        }
                    }
                    raf.seek(seekPoint);
                    raf.read(buf,0,oneDataLength);
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
                } else if ((ret = this.valueCacheMap.get(key)) == null) {
                    // キャッシュから取得できない場合はファイルから取得
                    int i = 0;
                    int line = 0;
                    Integer lineInteger = null;
                    byte[] buf = new byte[oneDataLength];
                    long seekPoint = 0L;


                    lineInteger = (Integer)super.get(key);

                    if (lineInteger != null) {
                        line = lineInteger.intValue();
                    } else {
                        return null;
                    }
                    

                    // seek計算
                    seekPoint = new Long(seekOneDataLength).longValue() * new Long((line - 1)).longValue();

                    synchronized (sync) {
                        if (raf != null) {
                            raf.seek(seekPoint);
                            raf.read(buf,0,oneDataLength);
                        } else {
                            return null;
                        }
                    }

                    for (; i < buf.length; i++) {
                        if (buf[i] == 38) break;
                    }

                    ret = new String(buf, 0, i, ImdstDefine.keyWorkFileEncoding);
                    buf = null;

                    // キャッシュに登録
                    this.valueCacheMap.put(key, ret);
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
            int line = 0;
            Integer lineInteger = null;
            byte[] buf = new byte[oneDataLength];
            long seekPoint = 0L;

            synchronized (sync) {

                // Vacuum中はMap内の行数指定と実際のデータファイルでの位置が異なる場合があるため、
                // Vacuum中で且つ、Mapを更新中の場合はここsynchronizedする。
                lineInteger = (Integer)super.get(key);

                if (lineInteger != null) {
                    line = lineInteger.intValue();
                } else {
                    return null;
                }

                // seek計算
                seekPoint = new Long(seekOneDataLength).longValue() * new Long((line - 1)).longValue();

                raf.seek(seekPoint);
                raf.read(buf,0,oneDataLength);

                for (; i < buf.length; i++) {
                    if (buf[i] == 38) break;
                }

                ret = new String(buf, 0, i, ImdstDefine.keyWorkFileEncoding);
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
     * putをオーバーライド.<br>
     * MemoryモードとFileモードで保存方法が異なる.<br>
     *
     * @param key 登録kye値(全てStringとなる)
     * @param value 登録value値(全てStringとなる)
     * @return Object 返却値(Fileモード時は返却値は常にnull)
     */
    public Object put(Object key, Object value) {
        Object ret = null;
        if (this.memoryMode) {
            ret = super.put(key, value);
        } else {
            StringBuffer writeBuf = new StringBuffer();
            int valueSize = (value.toString()).length();

            try {

                // キャッシュから削除
                if(this.valueCacheMap.containsKey(key))
                    this.valueCacheMap.remove(key);

                if (readObjectFlg == true) {

                    int line = 0;
                    Integer lineInteger = null;
                    long seekPoint = 0L;

                    writeBuf.append((String)value);

                    // 渡されたデータが固定の長さ分ない場合は足りない部分を補う
                    // 足りない文字列は固定の"&"で補う(38)
                    byte[] appendDatas = new byte[oneDataLength - valueSize];

                    for (int i = 0; i < appendDatas.length; i++) {
                        appendDatas[i] = 38;
                    }

                    writeBuf.append(new String(appendDatas));
                    writeBuf.append("\n");


                    if ((lineInteger = (Integer)super.get(key)) == null) {

                        // 書き込む行を決定
                        synchronized (sync) {

                            if (vacuumExecFlg) {
                                // Vacuum差分にデータを登録
                                Object[] diffObj = {"1", (String)key, (String)value};
                                this.vacuumDiffDataList.add(diffObj);
                            }

                            this.bw.write(writeBuf.toString());
                            this.bw.flush();
                            this.lineCount++;
                            super.put(key, new Integer(this.lineCount));
                            this.nowKeySize = super.size();
                        }
                    } else {
                        // すでにファイル上に存在する
                        line = lineInteger.intValue();

                        // seek計算
                        seekPoint = new Long(seekOneDataLength).longValue() * new Long((line - 1)).longValue();

                        synchronized (sync) {
                            if (vacuumExecFlg) {
                                // Vacuum差分にデータを登録
                                Object[] diffObj = {"1", key, value};
                                this.vacuumDiffDataList.add(diffObj);
                            }

                            if (raf != null) {
                                raf.seek(seekPoint);
                                raf.write(writeBuf.toString().getBytes(),0,oneDataLength);
                            }
                        }
                    }
                } else {
                    super.put(key, value);
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

        // データファイルモード時はCacheをチェック
        if (!this.memoryMode) {
            // キャッシュから削除
            if(this.valueCacheMap != null && this.valueCacheMap.containsKey(key))
                this.valueCacheMap.remove(key);
        }

        synchronized (sync) {
			ret = super.remove(key);
			this.nowKeySize = super.size();

            if (vacuumExecFlg) {

                Object diffObj[] = {"2", (String)key};
                this.vacuumDiffDataList.add(diffObj);
            }
        }

        // データファイルモード時はCacheをチェック
        if (!this.memoryMode) {
            // キャッシュから削除
            if(this.valueCacheMap != null && this.valueCacheMap.containsKey(key))
                this.valueCacheMap.remove(key);
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
        FileOutputStream tmpFos = null;
        OutputStreamWriter tmpOsw = null;
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

            tmpFos = new FileOutputStream(new File(this.lineFile + ".tmp"), true);
            tmpOsw = new OutputStreamWriter(tmpFos, ImdstDefine.keyWorkFileEncoding);
            tmpBw = new BufferedWriter(tmpOsw);
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
                    vacuumWorkMap.put(key, new Integer(putCounter).toString());
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
                    tmpBw.flush();
                    //
                    tmpBw.close();
                    tmpOsw.close();
                    tmpFos.close();

                    synchronized (sync) {

                        raf.close();

                        if(this.raf != null) this.raf.close();
                        if(this.bw != null) this.bw.close();
                        if(this.osw != null) this.osw.close();
                        if(this.fos != null) this.fos.close();

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
                                workMapData = new Integer((String)vacuumWorkMap.get(workKey));
                                super.put(workKey, workMapData);
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


    public void close() {
        try {
            synchronized (sync) {
                if(this.raf != null) this.raf.close();
                if(this.bw != null) this.bw.close();
                if(this.osw != null) this.osw.close();
                if(this.fos != null) this.fos.close();
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
                if(this.raf != null) {
                    this.raf.close();
                    this.raf = null;
                }

                if(this.bw != null) {
                    this.bw.close();
                    this.bw = null;
                }

                if(this.osw != null) {
                    this.osw.close();
                    this.osw = null;
                }

                if(this.fos != null) {
                    this.fos.close();
                    this.fos = null;
                }

                File dataFile = new File(this.lineFile);
                if(dataFile.exists()) {
                    dataFile.delete();
                }

                // データファイルモード時はCacheをチェック
                // キャッシュ全削除
                if (!this.memoryMode) {
                    // キャッシュから削除
                    if(this.valueCacheMap != null)
                        this.valueCacheMap.clear();
                }
            }
        } catch(Exception e3) {
            e3.printStackTrace();
            throw e3;
        }
    }

    public int getKeySize() {
        return this.nowKeySize;
    }

    public int getAllDataCount() {
        return this.lineCount;
    }

    public int getCacheDataSize() {
        if (this.valueCacheMap != null) {
            return valueCacheMap.size();
        } else {
            return -1;
        }
    }

    public int getMaxCacheSize() {
        return maxCacheSize;
    }

    public int getCacheSize() {
        if(this.valueCacheMap != null) {
            return this.valueCacheMap.size();
        } else {
            return -1;
        }
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
