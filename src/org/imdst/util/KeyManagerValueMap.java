package org.imdst.util;


import java.util.*;
import java.io.*;
import java.util.concurrent.*;

import org.batch.util.ILogger;
import org.batch.util.LoggerFactory;
import org.batch.lang.BatchException;
import org.imdst.util.StatusUtil;

/**
 * KeyとValueを管理する独自Mapクラス.<br>
 * メモリモードとファイルモードで動きが異なる.<br>
 * メモリモード:KeyとValueを親クラスであるHashMapで管理する.<br>
 * ファイルモード:Keyは親クラスのHashMapに、Valueはファイルに記録する<br>
 *                KeyとValueが格納させている行数を記録している.<br>
 *                行数から、ファイル内からValueを取り出す.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class KeyManagerValueMap extends ConcurrentHashMap implements Cloneable, Serializable {

    private boolean memoryMode = true;

    private transient FileOutputStream fos = null;
    private transient OutputStreamWriter osw = null;
    private transient BufferedWriter bw = null;
    private transient RandomAccessFile raf = null;
    private transient Object sync = new Object();
    private transient boolean vacuumeExecFlg = false;
    private transient ArrayList vacuumDiffDataList = null;

    private String lineFile = null;
    private int lineCount = 0;
    private int oneDataLength = new Double(ImdstDefine.saveDataMaxSize * 1.38).intValue();
    private int seekOneDataLength = (new Double(ImdstDefine.saveDataMaxSize * 1.38).intValue() + 1);
    private long lastDataChangeTime = 0L;
    private int nowKeySize = 0;

    private transient boolean readObjectFlg = false;

    // コンストラクタ
    public KeyManagerValueMap(int size) {
        super(size, new Double(size * 0.9).intValue(), 50);
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
            readObjectFlg  = true;
            memoryMode = false;

            this.fos = new FileOutputStream(new File(lineFile), true);
            this.osw = new OutputStreamWriter(this.fos, ImdstDefine.keyWorkFileEncoding);
            this.bw = new BufferedWriter (osw);
            this.raf = new RandomAccessFile(new File(lineFile) , "r");

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


    public Object getNoCnv(Object key) {
        Object ret = null;
        if (memoryMode) {
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
					// Vacuume中の場合はデータの格納先が変更されている可能性があるので、
					// ここでチェック
					if (vacuumeExecFlg) {
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

    public Object get(Object key) {
        Object ret = null;
        if (memoryMode) {
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
					// Vacuume中の場合はデータの格納先が変更されている可能性があるので、
					// ここでチェック
					if (vacuumeExecFlg) {
						if(!lineInteger.equals((Integer)super.get(key))) {
							// 再起呼び出し
							return get(key);
						}
					}
                    raf.seek(seekPoint);
                    raf.read(buf,0,oneDataLength);
                }

                for (; i < buf.length; i++) {
                    if (buf[i] == 38) break;
                }

                ret = new String(buf, 0, i, ImdstDefine.keyWorkFileEncoding);
            } catch (Exception e) {
                e.printStackTrace();
                // 致命的
                StatusUtil.setStatusAndMessage(1, "KeyManagerValueMap - get - Error [" + e.getMessage() + "]");
                
            }
        }
        return ret;
    }


    public Object put(Object key, Object value) {
        Object ret = null;
        if (memoryMode) {
            ret = super.put(key, value);
        } else {
            StringBuffer writeStr = new StringBuffer();
            int valueSize = (value.toString()).length();

            try {

                if (readObjectFlg == true) {
                    writeStr.append((String)value);

                    // 渡されたデータが固定の長さ分ない場合は足りない部分を補う
                    // 足りない文字列は固定の"&"で補う(38)
                    byte[] appendDatas = new byte[oneDataLength - valueSize];

                    for (int i = 0; i < appendDatas.length; i++) {
                        appendDatas[i] = 38;
                    }

                    writeStr.append(new String(appendDatas));
                    String write = writeStr.toString();

                    // 書き込む行を決定
                    synchronized (sync) {

						if (vacuumeExecFlg) {
							// Vacuum差分にデータを登録
							Object[] diffObj = {"1", key, value};
							vacuumDiffDataList.add(diffObj);
						}

                        this.lineCount++;
                        this.bw.write(write);
                        this.bw.write("\n");
                        //this.bw.newLine();
                        this.bw.flush();
                    	super.put(key, new Integer(lineCount));
                    	this.nowKeySize = super.size();
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


    public Object remove(Object key) {

		if (vacuumeExecFlg) {
    		synchronized (sync) {
				Object diffObj[] = {"2", key};
				vacuumDiffDataList.add(diffObj);
			}
		}
        return super.remove(key);
    }

    /**
     * データファイルの不要領域を掃除して新たなファイルを作りなおす.<br>
     *
     */
    public boolean vacuumData() {
        boolean ret = false;
        FileOutputStream tmpFos = null;
        OutputStreamWriter tmpOsw = null;
        BufferedWriter tmpBw = null;
        RandomAccessFile raf = null;
        ConcurrentHashMap vacuumWorkMap = null;
        String dataStr = null;
        Set entrySet = null;
        Iterator entryIte = null;
        String key = null;
        int putCounter = 0;


        synchronized (sync) {
            vacuumeExecFlg = true;
            vacuumDiffDataList = new ArrayList(1000);
        }

        vacuumWorkMap = new ConcurrentHashMap(super.size());

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
	                vacuumWorkMap.put(key, new Integer(putCounter));
				}
            }
        } catch (Exception e) {
            e.printStackTrace();
            // 致命的
            StatusUtil.setStatusAndMessage(1, "KeyManagerValueMap - vacuumData - Error [" + e.getMessage() + "]");
        } finally {
            try {
                synchronized (sync) {

                    // 正常終了の場合のみ、ファイルを変更
                    if (StatusUtil.getStatus() == 0)  {
                        raf.close();
                        // ファイルflush
                        tmpBw.flush();

                        tmpBw.close();
                        tmpOsw.close();
                        tmpFos.close();

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

						// 一旦初期化
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
				                workMapData = (Integer)vacuumWorkMap.get(workKey);
				                super.put(workKey, workMapData);
							}
			            }

						// サイズ格納
					    this.nowKeySize = super.size();
						// ファイルポインタ初期化
                        this.initNoMemoryModeSetting(this.lineFile);
						// Vacuum終了をマーク
						vacuumeExecFlg = false;
						// Vacuum中の差分を埋める
						if (vacuumDiffDataList.size() > 0) {
							
							Object[] diffObj = null;
							for (int i = 0; i < vacuumDiffDataList.size(); i++) {
								// 差分リストからデータを作成
								diffObj = (Object[])vacuumDiffDataList.get(i);
								if (diffObj[0].equals("1")) {
									// put
									put(diffObj[1], diffObj[2]);
								} else if (diffObj[0].equals("2")) {
									// remove
									remove(diffObj[1]);
								}
							}
						}
						vacuumDiffDataList = null;
						vacuumWorkMap = null;
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

            if(this.raf != null) this.raf.close();
            if(this.bw != null) this.bw.close();
            if(this.osw != null) this.osw.close();
            if(this.fos != null) this.fos.close();
        } catch(Exception e3) {
        }
    }

    public int getKeySize() {
        return this.nowKeySize;
    }

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
