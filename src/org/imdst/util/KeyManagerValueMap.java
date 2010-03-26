package org.imdst.util;

import java.util.*;
import java.io.*;

import org.batch.util.ILogger;
import org.batch.util.LoggerFactory;
import org.batch.lang.BatchException;
import org.imdst.util.StatusUtil;

/**
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class KeyManagerValueMap extends HashMap implements Cloneable, Serializable {

    private boolean memoryMode = true;

    private transient FileOutputStream fos = null;
    private transient OutputStreamWriter osw = null;
    private transient BufferedWriter bw = null;
    private transient RandomAccessFile raf = null;
    private String lineFile = null;
    private int lineCount = 0;
    private int oneDataLength = new Double(ImdstDefine.saveDataMaxSize * 1.38).intValue();
    private int seekOneDataLength = (new Double(ImdstDefine.saveDataMaxSize * 1.38).intValue() + 1);
    private long lastDataChangeTime = 0L;
    private int nowKeySize = 0;

    public KeyManagerValueMap(int size) {
        super(size);
    }

    /**
     * 本メソッドは使用前に必ず呼び出す<br>
     * Objectに書き出した後でも必須
     */
    public void initNoMemoryModeSetting(String lineFile) {
        try {
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

    public Object get(Object key) {
        Object ret = null;
        if (memoryMode) {
            ret = super.get(key);
        } else {
            try {

                int i = 0;
                byte[] buf = new byte[oneDataLength];

                int line = ((Integer)super.get(key)).intValue();
                // seek計算
                long seekPoint = new Long(seekOneDataLength).longValue() * new Long((line - 1)).longValue();

                raf.seek(seekPoint);
                raf.read(buf,0,oneDataLength);
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
            int valueSize = ((String)value).length();

            try {
                writeStr.append((String)value);

                // 渡されたデータが固定の長さ分ない場合は足りない部分を補う
                // 足りない文字列は固定の"&"で補う(38)
                byte[] appendDatas = new byte[oneDataLength - valueSize];

                for (int i = 0; i < appendDatas.length; i++) {
                    appendDatas[i] = 38;
                }

                writeStr.append(new String(appendDatas));
                // 書き込む行を決定
                this.lineCount++;
                this.bw.write(writeStr.toString());
                this.bw.write("\n");
                //this.bw.newLine();
                this.bw.flush();
                super.put(key, new Integer(lineCount));
                this.nowKeySize = super.size();
            } catch (Exception e) {

                e.printStackTrace();
                // 致命的
                StatusUtil.setStatusAndMessage(1, "KeyManagerValueMap - put - Error [" + e.getMessage() + "]");
                
            }
        }
        return ret;
    }

    /**
     * データファイルの不要領域を掃除して新たなファイルを作りなおす
     *
     */
    public boolean vacuumData() {
        boolean ret = false;
        FileOutputStream tmpFos = null;
        OutputStreamWriter tmpOsw = null;
        BufferedWriter tmpBw = null;
        RandomAccessFile raf = null;

        try {

            tmpFos = new FileOutputStream(new File(this.lineFile + ".tmp"), true);
            tmpOsw = new OutputStreamWriter(tmpFos, ImdstDefine.keyWorkFileEncoding);
            tmpBw = new BufferedWriter(tmpOsw);
            raf = new RandomAccessFile(new File(this.lineFile) , "r");

            String dataStr = null;
            Set entrySet = super.entrySet();
            Iterator entryIte = entrySet.iterator();   
            Integer key = null;
            int putCounter = 0;

            while(entryIte.hasNext()) {
                Map.Entry obj = (Map.Entry)entryIte.next();
                key = (Integer)obj.getKey();

                int i = 0;
                byte[] buf = new byte[oneDataLength];

                int line = ((Integer)super.get(key)).intValue();

                // seek計算
                long seekPoint = new Long(seekOneDataLength).longValue() * new Long((line - 1)).longValue();

                raf.seek(seekPoint);
                raf.read(buf, 0, oneDataLength);

                dataStr = new String(buf, ImdstDefine.keyWorkFileEncoding);
                tmpBw.write(dataStr);
                tmpBw.write("\n");
                putCounter++;
                super.put(key, new Integer(putCounter));

            }
            this.nowKeySize = super.size();
        } catch (Exception e) {
            //e.printStackTrace();
            // 致命的
            StatusUtil.setStatusAndMessage(1, "KeyManagerValueMap - vacuumData - Error [" + e.getMessage() + "]");
        } finally {
            try {
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
                    ret = true;

                }
            } catch(Exception e2) {
                e2.printStackTrace();
                try {
                    File tmpFile = new File(this.lineFile + ".tmp");
                    if (tmpFile.exists()) {
                        tmpFile.delete();
                    }
                } catch(Exception e3) {
                    // 致命的
                    StatusUtil.setStatusAndMessage(1, "KeyManagerValueMap - vacuumData - Error [" + e2.getMessage() + e3.getMessage() + "]");
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
