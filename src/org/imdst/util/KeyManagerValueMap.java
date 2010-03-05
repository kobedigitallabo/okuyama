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
    private int lineCount = 0;
    private int oneDataLength = new Double(ImdstDefine.saveDataMaxSize * 1.38).intValue();
    private int seekOneDataLength = (new Double(ImdstDefine.saveDataMaxSize * 1.38).intValue() + 1);


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
            int counter = 0;

            // 現在のファイルの終端
            while(br.readLine() != null){
                counter++;
            }
            this.lineCount = counter;
            br.close();
            isr.close();
            fis.close();
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
                    if (buf[i] == 35) break;
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
                // 足りない文字列は固定の"#"で補う(35)
                byte[] appendDatas = new byte[oneDataLength - valueSize];

                for (int i = 0; i < appendDatas.length; i++) {
                    appendDatas[i] = 35;
                }

                writeStr.append(new String(appendDatas));
                // 書き込む行を決定
                this.lineCount++;
                this.bw.write(writeStr.toString());
                this.bw.write("\n");
                //this.bw.newLine();
                this.bw.flush();
                super.put(key, new Integer(lineCount));
            } catch (Exception e) {
				System.out.println(oneDataLength - valueSize);
                e.printStackTrace();
                // 致命的
                StatusUtil.setStatusAndMessage(1, "KeyManagerValueMap - put - Error [" + e.getMessage() + "]");
                
            }
        }
        return ret;
    }

}
