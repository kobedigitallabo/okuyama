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

    private FileOutputStream fos = null;
    private OutputStreamWriter osw = null;
    private BufferedWriter bw = null;
    private RandomAccessFile raf = null;
	private int lineCount = 0;
	private int oneDataLength = 1024;
	private int seekOneDataLength = (1024 + 1);


	public KeyManagerValueMap(int size) {
		super(size);
	}

	public KeyManagerValueMap(boolean memMode, int size) {
		super(size);
		memoryMode = memMode;
		if (memoryMode == false) {
		}
		try {
			this.fos = new FileOutputStream(new File("c:/A.TXT"), true);
			this.osw = new OutputStreamWriter(this.fos, ImdstDefine.keyWorkFileEncoding);
			this.bw = new BufferedWriter (osw);
			this.raf = new RandomAccessFile(new File("c:/A.TXT") , "r");

		} catch(Exception e) {
			// 致命的
			StatusUtil.setStatusAndMessage(1, "KeyManagerValueMap - Error [" + e.getMessage() + "]");
		}

	}

	/**
	 * 本メソッドは使用前に必ず呼び出す<br>
	 * Objectに書き出した後でも必須
	 */
	public void initNoMemoryModeSetting(String lineFile) {
		try {
			this.fos = new FileOutputStream(new File(lineFile), true);
			this.osw = new OutputStreamWriter(this.fos, ImdstDefine.keyWorkFileEncoding);
			this.bw = new BufferedWriter (osw);
			this.raf = new RandomAccessFile(new File(lineFile) , "r");

            FileInputStream fis = new FileInputStream(new File(lineFile));
            InputStreamReader isr = new InputStreamReader(fis , ImdstDefine.keyWorkFileEncoding);
            BufferedReader br = new BufferedReader(isr);
            int counter = 0;
            while(br.readLine() != null){
				counter++;
			}
			this.lineCount = counter;
			br.close();
			isr.close();
			fis.close();
		} catch(Exception e) {
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
				byte[] buf = new byte[oneDataLength];

				int line = ((Integer)super.get(key)).intValue();
				
				// seek計算
				int seekInt = seekOneDataLength * line;

				raf.seek(seekInt);
				raf.read(buf,0,oneDataLength);
				ret = new String(buf);
			} catch (Exception e) {
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
			try {
				// 書き込む行を決定
				this.lineCount++;
				this.bw.write((String)value);
				this.bw.newLine();

			} catch (Exception e) {
				// 致命的
				StatusUtil.setStatusAndMessage(1, "KeyManagerValueMap - get - Error [" + e.getMessage() + "]");
				
			}
		}
		return ret;
	}

}
