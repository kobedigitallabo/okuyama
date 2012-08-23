package okuyama.imdst.util;

import java.io.*;
import java.util.concurrent.locks.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * To manage files using a List.<br>
 * A small amount of memory usage, so File.<br>
 * Memory capacity can be managed independently of the number of data.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class FileBaseDataList extends AbstractList {

    private String dataFileDir = null;

    private File dataFile = null;

    private RandomAccessFile raf = null;

    private BufferedWriter wr = null;

    private BufferedReader br = null;

    private int nowIndex = 0;

    private boolean waitFlg = false;

    public int waitTime = 100;





    // Total Size
    private AtomicInteger totalSize = null;

    private int oneDataLength = 35 + new Double(ImdstDefine.saveKeyMaxSize * 1.38 + 1).intValue() + new Double(ImdstDefine.saveDataMaxSize * 1.40 + 1).intValue();
//    private int oneDataLength = 1 + 170 + 2560;

    private List tmpList = null;

    private Object sync = new Object();

    /**
     * コンストラクタ.<br>
     *
     * @param dataFile
     * @return 
     * @throws
     */
    public FileBaseDataList(String dataFile) {
        this.dataFileDir = dataFile;
        this.dataFile = new File(dataFile);
        this.init();
    }


    /**
     * コンストラクタ.<br>
     *
     * @param dataFile
     * @return 
     * @throws
     */
    public FileBaseDataList(String dataFile, int size) {
        this.oneDataLength = size;
        this.dataFileDir = dataFile;
        this.dataFile = new File(dataFile);
        this.init();
    }


    /**
     * コンストラクタ.<br>
     * 遅延設定
     *
     * @param dataFile
     * @return 
     * @throws
     */
    public FileBaseDataList(String dataFile, int size, boolean waitFlg) {
        this.oneDataLength = size;
        this.dataFileDir = dataFile;
        this.dataFile = new File(dataFile);
        this.init();
        this.waitFlg = waitFlg;
    }


    public boolean init() {
        boolean ret = false;
        try {

            // clean data file
            if (this.dataFile.exists()) this.dataFile.delete();

            this.totalSize = new AtomicInteger(0);

            // start file stream
            this.raf = new RandomAccessFile(this.dataFile, "rwd");
            this.br = new BufferedReader(new FileReader(this.dataFile));
            this.wr = new BufferedWriter(new FileWriter(this.dataFile, true), this.oneDataLength+10);
            ret = true;
        } catch(Exception e) {
            e.printStackTrace();
        }
        return ret;
    }


    /**
     * add.<br>
     * 
     * @param value
     * @return boolean
     */
    public boolean add(Object value) {
        boolean ret = true;
        try {
            StringBuilder writeStrBuf = null;

            if (value instanceof Object[]) {
                writeStrBuf = new StringBuilder("ObjectsClass");
                Object[] objectValues = (Object[])value;

                for (int idx = 0; idx < objectValues.length; idx++) {
                    writeStrBuf.append(objectValues[idx]);
                    writeStrBuf.append("ObjectsClass");
                }
            }  else if (value instanceof String) {
                writeStrBuf = new StringBuilder("StringClass");
                writeStrBuf.append((String)value);
            }


            String writeStr = writeStrBuf.toString();

            synchronized (sync) {

                this.wr.write(writeStr);
                this.wr.write("\n");
                SystemUtil.diskAccessSync(this.wr);
                if (this.waitFlg) if ((this.size() % 5) == 0)Thread.sleep(this.waitTime);
            }
            writeStr = null;
            writeStrBuf = null;
        } catch (Exception e) {
            e.printStackTrace();
        }
        // The size of an increment
        this.totalSize.getAndIncrement();

        return ret;
    }


    /**
     * get.<br>
     * 
     * @param key
     */
    public Object get(int index) throws IndexOutOfBoundsException {
        Object ret = null;

        if (this.totalSize.get() > index) {
            try {

                synchronized (sync) { 

                    String tmp = null;
                    if (index == nowIndex) {

                        tmp = this.br.readLine();
                        nowIndex++;
                    } else {

                        this.nowIndex = 0;
                        this.br.close();
                        this.br = new BufferedReader(new FileReader(this.dataFile));
                        while((tmp = this.br.readLine()) != null) {
                            if(index == nowIndex) {
                                nowIndex++;
                                break;
                            }
                            nowIndex++;
                        }
                    }

                    if (tmp.indexOf("StringClass") == 0) {
                        String[] tmpStrs = tmp.split("StringClass");
                        ret = tmpStrs[tmpStrs.length - 1];
                    } else if (tmp.indexOf("ObjectsClass") == 0) {

                        String[] tmpStrs = tmp.split("ObjectsClass");
                        ret = tmpStrs[tmpStrs.length - 1];
                        ArrayList dataList = new ArrayList();

                        for (int idx = 0; idx < tmpStrs.length; idx++) {

                            if (!tmpStrs[idx].equals("")) {
                                dataList.add(tmpStrs[idx]);
                            }
                        }

                        int dataListSize = dataList.size();
                        Object[] retObjs = new Object[dataListSize];
                        for (int idx = 0; idx < dataListSize; idx++) {
                            retObjs[idx] = dataList.get(idx);
                        }
                        return retObjs;
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            throw new IndexOutOfBoundsException("List Size Of " + this.totalSize.get());
        }
        return ret;
    }




    /**
     * size.<br>
     *
     * @param
     * @return 
     * @throws
     */
    public int size() {
        return this.totalSize.get();
    }


    /**
     * clear.<br>
     *
     * @param
     * @return 
     * @throws
     */
    public void clear() {
        try {
            if(this.raf != null) {
                this.raf.close();
                this.raf = null;
            }

            if(this.wr != null) {
                this.wr.close();
                this.wr = null;
            }

            if(this.br != null) {
                this.br.close();
                this.br = null;
            }

            if (this.dataFile.exists()) this.dataFile.delete();
        } catch(Exception e) {
        }
    }

}
