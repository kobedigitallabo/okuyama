package okuyama.imdst.util.io;

import java.io.*;
import java.util.*;
import java.util.concurrent.locks.*;
import java.util.concurrent.*;
import java.util.concurrent.ArrayBlockingQueue;

import okuyama.imdst.util.*;


/**
 * IOのRandomAccessFileのラッパー.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class SortedSchedulingRandomAccess extends RandomAccessFile {

    public ArrayBlockingQueue readRequestSeekPointQueue = new ArrayBlockingQueue(65534);
    private int maxSeqSize = 65534;

    private long[] seekPointList = null;
    private ConcurrentHashMap responseDataMap = new ConcurrentHashMap(180);
    private ConcurrentHashMap responseDataSizeMap = new ConcurrentHashMap(180);

    private long nowSeekPoint = 0L;

    // 30万回のSeekでいくらかゴミが出た場合は全て削除
    private long clearCount= 300000;
    private long executeCount =0L;


    public SortedSchedulingRandomAccess(File target, String type) throws FileNotFoundException {
        super(target, type);
    }

    public void requestSeekPoint(long seekPoint, int start, int size) {
        try {
            Map requestParams = new HashMap(4);
            requestParams.put(2, start);
            requestParams.put(3, size);
            requestParams.put(4, seekPoint);
            this.readRequestSeekPointQueue.put(requestParams);

        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    public void seek(long seekPoint) throws IOException {
        this.nowSeekPoint = seekPoint;
        super.seek(seekPoint);
    }

    public void write(byte[] data, int start, int size) throws IOException {
        this.responseDataSizeMap.remove(this.nowSeekPoint);
        this.responseDataMap.remove(this.nowSeekPoint);
        super.write(data, start, size);
    }


    public int seekAndRead(long seekPoint, byte[] data, int start, int size) throws IOException {
        int ret = -1;
        try {
            executeCount++;
            if (this.clearCount < this.executeCount) {
                this.responseDataSizeMap.clear();
                this.responseDataMap.clear();
                this.readRequestSeekPointQueue.clear();
                executeCount = 0;
            }

            Integer readDataSize = (Integer)this.responseDataSizeMap.remove(seekPoint);
            if (readDataSize != null) {
                Map response = (Map)this.responseDataMap.remove(seekPoint);
                byte[] responseBytes = (byte[])response.get(1);
                for (int idx = 0; idx < responseBytes.length; idx++) {
                    data[idx] = responseBytes[idx];
                }
                return readDataSize.intValue();
            }

            if (this.readRequestSeekPointQueue.size() > 1) {

                int count = this.maxSeqSize;
                if (count > readRequestSeekPointQueue.size()) count = readRequestSeekPointQueue.size();

                System.out.println("Count=" + count + " responseDataMap=" + responseDataMap.size() + " responseDataSizeMap=" + responseDataSizeMap.size());
                this.seekPointList = new long[count];
                Map requestParams = null;
                for (int i = 0; i < count; i++){

                    requestParams = (Map)readRequestSeekPointQueue.take();
                    seekPointList[i] = ((Long)requestParams.get(4)).longValue();
                    this.responseDataMap.put(seekPointList[i], requestParams);
                }
                java.util.Arrays.sort(seekPointList);

                for (int i = 0; i < seekPointList.length; i++) {
                    super.seek(seekPointList[i]);
                    requestParams = (Map)this.responseDataMap.remove(seekPointList[i]);

                    int reqStart = ((Integer)requestParams.get(2)).intValue();
                    int reqSize = ((Integer)requestParams.get(3)).intValue();
                    byte[] reqData = new byte[reqSize];
                    Integer readDataLen = new Integer(super.read(reqData, reqStart, reqSize));
                    requestParams.put(1, reqData);
                    this.responseDataMap.put(seekPointList[i], requestParams);
                    this.responseDataSizeMap.put(seekPointList[i], readDataLen);
                }

                // 今回のメソッド呼び出しのリクエストデータを作成
                // 既に別リクエストが同一のseekポイントを指定している場合データが消えている可能性が
                // あるので、null確認を行う
                readDataSize = (Integer)this.responseDataSizeMap.remove(seekPoint);
                Map response = (Map)this.responseDataMap.remove(seekPoint);
                if (readDataSize == null || response == null) {
                    
                    super.seek(seekPoint);
                    ret = super.read(data, start, size);
                } else {
                    byte[] responseBytes = (byte[])response.get(1);
                    for (int idx = 0; idx < responseBytes.length; idx++) {
                        data[idx] = responseBytes[idx];
                    }
                    ret = readDataSize.intValue();
                }
            } else if (this.readRequestSeekPointQueue.size() == 1){

                Map requestParams = (Map)this.readRequestSeekPointQueue.take();
                long requestSeekPoint = ((Long)requestParams.get(4)).longValue();
                if (requestSeekPoint == seekPoint) {
                    super.seek(requestSeekPoint);
                    ret = super.read(data, start, size);
                } else {
                    super.seek(seekPoint);
                    ret = super.read(data, start, size);
                    this.readRequestSeekPointQueue.put(requestParams);
                }
            } else {
                super.seek(seekPoint);
                ret = super.read(data, start, size);
            }
        } catch(IOException ie) {
            throw ie;
        } catch(Exception e) {
            e.printStackTrace();
        }
        return ret;
    }
}
