package okuyama.imdst.util.io;

import java.io.*;
import java.util.*;
import java.util.concurrent.locks.*;
import java.util.concurrent.*;
import java.util.concurrent.ArrayBlockingQueue;

import okuyama.imdst.util.*;


/**
 * IOのRandomAccessFileのラッパー.<br>
 * RandomAccessにおいてシーク処理を行う前に、リクエストを一定以上蓄積し、シーク位置を昇順でソートし<br>
 * シーケンシャルアクセスになるようにスケジューリングを行う。<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class SortedSchedulingRandomAccess extends AbstractDataRandomAccess {

    protected Map dataPointMap = null;
    private Object sync = new Object();

    public SortedSchedulingRandomAccess(File target, String type) throws FileNotFoundException {
        super(target, type);
    }

    public void setDataPointMap(Map dataPointMap) {
        this.dataPointMap = dataPointMap;
    }


    public int seekAndRead(long seekPoint, byte[] data, int start, int size) throws IOException {
        int ret = -1;
        try {
            super.putHighReferenceData(seekPoint);
            super.seek(seekPoint);
            ret = super.read(data, start, size);
        } catch(IOException ie) {
            throw ie;
        } catch(Exception e) {
            e.printStackTrace();
        }
        return ret;
    }
}
