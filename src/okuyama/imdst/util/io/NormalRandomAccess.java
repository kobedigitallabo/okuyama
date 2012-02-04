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
public class NormalRandomAccess extends AbstractDataRandomAccess {

    protected Map dataPointMap = null;


    public NormalRandomAccess(File target, String type) throws FileNotFoundException {
        super(target, type);
    }

    public void setDataPointMap(Map dataPointMap) {
        this.dataPointMap = dataPointMap;
    }

    public void requestSeekPoint(long seekPoint, int start, int size) {

    }

    public void seek(long seekPoint) throws IOException {
        super.seek(seekPoint);
    }

    public void write(byte[] data, int start, int size) throws IOException {
        super.write(data, start, size);
    }


    public int seekAndRead(long seekPoint, byte[] data, int start, int size, Object key) throws IOException {
        int ret = 0;
        super.seek(seekPoint);
        ret = super.read(data, start, size);
        return ret;
    }

    public void close() throws IOException {
        super.close();
    }
}
