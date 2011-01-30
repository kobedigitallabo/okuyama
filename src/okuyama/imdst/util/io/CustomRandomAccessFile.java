package okuyama.imdst.util.io;

import java.io.*;
import java.util.*;

import okuyama.imdst.util.*;

public class CustomRandomAccessFile extends RandomAccessFile {
    
    private ValueCacheMap cache = null;
    
    private long nowSeekPoint = -1;
    
    private boolean realSeek = false;
    
    public CustomRandomAccessFile(File target, String type) throws FileNotFoundException {
        super(target, type);
        if (ImdstDefine.useValueCache) this.cache = new ValueCacheMap(ImdstDefine.valueCacheMaxSize);
    }


    public void seek(long seekPoint) throws IOException {
        this.realSeek = false;
        this.nowSeekPoint = seekPoint;
        if (this.cache != null && this.cache.containsKey(new Long(seekPoint)))
            return;

        super.seek(seekPoint);
        this.realSeek = true;
    }
    
    
    public void write(byte[] data, int start, int size) throws IOException {
        if (!this.realSeek) super.seek(this.nowSeekPoint);
        if (this.cache != null) this.cache.put(new Long(this.nowSeekPoint), data);
        super.write(data, start, size);
    }
    
    
    public int read(byte[] data, int start, int size) throws IOException {
        
        if (this.cache != null && this.realSeek == false) {
            byte[] tmpData = (byte[])this.cache.get(new Long(this.nowSeekPoint));
            if (tmpData != null) {
                int i = 0;
                for (i = 0; i < size; i++) {
                    data[start+i] = tmpData[i]; 
                }
                return i;
            } else {
                super.seek(this.nowSeekPoint);
            }
        }

        return super.read(data, start, size);
    }
    
    public void close() throws IOException {
        if (this.cache != null) this.cache.clear();
        this.cache = null;
        super.close();
    }
}

