package org.okuyama.hadoop.format;

import java.io.*;
import java.util.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.Text;

import okuyama.imdst.client.*;

public class OkuyamaRecordReader extends RecordReader<String, Text> implements Closeable {

    private OkuyamaInputSplit split = null;

    private Map resultData = null;
    private Set entrySet = null;
    private Iterator entryIte = null;
    private Map.Entry obj = null;


    public OkuyamaRecordReader(InputSplit split, TaskAttemptContext context) {
    }

    public void close() {
    }
    


    public String getCurrentKey() {
        return (String)this.obj.getKey();
    }

    public Text getCurrentValue() {
        Text ret = new Text((String)this.obj.getValue());
        return ret;
    }

    public float getProgress() {
        return new Float(0.0).floatValue();
    }
    

    public void initialize(InputSplit split, TaskAttemptContext context) {
        System.out.println(" ---------------------------------- initialize -----------------------------------");
        OkuyamaClient client = new OkuyamaClient();

        try {
            this.split = (OkuyamaInputSplit)split;

            client.setConnectionInfos(this.split.getLocations());
            client.autoConnect();
            this.resultData = client.getTagValues(this.split.getTag());

            if (this.resultData != null) {
                this.entrySet = this.resultData.entrySet();
                this.entryIte = this.entrySet.iterator(); 
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                client.close();
            } catch (Exception ee) {}
        }
    }

    public boolean nextKeyValue() {
        boolean ret = false;
        if (this.entryIte != null) {
            ret = this.entryIte.hasNext();
            if (ret) this.obj = (Map.Entry)this.entryIte.next();
        }

        return ret;
    }

}