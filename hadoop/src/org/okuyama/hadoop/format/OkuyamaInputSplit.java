package org.okuyama.hadoop.format;

import java.io.*;
import java.util.*;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;

public class OkuyamaInputSplit extends InputSplit  implements Writable{

    private String tag = "";

    private String[] masterNodes = null;

    public OkuyamaInputSplit() {}

    public OkuyamaInputSplit(String tagName, String[] masterNodeInfos) {

        this.tag = tagName;
        this.masterNodes = masterNodeInfos;
    }

    public String getTag() {
        return this.tag;
    }

    public long getLength() {

        return Long.MAX_VALUE;
    }

    public String[] getLocations() {

        return this.masterNodes;
    }

    public String toString() {
        StringBuilder masterNodeBuf = new StringBuilder(20);
        if (masterNodes != null) {
            String sep = "";
            for (int i = 0; i < this.masterNodes.length; i++) {
                masterNodeBuf.append(sep);
                masterNodeBuf.append(masterNodes[i]);
                sep = ", ";
            }
        }
        return "tag=[" + tag + "] masternode=[" + masterNodeBuf.toString() + "]";
    }

    /**
     * 
     */
    public void write(DataOutput out) throws IOException
    {
        System.out.println("---------------- write -------------------");
        out.writeUTF(this.tag);

        StringBuilder masterNodeBuf = new StringBuilder(20);
        if (this.masterNodes != null) {
            String sep = "";
            for (int i = 0; i < this.masterNodes.length; i++) {
                masterNodeBuf.append(sep);
                masterNodeBuf.append(masterNodes[i]);
                sep = ",";
            }
        }
        out.writeUTF(masterNodeBuf.toString());
    }

    public void readFields(DataInput in) throws IOException
    {
        System.out.println("---------------- readFields -------------------");
        this.tag = in.readUTF();

        String masterNodeInfos = in.readUTF();
        if (masterNodeInfos != null) {
            this.masterNodes = masterNodeInfos.split(",");
        }
    }
}