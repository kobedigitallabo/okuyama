package okuyama.imdst.util.serializemap;

import java.util.*;
import java.io.*;

import okuyama.imdst.util.*;

public class ByteDataSerializeCustomHashMap extends HashMap implements Cloneable, Serializable {

    public byte[] byteData = null;


    public ByteDataSerializeCustomHashMap(byte[] byteData) {
        this.byteData = byteData;
    }



    public Object put(Object key, Object value) {

        Object ret = super.put(key, value);
        String keyStr = null;
        String valueInt = null;

        if (value instanceof String)  {

            keyStr = ((CoreMapKey)key).toString();
            valueInt = (String)value;
        } else {

            keyStr = ((CoreMapKey)key).toString();
            valueInt = new String((byte[])value);
        }

        String nowDataStr = new String(this.byteData);

        if (nowDataStr.indexOf(keyStr + ";") != -1) {
            String[] dataList = nowDataStr.split(",");
            StringBuilder newDataBuf = new StringBuilder();
            String sep = "";
            for (int idx = 0; idx < dataList.length; idx++) {

                String[] singleData = dataList[idx].split(";");
                if (!singleData[0].equals(key)) {
                    newDataBuf.append(sep);
                    newDataBuf.append(dataList);
                    sep = ",";
                }
            }
            byte[] cnvByteData = newDataBuf.toString().getBytes();
            boolean changeFlg = false;
            for (int i = 0; i < this.byteData.length; i++) {

                if (cnvByteData.length > i) {
                    if (this.byteData[i] != cnvByteData[i]) {
                        changeFlg = true;
                        this.byteData[i] = cnvByteData[i];
                    }
                } else {
                    this.byteData[i] = 0;
                }
            }
        }


        int chkIdx = 0;

        for (; chkIdx < this.byteData.length; chkIdx++) {
            if (this.byteData[chkIdx] == 0) break;
        }

        byte[] putFullData = null;
        if (chkIdx == 0) {
            putFullData = (keyStr + ";" + valueInt).getBytes();
        } else {
            putFullData = ("," + keyStr + ";" + valueInt).getBytes();
        }

        if (this.byteData.length < (chkIdx + putFullData.length)) {
            byte[] newByteData = new byte[chkIdx + putFullData.length + 1024];

            for (int i = 0; i < this.byteData.length; i++) {
                newByteData[i] = this.byteData[i];
            }
            this.byteData = newByteData;

            chkIdx = 0;
            for (; chkIdx < this.byteData.length; chkIdx++) {
                if (this.byteData[chkIdx] == 0) break;
            }
        }



        for (int i = 0; i < putFullData.length; i++) {
            this.byteData[chkIdx] = putFullData[i];
            chkIdx++;
        }

        return ret;
    }


    public Object remove(Object key) {
        Object ret = super.remove(key);

        String keyStr = ((CoreMapKey)key).toString();

        String nowDataStr = new String(this.byteData);

        if (nowDataStr.indexOf(keyStr + ";") != -1) {
            String[] dataList = nowDataStr.split(",");
            StringBuilder newDataBuf = new StringBuilder();
            String sep = "";
            for (int idx = 0; idx < dataList.length; idx++) {

                String[] singleData = dataList[idx].split(";");
                if (!singleData[0].equals(keyStr)) {
                    newDataBuf.append(sep);
                    newDataBuf.append(dataList[idx]);
                    sep = ",";
                }
            }

            byte[] cnvByteData = newDataBuf.toString().getBytes();
            boolean changeFlg = false;
            for (int i = 0; i < this.byteData.length; i++) {

                if (cnvByteData.length > i) {
                    if (this.byteData[i] != cnvByteData[i]) {
                        changeFlg = true;
                        this.byteData[i] = cnvByteData[i];
                    }
                } else {
                    this.byteData[i] = 0;
                }
            }
        }

        return ret;
    }

    public Object originalPut(Object key, Object value) {

        return super.put(key, value);
    }


}