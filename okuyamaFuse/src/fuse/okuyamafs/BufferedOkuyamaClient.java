package fuse.okuyamafs;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.io.*;


import okuyama.imdst.client.*;
import okuyama.imdst.util.*;


public class BufferedOkuyamaClient extends OkuyamaClient {

    protected static boolean bufferedFlg = false;

    protected static Map putBufferedDataMap = new ConcurrentHashMap(350);
    protected static Map deleteBufferedDataMap = new ConcurrentHashMap(350);


    protected static ArrayBlockingQueue okuyamaRequestQueue = null;

    protected static OkuyamaClientFactory factory = null;

    protected static OkuyamaSendWorker[] workerList = null;

    protected static Object[] sendSyncObject = null;

    protected static volatile int parallel = 10;
    protected static volatile int syncParallel = 100;

    protected OkuyamaClient client = null;

    static boolean stripingDataBlock = true;
    static int stripingLevel = 8;
    static int stripingMinBlockSize = 128;

    private OkuyamaClient[] stripingDataClient = new OkuyamaClient[4];

    static {
        if (OkuyamaFilesystem.blockSize > (1024*48)) {
            parallel = 4;
            okuyamaRequestQueue = new ArrayBlockingQueue(40);
        } else if (OkuyamaFilesystem.blockSize > (1024*24)) {
            parallel = 10;
            okuyamaRequestQueue = new ArrayBlockingQueue(500);
        } else {
            okuyamaRequestQueue = new ArrayBlockingQueue(4000);
            parallel = 16;
        }
    }


    public BufferedOkuyamaClient(OkuyamaClient client) {
        this.client = client;
        if (stripingDataBlock == true) {
            try {
                stripingDataClient[0] = BufferedOkuyamaClient.factory.getClient(300*1000);
                stripingDataClient[1] = BufferedOkuyamaClient.factory.getClient(300*1000);
                stripingDataClient[2] = BufferedOkuyamaClient.factory.getClient(300*1000);
                stripingDataClient[3] = BufferedOkuyamaClient.factory.getClient(300*1000);
            } catch (Exception e) {}
        }
    }


    public String[] setNewValue(String key, String value) throws OkuyamaClientException {
        return this.client.setNewValue(key, value);
    }

    public String[] setNewObjectValue(String key, Object value) throws OkuyamaClientException {
        return this.client.setNewObjectValue(key, value);
    }



    /**
     * 一度しか呼ばない
     *
     */
    public static void initClientMaster(OkuyamaClientFactory factory, boolean bufferedFlg, boolean stripingDataBlock) throws Exception {

        BufferedOkuyamaClient.stripingDataBlock = stripingDataBlock;
        BufferedOkuyamaClient.factory = factory;
        if (bufferedFlg == false) {
            BufferedOkuyamaClient.bufferedFlg = bufferedFlg;
        } else {
            BufferedOkuyamaClient.bufferedFlg = true;
            workerList = new OkuyamaSendWorker[parallel];
            sendSyncObject = new Object[syncParallel];

            for (int idx = 0; idx < parallel; idx++) {
                workerList[idx] = new OkuyamaSendWorker();
                workerList[idx].start();
            }
            for (int idx = 0; idx < syncParallel; idx++) {
                sendSyncObject[idx] = new Object();
            }

        }
    }



    public void close() throws OkuyamaClientException {
        this.client.close();
        if (stripingDataBlock == true) {
            try {
                stripingDataClient[0].close();
                stripingDataClient[1].close();
                stripingDataClient[2].close();
                stripingDataClient[3].close();
            } catch (Exception e) {}
        }
    }


    public boolean setValue(String key, String value) throws OkuyamaClientException {
        if (!BufferedOkuyamaClient.bufferedFlg) return this.client.setValue(key, value);

        try {

            while (true) {
                synchronized(sendSyncObject[((key.hashCode() << 1) >>> 1) % syncParallel]) {
                    Object[] request = new Object[3];
                    request[0] = new Integer(1);
                    request[1] = key;
                    request[2] = value;
                    if (okuyamaRequestQueue.offer(request)) {

                        putBufferedDataMap.put(key, value);
                        deleteBufferedDataMap.remove(key);

                        break;
                    }

                }
            }
        } catch (Exception ee) {
            throw new OkuyamaClientException(ee);
        }

        return true;
    }

    public String[] getValue(String key) throws OkuyamaClientException {
        if (!BufferedOkuyamaClient.bufferedFlg) return this.client.getValue(key);

        String[] ret = null;

        try {
            synchronized(sendSyncObject[((key.hashCode() << 1) >>> 1) % syncParallel]) {
                Object value = (Object)putBufferedDataMap.get(key);

                String[] realClientRet = null;

                if (value == null) {
                    realClientRet = this.client.getValue(key);
                    if (realClientRet != null && realClientRet[0].equals("true")) {
                        value = realClientRet[1];
                    }
                }

                if (value != null) {
                    if (deleteBufferedDataMap.containsKey(key)) {
                        value = null;
                    }
                }

                if (value == null) {

                    ret = new String[1];
                    ret[0] = "false";
                } else {

                    ret = new String[2];
                    ret[0] = "true";
                    ret[1] = value.toString();
                }
            }
        } catch (Exception ee) {
            throw new OkuyamaClientException(ee);
        }
        return ret;
    }


    public Object[] getObjectValue(String key) throws OkuyamaClientException {
        if (!BufferedOkuyamaClient.bufferedFlg) return this.client.getObjectValue(key);
        Object[] ret = null;

        try {
            synchronized(sendSyncObject[((key.hashCode() << 1) >>> 1) % syncParallel]) {
                Object value = (Object)putBufferedDataMap.get(key);

                Object[] realClientRet = null;

                if (value == null) {
                    realClientRet = this.client.getObjectValue(key);
                    if (realClientRet != null && realClientRet[0].equals("true")) {
                        value = realClientRet[1];
                    }
                }

                if (value != null) {
                    if (deleteBufferedDataMap.containsKey(key)) {
                        value = null;
                    }
                }

                if (value == null) {

                    ret = new Object[1];
                    ret[0] = "false";
                } else {

                    ret = new Object[2];
                    ret[0] = "true";
                    ret[1] = value;
                }
            }
        } catch (Exception ee) {
            throw new OkuyamaClientException(ee);
        }
        return ret;
    }


    public boolean setObjectValue(String key, Object value) throws OkuyamaClientException {
        if (!BufferedOkuyamaClient.bufferedFlg) return this.client.setObjectValue(key, value);
        try {
            while (true) {

                synchronized(sendSyncObject[((key.hashCode() << 1) >>> 1) % syncParallel]) {
                    Object[] request = new Object[3];

                    request[0] = new Integer(2);
                    request[1] = key;
                    request[2] = value;
                    if (okuyamaRequestQueue.offer(request)) {

                        putBufferedDataMap.put(key, value);
                        deleteBufferedDataMap.remove(key);
                        break;
                    }
                }
            }
        } catch (Exception ee) {
            throw new OkuyamaClientException(ee);
        }
        return true;
    }

    public boolean sendByteValue(String key, byte[] value) throws OkuyamaClientException {
        if (!BufferedOkuyamaClient.bufferedFlg) return this.client.sendByteValue(key, value);

        try {
            while (true) {
                synchronized(sendSyncObject[((key.hashCode() << 1) >>> 1) % syncParallel]) {

                    Object[] request = new Object[3];

                    request[0] = new Integer(3);
                    request[1] = key;
                    request[2] = value;
                    if (okuyamaRequestQueue.offer(request)) {

                        putBufferedDataMap.put(key, value);
                        deleteBufferedDataMap.remove(key);
                        break;
                    }
                }
            }
        } catch (Exception ee) {
            throw new OkuyamaClientException(ee);
        }

        return true;
    }




    public String[] removeValue(String key) throws OkuyamaClientException {
        if (!BufferedOkuyamaClient.bufferedFlg) return this.client.removeValue(key);
        String[] ret = null;
        try {
            while (true) {
                synchronized(sendSyncObject[((key.hashCode() << 1) >>> 1) % syncParallel]) {

                    Object[] request = new Object[2];

                    request[0] = new Integer(4);
                    request[1] = key;
                    if (okuyamaRequestQueue.offer(request)) {

                        ret = new String[2];
                        ret[0] = "true";

                        Object removeRet = putBufferedDataMap.remove(key);
                        if (removeRet == null) {
                            String[] realClientRmRet = this.client.getValue(key);
                            if (realClientRmRet[0].equals("false")) {
                                ret = new String[1];
                                ret[0] = "false";
                                return ret;
                            }
                        }
                        deleteBufferedDataMap.put(key, new Integer(1));
                        break;
                    }
                }
            }
        } catch (Exception ee) {
            throw new OkuyamaClientException(ee);
        }
        return ret;
    }


    public boolean requestRemoveValue(String key) throws OkuyamaClientException {
        if (!BufferedOkuyamaClient.bufferedFlg) return this.client.requestRemoveValue(key);
        try {
            while (true) {
                synchronized(sendSyncObject[((key.hashCode() << 1) >>> 1) % syncParallel]) {
                    Object[] request = new Object[2];

                    request[0] = new Integer(4);
                    request[1] = key;
                    if (okuyamaRequestQueue.offer(request)) {

                        putBufferedDataMap.remove(key);
                        deleteBufferedDataMap.put(key, new Integer(1));
                        break;
                    }
                }
            }
        } catch (Exception ee) {
            throw new OkuyamaClientException(ee);
        }
        return true;
    }

    public String[] responseRemoveValue(String key) throws OkuyamaClientException {
        if (!BufferedOkuyamaClient.bufferedFlg) return this.client.responseRemoveValue(key);
        String[] ret = null;

        try {
            int i = 0;
            while (true) {
                i++;
                synchronized(sendSyncObject[((key.hashCode() << 1) >>> 1) % syncParallel]) {
                    if (deleteBufferedDataMap.containsKey(key) == false) break;
                    if ((i % 100) == 0) Thread.sleep(10);
                }
            }
            ret = new String[2];
            ret[0] = "true";
        } catch (Exception ee) {
            throw new OkuyamaClientException(ee);
        }
        return ret;
    }


    public Object[] readByteValue(String key) throws OkuyamaClientException {
        if (!BufferedOkuyamaClient.bufferedFlg) return this.client.readByteValue(key);
        Object[] ret = null;

        try {
            synchronized(sendSyncObject[((key.hashCode() << 1) >>> 1) % syncParallel]) {
                byte[] value = (byte[])putBufferedDataMap.get(key);

                if (value == null) {

                    // Stripingの場合で処理がことなる
                    if (BufferedOkuyamaClient.stripingDataBlock == false) {
                        Object[] realClientRet = this.client.readByteValue(key);

                        if (realClientRet != null && realClientRet[0].equals("true")) {

                            value = (byte[])realClientRet[1];
                        }
                    } else {
                        // StripingBlock;
                        value = this.readStripingBlock(key);
                        if (value.length == 0) value =  null;
                    }
                }

                if (value != null) {
                    if (deleteBufferedDataMap.containsKey(key)) {
                        value = null;
                    }
                }

                if (value == null) {

                    ret = new Object[1];
                    ret[0] = "false";
                } else {
                    ret = new Object[2];
                    ret[0] = "true";
                    ret[1] = value;
                }
            }
        } catch (Exception ee) {
            throw new OkuyamaClientException(ee);
        }
        return ret;
    }

    private byte[] readStripingBlock(String key) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try {

            int strpngIndex = 0;
            for (int idx = 0; idx < BufferedOkuyamaClient.stripingLevel / 4; idx++) {
                stripingDataClient[0].requestReadByteValue(key+"\t"+strpngIndex);
                strpngIndex++;
                stripingDataClient[1].requestReadByteValue(key+"\t"+strpngIndex);
                strpngIndex++;
                stripingDataClient[2].requestReadByteValue(key+"\t"+strpngIndex);
                strpngIndex++;
                stripingDataClient[3].requestReadByteValue(key+"\t"+strpngIndex);
                strpngIndex++;

                boolean endRead = false;
                
                Object[] stripingRet1 = stripingDataClient[0].responseReadByteValue(key+"\t"+(strpngIndex - 4));
                if (endRead == false && stripingRet1[0].equals("true")) {
                    baos.write((byte[])stripingRet1[1]);
                } else {
                    endRead = true;
                }

                Object[] stripingRet2 = stripingDataClient[1].responseReadByteValue(key+"\t"+(strpngIndex - 3));
                if (endRead == false && stripingRet2[0].equals("true")) {
                    baos.write((byte[])stripingRet2[1]);
                } else {
                    endRead = true;
                }

                Object[] stripingRet3 = stripingDataClient[2].responseReadByteValue(key+"\t"+(strpngIndex - 2));
                if (endRead == false && stripingRet3[0].equals("true")) {
                    baos.write((byte[])stripingRet3[1]);
                } else {
                    endRead = true;
                }

                Object[] stripingRet4 = stripingDataClient[3].responseReadByteValue(key+"\t"+(strpngIndex - 1));
                if (endRead == false && stripingRet4[0].equals("true")) {
                    baos.write((byte[])stripingRet4[1]);
                } else {
                    endRead = true;
                }

                if (endRead == true) break;
            }

        } catch (Exception e) {
            throw e;
        }
        return baos.toByteArray();
    }
}

class OkuyamaSendWorker extends Thread {


    public void run () {

        OkuyamaClient client = null;
        Object[] requestData = null;


        while (true) {

            try {

                // リクエストがnullの場合だけQueueから取り出す。
                // 正常にokuyamaに伝播した場合、nullとするからである。Exception発生時はnull化されない
                if (requestData == null) {
                    // [0] = type(1=put, 2=remove), [1]=DataType(1=byte, 2=String, 3=Object), [2]=Key, [3]=Value

                    while (true) {
                        requestData = (Object[])BufferedOkuyamaClient.okuyamaRequestQueue.poll(1000, TimeUnit.MILLISECONDS);

                        if (requestData != null) break;
                        if (OkuyamaFilesystem.jvmShutdownStatus == true) break;
                    }

                    if (requestData == null && OkuyamaFilesystem.jvmShutdownStatus == true) break;
                }

                client = BufferedOkuyamaClient.factory.getClient(300*1000);


                String key = (String)requestData[1];

                synchronized(BufferedOkuyamaClient.sendSyncObject[((key.hashCode() << 1) >>> 1) % BufferedOkuyamaClient.syncParallel]) {

                    int method = ((Integer)requestData[0]).intValue();

                    switch (method) {
                        case 1 :
                            // setValueの処理
                            String nowBufferedValueStr = (String)BufferedOkuyamaClient.putBufferedDataMap.get(key);
                            String requestValueStr = (String)requestData[2];

                            // 現在バッファ中のObjectのアドレスと登録QueueのObjectのアドレスが同値の場合は
                            // 登録する意味がある。異なる場合は後続のリクエストに上書きされているので、
                            // いづれそちらが行われるので反映しても無駄となる。
                            // 削除処理がこのJobの後にQueueに入った場合も、バッファが削除されているので、
                            // 反映しても無駄である
                            try {
                                if (nowBufferedValueStr == requestValueStr) {
                                    if (client.setValue(key ,requestValueStr)) {

                                        BufferedOkuyamaClient.putBufferedDataMap.remove(key);
                                    } else {
                                        client = null;
                                        client = BufferedOkuyamaClient.factory.getClient();

                                        if (client.setValue(key ,requestValueStr)) {
                                            BufferedOkuyamaClient.putBufferedDataMap.remove(key);

                                        } else {
                                            throw new Exception("setValue - error");
                                        }
                                    }
                                }
                            } catch (Exception setE) {
                                try {
                                    if (client != null )client.close();
                                } catch (Exception setEVC) {
                                }
                                Thread.sleep(500);
                                client = null;
                                client = BufferedOkuyamaClient.factory.getClient();
                                if (client.setValue(key ,requestValueStr)) {
                                    BufferedOkuyamaClient.putBufferedDataMap.remove(key);
                                } else {
                                    throw new Exception("setValue - error");
                                }
                            }
                            break;
                        case 2 :

                            // setObjectValueの処理
                            Object nowBufferedValueObj = BufferedOkuyamaClient.putBufferedDataMap.get(key);
                            Object requestValueObj = requestData[2];

                            // 現在バッファ中のObjectのアドレスと登録QueueのObjectのアドレスが同値の場合は
                            // 登録する意味がある。異なる場合は後続のリクエストに上書きされているので、
                            // いづれそちらが行われるので反映しても無駄となる。
                            // 削除処理がこのJobの後にQueueに入った場合も、バッファが削除されているので、
                            // 反映しても無駄である
                            try {
                                if (nowBufferedValueObj == requestValueObj) {
                                    if (client.setObjectValue(key ,requestValueObj)) {

                                        BufferedOkuyamaClient.putBufferedDataMap.remove(key);
                                    } else {
                                        client = null;
                                        client = BufferedOkuyamaClient.factory.getClient();
                                        if (client.setObjectValue(key ,requestValueObj)) {
                                            BufferedOkuyamaClient.putBufferedDataMap.remove(key);
                                        } else {
                                            throw new Exception("setObjectValue - error");
                                        }
                                    }
                                }
                            } catch (Exception setOE) {
                                try {
                                    if (client != null )client.close();
                                } catch (Exception setOEC) {
                                }

                                Thread.sleep(500);
                                client = null;
                                client = BufferedOkuyamaClient.factory.getClient();
                                if (client.setObjectValue(key ,requestValueObj)) {
                                    BufferedOkuyamaClient.putBufferedDataMap.remove(key);
                                } else {
                                    throw new Exception("setObjectValue - error");
                                }
                            }

                            break;
                        case 3 :

                            // sendByteValueの処理
                            byte[] nowBufferedValueBytes = (byte[])BufferedOkuyamaClient.putBufferedDataMap.get(key);
                            byte[] requestValueBytes = (byte[])requestData[2];

                            // 現在バッファ中のObjectのアドレスと登録QueueのObjectのアドレスが同値の場合は
                            // 登録する意味がある。異なる場合は後続のリクエストに上書きされているので、
                            // いづれそちらが行われるので反映しても無駄となる。
                            // 削除処理がこのJobの後にQueueに入った場合も、バッファが削除されているので、
                            // 反映しても無駄である

                            // StripingModeの場合とそれ以外で処理が異なる
                            if (BufferedOkuyamaClient.stripingDataBlock == false) {
                                // Stripingではない
                                try {
                                    if (nowBufferedValueBytes == requestValueBytes) {

                                        if (client.sendByteValue(key ,requestValueBytes)) {

                                            BufferedOkuyamaClient.putBufferedDataMap.remove(key);
                                        } else {

                                            client = null;
                                            client = BufferedOkuyamaClient.factory.getClient();
                                            if (client.sendByteValue(key ,requestValueBytes)) {
                                                BufferedOkuyamaClient.putBufferedDataMap.remove(key);
                                            } else {
                                                throw new Exception("sendByteValue - error");
                                            }
                                        }
                                    }
                                } catch (Exception sendBE) {
                                    try {
                                        if (client != null )client.close();
                                    } catch (Exception sendBEC) {
                                    }
                                    Thread.sleep(500);
                                    client = null;
                                    client = BufferedOkuyamaClient.factory.getClient();
                                    if (client.sendByteValue(key ,requestValueBytes)) {
                                        BufferedOkuyamaClient.putBufferedDataMap.remove(key);
                                    } else {
                                        throw new Exception("sendByteValue - error");
                                    }
                                }

                                break;
                            } else {

                                // Striping
                                if (requestValueBytes.length < BufferedOkuyamaClient.stripingMinBlockSize) {

                                    try {
                                        if (nowBufferedValueBytes == requestValueBytes) {


                                            if (client.sendByteValue(key + "\t0",requestValueBytes)) {
                                                for (int idx = 1; idx < BufferedOkuyamaClient.stripingLevel; idx++) {
                                                    client.removeValue(key + "\t" + idx);
                                                }
                                                BufferedOkuyamaClient.putBufferedDataMap.remove(key);
                                            } else {

                                                client = null;
                                                client = BufferedOkuyamaClient.factory.getClient();
                                                if (client.sendByteValue(key + "\t0" ,requestValueBytes)) {
                                                    for (int idx = 1; idx < BufferedOkuyamaClient.stripingLevel; idx++) {
                                                        client.removeValue(key + "\t" + idx);
                                                    }

                                                    BufferedOkuyamaClient.putBufferedDataMap.remove(key);
                                                } else {
                                                    throw new Exception("sendByteValue - error");
                                                }
                                            }
                                        }
                                    } catch (Exception sendBE) {
                                        try {
                                            if (client != null )client.close();
                                        } catch (Exception sendBEC) {
                                        }
                                        Thread.sleep(500);
                                        client = null;
                                        client = BufferedOkuyamaClient.factory.getClient();
                                        if (client.sendByteValue(key + "\t0" ,requestValueBytes)) {
                                            for (int idx = 1; idx < BufferedOkuyamaClient.stripingLevel; idx++) {
                                                client.removeValue(key + "\t" + idx);
                                            }

                                            BufferedOkuyamaClient.putBufferedDataMap.remove(key);
                                        } else {
                                            throw new Exception("sendByteValue - error");
                                        }
                                    }
                                } else {

                                    // Stripingにするサイズである
                                    try {
                                        if (nowBufferedValueBytes == requestValueBytes) {

                                            OkuyamaClient[] stripingDataClient = new OkuyamaClient[BufferedOkuyamaClient.stripingLevel];
                                            stripingDataClient[0] = client;
                                            for (int strpngIndex = 1; strpngIndex < BufferedOkuyamaClient.stripingLevel; strpngIndex++) {
                                                stripingDataClient[strpngIndex] = BufferedOkuyamaClient.factory.getClient(300*1000);
                                            }

                                            int oneStripingSize = requestValueBytes.length / BufferedOkuyamaClient.stripingLevel;
                                            int lastStripingSize = oneStripingSize + (requestValueBytes.length % BufferedOkuyamaClient.stripingLevel);

                                            Object[] stripingBlockList = new Object[BufferedOkuyamaClient.stripingLevel];

                                            for (int idx = 0; idx < BufferedOkuyamaClient.stripingLevel; idx++) {
                                                byte[] stripingBlock = null;

                                                if (idx == (BufferedOkuyamaClient.stripingLevel - 1)) {
                                                    stripingBlock = new byte[lastStripingSize];
                                                } else {
                                                    stripingBlock = new byte[oneStripingSize];
                                                }

                                                System.arraycopy(requestValueBytes, (idx*oneStripingSize), stripingBlock, 0, stripingBlock.length);
                                                stripingDataClient[idx].requestByteValue(key + "\t" + idx, stripingBlock);
                                                stripingBlockList[idx] = stripingBlock;
                                            }

                                            boolean allStipingBlockSave = true;
                                            for (int idx = 0; idx < BufferedOkuyamaClient.stripingLevel; idx++) {

                                                if (!stripingDataClient[idx].responseByteValue(key + "\t" + idx, (byte[])stripingBlockList[idx])) {

                                                    allStipingBlockSave = false;
                                                    Thread.sleep(300);

                                                    if (idx == 0) {
                                                        client = null;
                                                        stripingDataClient[idx] = null;
                                                        stripingDataClient[idx] = BufferedOkuyamaClient.factory.getClient();
                                                        client = stripingDataClient[idx];
                                                    } else {

                                                        stripingDataClient[idx] = null;
                                                        stripingDataClient[idx] = BufferedOkuyamaClient.factory.getClient();
                                                    }
                                                    if (stripingDataClient[idx].sendByteValue(key + "\t" + idx , (byte[])stripingBlockList[idx])) {

                                                        allStipingBlockSave = true;
                                                    } else {
                                                        throw new Exception("sendByteValue - error");
                                                    }
                                                } else {
                                                    if (idx != 0) {
                                                        stripingDataClient[idx].close();
                                                        stripingDataClient[idx] = null;
                                                    }
                                                }
                                            }

                                            if (allStipingBlockSave) {
                                                BufferedOkuyamaClient.putBufferedDataMap.remove(key);
                                            } else {
                                                throw new Exception("sendByteValue - error");
                                            }
                                        }
                                    } catch (Exception sendBE) {
                                        throw new Exception("sendByteValue - error");
                                    }
                                }
                                break;
                            }
                        case 4 :

                            // removeValueの処理
                            // Removeは削除をokuyamaへ実行後、Removeのマーキングバッファから該当Keyを削除
                            if (BufferedOkuyamaClient.deleteBufferedDataMap.containsKey(key)) {
                                if (BufferedOkuyamaClient.stripingDataBlock == false) {
                                    try {

                                        String[] removeStr = client.removeValue(key);
                                        BufferedOkuyamaClient.deleteBufferedDataMap.remove(key);
                                    } catch (Exception removeE) {
                                        try {
                                            if (client != null )client.close();
                                        } catch (Exception removeEC) {
                                        }
                                        Thread.sleep(500);
                                        client = null;
                                        client = BufferedOkuyamaClient.factory.getClient();
                                        String[] removeStr = client.removeValue(key);
                                        BufferedOkuyamaClient.deleteBufferedDataMap.remove(key);
                                    }
                                } else {

                                    String[] removeStr = client.removeValue(key);

                                    // 以降StripingBlcokの処理
                                    for (int idx = 0; idx < BufferedOkuyamaClient.stripingLevel; idx++) {
                                        
                                        try {

                                            removeStr = client.removeValue(key + "\t" + idx);
                                        } catch (Exception removeE) {
                                            try {
                                                if (client != null )client.close();
                                            } catch (Exception removeEC) {
                                            }
                                            Thread.sleep(500);
                                            client = null;
                                            client = BufferedOkuyamaClient.factory.getClient();
                                            removeStr = client.removeValue(key + "\t" + idx);
                                        }
                                    }
                                    BufferedOkuyamaClient.deleteBufferedDataMap.remove(key);
                                } 
                            }
                            break;
                    }
                }
            } catch (Exception ee) {
                ee.printStackTrace();
            } finally {
                try {
                    client.close();
                } catch (Exception e2) {
                }
                requestData = null;
            }
        }
    }
}