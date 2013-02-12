package fuse.okuyamafs;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

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

    static {
        if (OkuyamaFilesystem.blockSize > (1024*48)) {
            parallel = 4;
            okuyamaRequestQueue = new ArrayBlockingQueue(40);
        } else if (OkuyamaFilesystem.blockSize > (1024*24)) {

            okuyamaRequestQueue = new ArrayBlockingQueue(1550);
        } else {
            okuyamaRequestQueue = new ArrayBlockingQueue(15550);
            parallel = 30;
        }
    }


    public BufferedOkuyamaClient(OkuyamaClient client) {
        this.client = client;
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
    public static void initClientMaster(OkuyamaClientFactory factory, boolean bufferedFlg) throws Exception {

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

                Object[] realClientRet = null;

                if (value == null) {
                    realClientRet = this.client.readByteValue(key);
                    if (realClientRet != null && realClientRet[0].equals("true")) {
                        value = (byte[])realClientRet[1];
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

                            break;
                        case 4 :

                            // removeValueの処理
                            // Removeは削除をokuyamaへ実行後、Removeのマーキングバッファから該当Keyを削除
                            if (BufferedOkuyamaClient.deleteBufferedDataMap.containsKey(key)) {

                                String[] removeStr = client.removeValue(key);
                                BufferedOkuyamaClient.deleteBufferedDataMap.remove(key);
                            }
                            break;
                    }
                }
            } catch (Exception ee) {
                ee.printStackTrace();
                System.exit(1);
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