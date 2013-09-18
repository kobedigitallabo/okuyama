package fuse.okuyamafs;



import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import okuyama.imdst.client.*;
import okuyama.imdst.util.*;

/**
 * OkuyamaFuse.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class DelayStoreDaemon extends Thread {

    public static AtomicInteger nowQueueJob = new AtomicInteger(0);


    private ArrayBlockingQueue storeQueue = null;
    private String[] masterNodeInfos = null;
    private OkuyamaClient client = null;

    private ExpireCacheMap cacheMap = null;
    private Object sync = new Object();

    private OkuyamaClientFactory factory = null;

    private Map nowPutStringRequestKey = new HashMap(50000);
    private Map nowPutByteRequestKey = new HashMap(50000);
    private Map nowPutMapRequestKey = new HashMap(50000);


    public DelayStoreDaemon(String[] masterNodeInfos, int queueSize, ExpireCacheMap cacheMap, OkuyamaClientFactory factory) {
        this.masterNodeInfos = masterNodeInfos;

        this.storeQueue = new ArrayBlockingQueue(queueSize);
        this.cacheMap = cacheMap;
        this.factory = factory;
    }


    public void run() {
        System.out.println("DelayStoreDaemon-start");
        try {
            this.client = null;
            Object[] requestData = null;

            while (true) {

                try {

                    // リクエストがnullの場合だけQueueから取り出す。
                    // 正常にokuyamaに伝播した場合、nullとするからである。Exception発生時はnull化されない
                    if (requestData == null) {
                        // [0] = type(1=put, 2=remove), [1]=DataType(1=byte, 2=String, 3=Object), [2]=Key, [3]=Value

                        while (true) {
                            requestData = (Object[])this.storeQueue.poll(1000, TimeUnit.MILLISECONDS);

                            if (requestData != null) break;
                            if (OkuyamaFilesystem.jvmShutdownStatus == true) break;
                        }

                        if (requestData == null && OkuyamaFilesystem.jvmShutdownStatus == true) break;
                        if (DelayStoreDaemon.nowQueueJob.get() > 0) DelayStoreDaemon.nowQueueJob.decrementAndGet();
                    }

                    this.client = this.factory.getClient();
                    //System.out.println("qs=" + this.storeQueue.size());
                    if (requestData[0].equals("1")) {
                        if (requestData[1].equals("1")) {

                            if (requestData.length == 5) {

                                String key = (String)requestData[2];

                                // このリクエストがQueueに登録された後に同じKeyで再度リクエストがある場合、
                                // このリクエストは処理しない
                                boolean putExec = false;
                                long putTime = ((Long)requestData[4]).longValue();
                                synchronized(this.nowPutByteRequestKey) {

                                    Long lastPutTimeLong = (Long)this.nowPutByteRequestKey.get(key);
                                    if (lastPutTimeLong == null) {

                                        putExec = true;
                                        this.nowPutByteRequestKey.remove(key);
                                    } else {
                                        long lastPutTime = lastPutTimeLong.longValue();
                                        if (putTime >= lastPutTime) {
                                            putExec = true;
                                            this.nowPutByteRequestKey.remove(key);
                                        }
                                    }
                                }

                                if (putExec == true) {
                                    byte[] value = (byte[])requestData[3];
                                    this.client.sendByteValue(key, value);
                                    if (this.nowPutByteRequestKey.containsKey(key)) this.nowPutByteRequestKey.remove(key);
                                }
                            } else if (requestData.length > 5) {

                                String key = (String)requestData[2];

                                // このリクエストがQueueに登録された後に同じKeyで再度リクエストがある場合、
                                // このリクエストは処理しない
                                boolean putExec = false;
                                long putTime = ((Long)requestData[5]).longValue();
                                synchronized(this.nowPutByteRequestKey) {

                                    Long lastPutTimeLong = (Long)this.nowPutByteRequestKey.get(key);

                                    if (lastPutTimeLong == null) {

                                        putExec = true;
                                        this.nowPutByteRequestKey.remove(key);
                                    } else {
                                        long lastPutTime = lastPutTimeLong.longValue();
                                        if (putTime >= lastPutTime) {
                                            putExec = true;
                                            this.nowPutByteRequestKey.remove(key);
                                        }
                                    }
                                }

                                if (putExec) {
                                    byte[] value = (byte[])requestData[3];
                                    int realStartPoint = ((Integer)requestData[4]).intValue();
                                    long requestSetTime = ((Long)requestData[5]).longValue();
                                    Object[] replaceRet = this.client.readByteValue(key);

                                    byte[] replaceBytes = null;
                                    if (replaceRet[0].equals("true")) {
                                        // データ有り
                                        replaceBytes = (byte[])replaceRet[1];
                                        if (replaceBytes != null) {
                                            System.arraycopy(value, realStartPoint, replaceBytes, realStartPoint, (value.length - realStartPoint));
                                        } else {
                                            replaceBytes = value;
                                        }
                                    } else {
                                        replaceBytes = value;
                                    }

                                    this.client.sendByteValue(key, replaceBytes);
                                    this.cacheMap.removeStoreTmpCache(key, requestSetTime);
                                    if (this.nowPutByteRequestKey.containsKey(key)) this.nowPutByteRequestKey.remove(key);
                                }
                            }
                        } else if (requestData[1].equals("2")) {

                            String key = (String)requestData[2];
                            // このリクエストがQueueに登録された後に同じKeyで再度リクエストがある場合、
                            // このリクエストは処理しない
                            boolean putExec = false;
                            long putTime = ((Long)requestData[4]).longValue();
                            synchronized(this.nowPutStringRequestKey) {
                                Long lastPutTimeLong = (Long)this.nowPutStringRequestKey.get(key);

                                if (lastPutTimeLong == null) {

                                    putExec = true;
                                    this.nowPutStringRequestKey.remove(key);
                                } else {

                                    long lastPutTime = lastPutTimeLong.longValue();
                                    if (putTime >= lastPutTime) {
                                        putExec = true;
                                        this.nowPutStringRequestKey.remove(key);
                                    }
                                }
                            }

                            if (putExec) {
                                String value = (String)requestData[3];
                                this.client.setValue(key, value);
                            }
                        } else if (requestData[1].equals("3")) {

                            String key = (String)requestData[2];

                            // このリクエストがQueueに登録された後に同じKeyで再度リクエストがある場合、
                            // このリクエストは処理しない
                            boolean putExec = false;
                            long putTime = ((Long)requestData[4]).longValue();
                            synchronized(this.nowPutMapRequestKey) {
                                long lastPutTime = ((Long)this.nowPutMapRequestKey.get(key)).longValue();
                                if (putTime >= lastPutTime) {
                                    putExec = true;
                                    this.nowPutMapRequestKey.remove(key);
                                }
                            }

                            if (putExec) {

                                Map value = (Map)requestData[3];
                                this.client.setObjectValue(key, value);
                            }
                        }
                    } else if (requestData[0].equals("2")) {

                        String key = (String)requestData[1];
                        client.removeValue(key);
                    }
                    requestData = null;
                } catch(OkuyamaClientException oe) {
                    oe.printStackTrace();

                    try {
                        if (this.client != null) {
                            
                            this.client = null;
                        }
                    } catch(Exception innerE) {
                        innerE.printStackTrace();
                    }

                } catch(Exception innerE) {
                    innerE.printStackTrace();
                } finally {
                    try {
                        if (this.client != null) {
                            
                            this.client.close();
                        }
                    } catch(Exception innerE) {}
                }
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
        System.out.println("DelayStoreDaemon-end");
    }



    public Object putString(Object key, String value) {
        synchronized(this.sync) {
            try {
                long putTime = System.currentTimeMillis();
                Object[] request = new Object[5];
                request[0] = "1";
                request[1] = "2";
                request[2] = key;
                request[3] = value;
                request[4] = putTime;

                this.storeQueue.put(request);

                synchronized(this.nowPutStringRequestKey) {
                    this.nowPutStringRequestKey.put(key, putTime);
                }
                DelayStoreDaemon.nowQueueJob.incrementAndGet();
            } catch(Exception innerE) {
                innerE.printStackTrace();
            }
        }
        return null;
    }

    public Object putMap(Object key, Map value) {
        synchronized(this.sync) {
            try {

                long putTime = System.currentTimeMillis();
                Object[] request = new Object[5];
                request[0] = "1";
                request[1] = "3";
                request[2] = key;
                request[3] = value;
                request[4] = putTime;
                this.storeQueue.put(request);

                synchronized(this.nowPutMapRequestKey) {
                    this.nowPutMapRequestKey.put(key, putTime);
                }

                DelayStoreDaemon.nowQueueJob.incrementAndGet();
            } catch(Exception innerE) {
                innerE.printStackTrace();
            }
        }
        return null;
    }

    public Object putBytes(Object key, byte[] value) {
        synchronized(this.sync) {
            try {
                long putTime = System.currentTimeMillis();
                Object[] request = new Object[5];
                request[0] = "1";
                request[1] = "1";
                request[2] = key;
                request[3] = value;
                request[4] = putTime;

                this.storeQueue.put(request);
                synchronized(this.nowPutByteRequestKey) {
                    this.nowPutByteRequestKey.put(key, putTime);
                }
                DelayStoreDaemon.nowQueueJob.incrementAndGet();
            } catch(Exception innerE) {
                innerE.printStackTrace();
            }
        }
        return null;
    }

    public Object putBytes(Object key, byte[] value, Integer realStartPoint, long cacheSetTime) {
        synchronized(this.sync) {
            try {
                long putTime = System.currentTimeMillis();
                Object[] request = new Object[7];
                request[0] = "1";
                request[1] = "1";
                request[2] = key;
                request[3] = value;
                request[4] = realStartPoint;
                request[5] = cacheSetTime;
                request[6] = putTime;

                this.storeQueue.put(request);
                synchronized(this.nowPutByteRequestKey) {
                    this.nowPutByteRequestKey.put(key, putTime);
                }

                DelayStoreDaemon.nowQueueJob.incrementAndGet();
            } catch(Exception innerE) {
                innerE.printStackTrace();
            }
        }
        return null;
    }


    public Object remove(Object key) {
        synchronized(this.sync) {
            try {

                Object[] request = new Object[2];
                request[0] = "2";
                request[1] = key;
                this.storeQueue.put(request);
                DelayStoreDaemon.nowQueueJob.incrementAndGet();
            } catch(Exception innerE) {
                innerE.printStackTrace();
            }
        }
        return null;
    }
}
