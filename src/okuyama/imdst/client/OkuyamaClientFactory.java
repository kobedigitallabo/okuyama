package okuyama.imdst.client;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;



/**
 * OkuyamaClient用のコネクションプール.<br>
 * 本クラスを利用してOkuyamaClientの管理を行うことで、<br>
 * Clientのプーリングが行われ、接続のコスト削減につながる.<br>
 * <br>
 * 利用方法)<br>
 * // MasterNodeの接続情報を作成して、OkuyamaClientFactory.getFactoryに渡すことでコネクションプールを取得<br>
 * // ここで取得されるインスタンスはシングルトンとなり全てのスレッドで唯一となる<br>
 * // スレッド毎に新しいFactoryを利用したい場合はgetNewFactoryメソッドを利用する<br>
 * String[] masterNodes = {"192.168.1.1:8888","192.168.1.2:8888"};<br>
 * OkuyamaClientFactory factory = OkuyamaClientFactory.getFactory(masterNodes, 20);<br>
 * <br>
 * // プールからClientを取得<br>
 * OkuyamaClient okuyamaClient = factory.getClient();<br>
 * <br>
 * // 以降は通常のOkuyamaClientの利用方法と同様<br>
 * String[] getResult = okuyamaClient.getValue("Key-XXXX");<br>
 * if (getResult[0].equals("true")) {<br>
 *  System.out.println(getResult[1]);<br>
 * }<br>
 * <br>
 * // close()を呼び出すことでコネクションプールに返却される<br>
 * okuyamaClient.close();<br>
 *<br>
 * // アプリケーションそのものを終了する際は以下でFactoryを終了させて全てのコネクションを破棄する<br>
 * // 終了後も再度getFactoryを呼び出せば新たにfactoryが再生成される<br>
 * factory.shutdown();<br>
 *
 * <br>
 * <br>
 * <br>
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class OkuyamaClientFactory {

    private String[] masterNodeInfos = null;
    private String singleMasterNodeAddr = null;
    private int singleMasterNodePort = -1;
    private int maxClients = 100;
    private int maxUseCount = 10000;
    boolean shutdownFlg = false;

    private ArrayBlockingQueue clientQueue = null;

    private ReadWriteLock rwLock = new ReentrantReadWriteLock();

    public static Object lock = new Object();
    public static ConcurrentHashMap factoryMap = new ConcurrentHashMap(32, 32, 64);



    // インスタンス化不可
    private OkuyamaClientFactory() {}


    /**
     * OkuyamaClientのプールを取得する.<br>
     * 本メソッドは接続先情報単位で唯一のFactoryのインスタンスが返される.<br>
     * 本メソッドから取得したOkuyamaClientFactoryのインスタンスからgetClientにてOkuyamaClientを取得して利用する.<br>
     *
     * @param masterNodeInfos 接続するMasterNodeの情報<br>1つで複数でも、配列にてMasterNodeの情報を指定する。フォーマットは"アドレス:Port"である<br>例){"192.168.1:1:8888", "192.168.1.2:8888", "192.168.1.3:8888"}
     * @param maxClients 作成されるコネクションプール上でプーリングされる最大数
     * @throws OkuyamaClientException MasterNodeの指定間違い
     */
    public static OkuyamaClientFactory getFactory(String[] masterNodeInfos, int maxClients) throws OkuyamaClientException {

        if (masterNodeInfos == null || masterNodeInfos.length < 1) throw new OkuyamaClientException("The connection information on MasterNode is not set up");
        OkuyamaClientFactory me = null;

        // 既存のFactoryが存在する場合はそちらを返す
        me = getExistingFactory(masterNodeInfos);
        if (me != null) return me;

        // 既存のFactoryが存在しない
        synchronized (lock) {
            me = getExistingFactory(masterNodeInfos);
            if (me != null) return me;

            me = new OkuyamaClientFactory();

            if (masterNodeInfos.length > 1) {
                me.masterNodeInfos = masterNodeInfos;
            } else {
                try {
                    String[] info = masterNodeInfos[0].split(":");
                    me.singleMasterNodeAddr = info[0];
                    me.singleMasterNodePort = Integer.parseInt(info[1]);
                } catch(Exception e) {
                    throw new OkuyamaClientException(e);
                }
            }

            me.clientQueue = new ArrayBlockingQueue(maxClients);
            StringBuilder buf = new StringBuilder();
            String poolKey = null;
            for (int i = 0; i < masterNodeInfos.length; i++) {
                buf.append(masterNodeInfos[i]);
                buf.append(",");
            }
            poolKey = buf.toString();
            factoryMap.put(poolKey, me);
        }
        return me;
    }


    /**
     * OkuyamaClientのプールを取得する.<br>
     * !!本メソッドは呼び出し毎にコネクションプールが作成される!!.<br>
     * 本メソッドから取得したOkuyamaClientFactoryのインスタンスからgetClientにてOkuyamaClientを取得して利用する.<br>
     *
     * @param masterNodeInfos 接続するMasterNodeの情報<br>1つで複数でも、配列にてMasterNodeの情報を指定する。フォーマットは"アドレス:Port"である<br>例){"192.168.1:1:8888", "192.168.1.2:8888", "192.168.1.3:8888"}
     * @param maxClients 作成されるコネクションプール上でプーリングされる最大数
     * @throws OkuyamaClientException MasterNodeの指定間違い
     */
    public static OkuyamaClientFactory getNewFactory(String[] masterNodeInfos, int maxClients) throws OkuyamaClientException {

        OkuyamaClientFactory me = new OkuyamaClientFactory();

        if (masterNodeInfos.length > 1) {
            me.masterNodeInfos = masterNodeInfos;
        } else {
            try {
                String[] info = masterNodeInfos[0].split(":");
                me.singleMasterNodeAddr = info[0];
                me.singleMasterNodePort = Integer.parseInt(info[1]);
            } catch(Exception e) {
                throw new OkuyamaClientException(e);
            }
        }

        me.clientQueue = new ArrayBlockingQueue(maxClients);
        return me;
    }


    private static OkuyamaClientFactory getExistingFactory(String[] masterNodeInfos) {
        OkuyamaClientFactory me = null;
        StringBuilder chkBuf = new StringBuilder();
        String chkStr = null;
        for (int i = 0; i < masterNodeInfos.length; i++) {
            chkBuf.append(masterNodeInfos[i]);
            chkBuf.append(",");
        }
        chkStr = chkBuf.toString();
        if (factoryMap.containsKey(chkStr)) me = (OkuyamaClientFactory)factoryMap.get(chkStr);
        if (me != null && me.shutdownFlg == true) me = null;
        return me;
    }


    /**
     * OkuyamaClientを取得する.<br>
     * 内部的に接続状態の確認を行ったのちに返却される<br>
     * プールに1つもOkuyamaClientが存在しない場合は新規に作成されて返される<br>
     * 本メソッドから取得したOkuyamaClientのcloseメソッドを呼び出すことでプールに返却される<br>
     *
     * @throws OkuyamaClientException 有効なOkuyamaClientの返却に失敗
     */
    public OkuyamaClient getClient() throws OkuyamaClientException {

        return this.getClient(false);
    }


    /**
     * OkuyamaClientを取得する.<br>
     * 内部的に接続状態の確認を行ったのちに返却される<br>
     * プールに1つもOkuyamaClientが存在しない場合は新規に作成されて返される<br>
     * 本メソッドから取得したOkuyamaClientのcloseメソッドを呼び出すことでプールに返却される<br>
     *
     * @throws OkuyamaClientException 有効なOkuyamaClientの返却に失敗
     */
    public OkuyamaClient getClient(boolean noCheck) throws OkuyamaClientException {

        OkuyamaClient client = null;

        try {
            this.rwLock.readLock().lock();
            if (shutdownFlg) throw new OkuyamaClientException("Since shutdown is already called, it cannot use");

            client = (OkuyamaClient)this.clientQueue.poll();
            if (client != null) {

                if (((ClientRedirector)client).useCount > this.maxUseCount) {
                    try {

                        ((ClientRedirector)client).clientClose();
                    } catch (Exception innerE) {}
                    client = null;
                } else {

                    if (noCheck == false) {

                        try {
                            client.getOkuyamaVersion();
                        } catch (Exception innerE) {
                            client = null;
                        }
                    }
                }
            }

            if (client == null) {

                client = new ClientRedirector(this);
                ((ClientRedirector)client).createTime = System.currentTimeMillis();

                if (this.singleMasterNodeAddr != null) {
                    client.connect(this.singleMasterNodeAddr, this.singleMasterNodePort);
                } else {
                    client.setConnectionInfos(this.masterNodeInfos);
                    client.autoConnect();
                }
            }
            ((ClientRedirector)client).incrUseCount();
            ((ClientRedirector)client).lastUseTime = System.currentTimeMillis();
            ((ClientRedirector)client).returnFlg = false;
        } catch (Exception e) {
            throw new OkuyamaClientException(e);
        } finally {
            this.rwLock.readLock().unlock();
        }

        return client;
    }



    /**
     * Client返却メソッド.<br>
     * クライアント利用者が呼び出す必要はなく、okuyama.imdst.client.ClientRedirectorが呼び出す<br>
     *
     * @param client
     */
    void returnConnect(ClientRedirector client) {

        try {
            this.rwLock.readLock().lock();
            if (this.shutdownFlg) {
                client.clientClose();
                return;
            }

            boolean closeFlg = false;

            if (!this.clientQueue.offer(client)) {
                closeFlg = true;
            }

            if (closeFlg) {
                try {
                    client.clientClose();
                } catch (Exception inner) {
                    client = null;
                }
            }
        } catch(Exception e) {
        } finally {
            this.rwLock.readLock().unlock();
        }
    } 


    /**
     * Factoryを停止します.<br>
     * 本メソッドを呼び出した後のFactoryインスタンスは利用できなくなります。<br>
     *
     * @param client
     */
    public void shutdown() {

        try {
            this.rwLock.writeLock().lock();
            this.shutdownFlg = true;
            OkuyamaClient client = null;
            while ((client = (OkuyamaClient)this.clientQueue.poll()) != null) {
                try {
                    ((ClientRedirector)client).clientClose();
                } catch (Exception inner) {
                    // 無視
                }
            }
        } finally {
            this.rwLock.writeLock().unlock();
        }
    } 
}