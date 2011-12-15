package okuyama.imdst.client;

import java.util.*;

import java.util.concurrent.ArrayBlockingQueue;


public class OkuyamaClientFactory {

    private String[] masterNodeInfos = null;
    private String singleMasterNodeAddr = null;
    private int singleMasterNodePort = -1;
    private int maxClients = 100;

    private ArrayBlockingQueue clientQueue = null;

    public Object lock = new Object();


    // インスタンス化不可
    private OkuyamaClientFactory() {}


    public static OkuyamaClientFactory getFactory(String[] masterNodeInfos, int maxClients) throws OkuyamaClientException {

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


    public OkuyamaClient getClient() throws OkuyamaClientException {

        OkuyamaClient client = null;

        try {
            client = (OkuyamaClient)this.clientQueue.poll();
            if (client != null) {

                if (((ClientRedirector)client).useCount > 10000) {
                    try {

                        ((ClientRedirector)client).clientClose();
                    } catch (Exception innerE) {}
                    client = null;
                } else {

                    try {
                        client.getOkuyamaVersion();
                    } catch (Exception innerE) {
                        client = null;
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
        } catch (Exception e) {
            throw new OkuyamaClientException(e);
        }

        ((ClientRedirector)client).incrUseCount();
        ((ClientRedirector)client).lastUseTime = System.currentTimeMillis();
        ((ClientRedirector)client).returnFlg = false;
        return client;
    }


    public void returnConnect(ClientRedirector client) {

        boolean closeFlg = false;

        if (!this.clientQueue.offer(client)) {
            closeFlg = true;
        }

        if (closeFlg) {
            try {
                client.clientClose();
            } catch (Exception e) {
                client = null;
            }
        }
    } 
}