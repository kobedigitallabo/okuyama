package org.imdst.helper;

import java.io.*;
import java.util.*;
import java.net.*;

import org.batch.lang.BatchException;
import org.batch.job.AbstractHelper;
import org.batch.job.IJob;
import org.batch.util.ILogger;
import org.batch.util.LoggerFactory;
import org.imdst.util.KeyMapManager;
import org.imdst.util.ImdstDefine;
import org.imdst.util.DataDispatcher;
import org.imdst.util.StatusUtil;
/**
 * Node間の接続プールを管理するクラス<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class ConnectionPoolHelper extends AbstractMasterManagerHelper {

    // ノードの監視サイクル時間(ミリ秒)
    private int connectCycle = 1000;

    private ArrayList nodeList = null;

    /**
     * Logger.<br>
     */
    private static ILogger logger = LoggerFactory.createLogger(ConnectionPoolHelper.class);

    // 初期化メソッド定義
    public void initHelper(String initValue) {
    }

    // Jobメイン処理定義
    public String executeHelper(String optionParam) throws BatchException {
        logger.debug("ConnectionPoolHelper - executeHelper - start");
        String ret = SUCCESS;
        String serverStopMarkerFileName = null;
        File serverStopMarkerFile = null;

        boolean serverRunning = true;

        int counter = 0;
        int maxConnCnt = new Integer(super.getPropertiesValue("KeyNodeConnectionPoolMax")).intValue();
        int maxNodeConnCnt = new Integer(super.getPropertiesValue("OneKeyNodeConnectionPoolMax")).intValue();

        try{
            while (serverRunning) {

                this.nodeList = new ArrayList();

                HashMap allNodeInfo = DataDispatcher.getAllDataNodeInfo();
                ArrayList mainNodeList = (ArrayList)allNodeInfo.get("main");
                ArrayList subNodeList = (ArrayList)allNodeInfo.get("sub");
                ArrayList transactionManagerList = DataDispatcher.getTransactionManagerInfo();

                if (mainNodeList != null) {
                    for (int i = 0; i < mainNodeList.size(); i++) {
                        this.nodeList.add(mainNodeList.get(i));
                    }
                }

                if (subNodeList != null) {
                    for (int i = 0; i < subNodeList.size(); i++) {
                        this.nodeList.add(subNodeList.get(i));
                    }
                }

                if (transactionManagerList != null) {
                    for (int i = 0; i < transactionManagerList.size(); i++) {
                        this.nodeList.add(transactionManagerList.get(i));
                    }
                }

                // 停止ファイル関係チェック
                if (StatusUtil.getStatus() == 1) {
                    serverRunning = false;
                    logger.info("ConnectionPoolHelper - 状態異常です");
                }

                if (StatusUtil.getStatus() == 2) {
                    serverRunning = false;
                    logger.info("ConnectionPoolHelper - 終了状態です");
                }

                serverStopMarkerFileName = super.getPropertiesValue("ServerStopFile");

                serverStopMarkerFile = new File(new File(serverStopMarkerFileName).getAbsolutePath());
                if (serverStopMarkerFile.exists()) {
                    serverRunning = false;
                    logger.info("ConnectionPoolHelper - Server停止ファイルが存在します");
                    StatusUtil.setStatus(2);
                }

                // ノード数分処理実行
                counter = super.getNowConnectionPoolCount();

                for (int i = 0; i < maxNodeConnCnt; i++) {
                    if (counter >= maxConnCnt) break;

                    for (int j = 0; j < this.nodeList.size(); j++) {

                        String nodeInfo = (String)nodeList.get(j);
                        Object[] connectionMap = this.createNodeConnection(nodeInfo);
                        if (connectionMap != null) {
                            super.setActiveConnection(nodeInfo, connectionMap);
                        }
                        counter++;
                        if (counter >= maxConnCnt) break;
                    }
                    if (counter >= maxConnCnt) break;
                }

                Thread.sleep(connectCycle);
            }
        } catch(Exception e) {
            logger.error("ConnectionPoolHelper - executeHelper - Error", e);
            throw new BatchException(e);
        }

        logger.debug("ConnectionPoolHelper - executeHelper - end");
        return ret;
    }


    /**
     * Helper後処理.<br>
     *
     */
    public void endHelper() {
    }


    /**
     * ノードに対しての接続を作成する
     *
     */
    private Object[] createNodeConnection(String nodeInfo) {
        Object[] ret = null;
        HashMap dtMap = null;
        Long connectTime = null;
        String[] nodeInfos = nodeInfo.split(":");

        try {

            InetSocketAddress inetAddr = new InetSocketAddress(nodeInfos[0], Integer.parseInt(nodeInfos[1]));
            Socket socket = new Socket();
            socket.connect(inetAddr, ImdstDefine.nodeConnectionOpenTimeout);
            socket.setSoTimeout(ImdstDefine.nodeConnectionTimeout);

            OutputStreamWriter osw = new OutputStreamWriter(socket.getOutputStream() , ImdstDefine.keyHelperClientParamEncoding);
            PrintWriter pw = new PrintWriter(new BufferedWriter(osw));

            InputStreamReader isr = new InputStreamReader(socket.getInputStream(), ImdstDefine.keyHelperClientParamEncoding);
            BufferedReader br = new BufferedReader(isr);

            dtMap = new HashMap();

            // Socket, Writer, Readerをキャッシュ
            dtMap.put(ImdstDefine.keyNodeSocketKey, socket);
            dtMap.put(ImdstDefine.keyNodeStreamWriterKey, osw);
            dtMap.put(ImdstDefine.keyNodeStreamReaderKey, isr);

            dtMap.put(ImdstDefine.keyNodeWriterKey, pw);
            dtMap.put(ImdstDefine.keyNodeReaderKey, br);
            connectTime = new Long(System.currentTimeMillis());
            ret = new Object[2];

            ret[ImdstDefine.keyNodeConnectionMapKey] = dtMap;
            ret[ImdstDefine.keyNodeConnectionMapTime] =  connectTime;
        } catch(Exception e) {
            logger.info("Connection Pool Error = [" + nodeInfo + "]");
            ret = null;
        }
        logger.info("Connection Pool Error = [" + nodeInfo + "]");
        return ret;
    }


}