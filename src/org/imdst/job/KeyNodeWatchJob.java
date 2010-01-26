package org.imdst.job;

import java.io.*;
import java.net.*;
import java.util.*;

import org.batch.lang.BatchException;
import org.batch.job.AbstractJob;
import org.batch.job.AbstractHelper;
import org.batch.job.IJob;
import org.batch.util.ILogger;
import org.batch.util.LoggerFactory;
import org.imdst.util.KeyMapManager;
import org.imdst.util.StatusUtil;
import org.imdst.util.ImdstDefine;
import org.imdst.util.DataDispatcher;
import org.imdst.util.JavaSystemApi;

/**
 * KeyNodeを監視して生存確認を行う.<br>
 * KeyNodeが停止している場合はそのNodeの復帰を待ち、復帰後、そのNodeとペアーのノードからデータ転送を行う.<br>
 *
 * Serverソケット関係の終了を監視.<br>
 * Parameterファイルに設定されているマーカーファイル郡を使用して管理を行う.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class KeyNodeWatchJob extends AbstractJob implements IJob {

    // ノードの監視サイクル時間(ミリ秒)
    private int checkCycle = 5000;

    private ArrayList mainNodeList = null;

    private ArrayList subNodeList = null;

    private HashMap checkErrorMap = null;
    /**
     * Logger.<br>
     */
    private static ILogger logger = LoggerFactory.createLogger(KeyNodeWatchJob.class);

    // 初期化メソッド定義
    public void initJob(String initValue) {
        logger.debug("KeyNodeWatchJob - initJob - start");
        HashMap allNodeInfo = DataDispatcher.getAllDataNodeInfo();
        this.mainNodeList = (ArrayList)allNodeInfo.get("main");
        this.subNodeList = (ArrayList)allNodeInfo.get("sub");
        this.checkErrorMap = new HashMap();
        logger.debug("KeyNodeWatchJob - initJob - end");
    }

    // Jobメイン処理定義
    public String executeJob(String optionParam) throws BatchException {
        logger.debug("KeyNodeWatchJob - executeJob - start");
        String ret = SUCCESS;
        String serverStopMarkerFileName = null;
        File serverStopMarkerFile = null;

        boolean serverRunning = true;

        Object[] shareKeys = null;
        ServerSocket serverSocket = null;

        try{
            while (serverRunning) {
                // ノード数分チェック
                for (int i = 0; i < mainNodeList.size(); i++) {
                    Thread.sleep(checkCycle);

                    // 停止ファイル関係チェック
                    if (StatusUtil.getStatus() == 1) {
                        serverRunning = false;
                        logger.info("MasterManagerJob - 状態異常です");
                    }

                    if (StatusUtil.getStatus() == 2) {
                        serverRunning = false;
                        logger.info("MasterManagerJob - 終了状態です");
                    }

                    serverStopMarkerFileName = super.getPropertiesValue("ServerStopFile");

                    serverStopMarkerFile = new File(new File(serverStopMarkerFileName).getAbsolutePath());
                    if (serverStopMarkerFile.exists()) {
                        serverRunning = false;
                        logger.info("MasterManagerJob - Server停止ファイルが存在します");
                        StatusUtil.setStatus(2);
                    }

                    // ノードチェック(メイン)
                    String nodeInfo = (String)mainNodeList.get(i);
                    String[] nodeDt = nodeInfo.split(":");

                    logger.info(nodeDt[0] + ":" +  nodeDt[1] + " Node Check Start");

                    if(!this.execNodePing(nodeDt[0], new Integer(nodeDt[1]).intValue())) {
                        logger.info(nodeDt[0] + ":" +  nodeDt[1] + " Node Check Error");
                        checkErrorMap.put(nodeInfo, new Boolean(false));
                    }

                    // ノードチェック(Sub)
                    if (subNodeList != null && i < subNodeList.size()) {
                        String subNodeInfo = (String)subNodeList.get(i);
                        String[] subNodeDt = subNodeInfo.split(":");

                        logger.info(subNodeDt[0] + ":" +  subNodeDt[1] + " Sub Node Check Start");
                        if(!this.execNodePing(subNodeDt[0], new Integer(subNodeDt[1]).intValue())) {
                            logger.info(subNodeDt[0] + ":" +  subNodeDt[1] + " SubNode Check Error");
                            checkErrorMap.put(subNodeInfo, new Boolean(false));
                        }
                    }
                }
            }
        } catch(Exception e) {
            logger.error("MasterManagerJob - executeJob - Error", e);
            throw new BatchException(e);
        }

        logger.debug("MasterManagerJob - executeJob - end");
        return ret;
    }


    private boolean execNodePing(String nodeName, int port) {
        boolean ret = true;
        BufferedReader br = null;
        PrintWriter pw = null;
        Socket socket = null;
        String[] retParams = null;
        
        try {
            // 接続テスト
            socket = new Socket(nodeName, port);

            OutputStreamWriter osw = new OutputStreamWriter(socket.getOutputStream() , ImdstDefine.keyHelperClientParamEncoding);
            pw = new PrintWriter(new BufferedWriter(osw));

            InputStreamReader isr = new InputStreamReader(socket.getInputStream(), ImdstDefine.keyHelperClientParamEncoding);
            br = new BufferedReader(isr);

            // Key値でデータノード名を保存
            StringBuffer buf = new StringBuffer();
            // パラメータ作成 処理タイプ[セパレータ]キー値のハッシュ値文字列[セパレータ]データノード名
            buf.append("10");
            buf.append(ImdstDefine.keyHelperClientParamSep);
            buf.append("PING_CHECK");
            buf.append(ImdstDefine.keyHelperClientParamSep);
            buf.append(new Date().getTime());
            logger.info(buf.toString());
            // 送信
            pw.println(buf.toString());
            pw.flush();

            // 返却値取得
            String retParam = br.readLine();

            retParams = retParam.split(ImdstDefine.keyHelperClientParamSep);

            if (!retParams[1].equals("true")) {
                ret = false;
                logger.info(retParams[2]);
            }
        } catch(Exception e) {
            ret = false;
            logger.info("Node Ping Chekc Error Node Name = [" + nodeName + "] Port [" + port + "]");
            logger.info(e);
        } finally {
            try {
                if (br != null) br.close();
                if (pw != null) pw.close();
                if (socket != null) socket.close();
            } catch(Exception e2) {
                // 無視
                logger.error(e2);
            }
        }
        return ret;
    }
}