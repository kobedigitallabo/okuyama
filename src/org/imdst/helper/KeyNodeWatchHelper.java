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
 * KeyNodeの監視を行うHelperクラス<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class KeyNodeWatchHelper extends AbstractMasterManagerHelper {

    // ノードの監視サイクル時間(ミリ秒)
    private int checkCycle = 1000;

    private ArrayList mainNodeList = null;

    private ArrayList subNodeList = null;

	private String nodeStatusStr = null;
    /**
     * Logger.<br>
     */
    private static ILogger logger = LoggerFactory.createLogger(KeyNodeWatchHelper.class);

    // 初期化メソッド定義
    public void initHelper(String initValue) {
        HashMap allNodeInfo = DataDispatcher.getAllDataNodeInfo();
        this.mainNodeList = (ArrayList)allNodeInfo.get("main");
        this.subNodeList = (ArrayList)allNodeInfo.get("sub");
    }

    // Jobメイン処理定義
    public String executeHelper(String optionParam) throws BatchException {
        logger.debug("KeyNodeWatchHelper - executeHelper - start");
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
                        logger.info("KeyNodeWatchHelper - 状態異常です");
                    }

                    if (StatusUtil.getStatus() == 2) {
                        serverRunning = false;
                        logger.info("KeyNodeWatchHelper - 終了状態です");
                    }

                    serverStopMarkerFileName = super.getPropertiesValue("ServerStopFile");

                    serverStopMarkerFile = new File(new File(serverStopMarkerFileName).getAbsolutePath());
                    if (serverStopMarkerFile.exists()) {
                        serverRunning = false;
                        logger.info("KeyNodeWatchHelper - Server停止ファイルが存在します");
                        StatusUtil.setStatus(2);
                    }


                    // ノードチェック(メイン)
                    String nodeInfo = (String)mainNodeList.get(i);
                    String[] nodeDt = nodeInfo.split(":");

                    logger.info("************************************************************");
                    logger.info(nodeDt[0] + ":" +  nodeDt[1] + " Node Check Start");

                    if(!this.execNodePing(nodeDt[0], new Integer(nodeDt[1]).intValue())) {
                        // ノードダウン
                        logger.info(nodeDt[0] + ":" +  nodeDt[1] + " Node Check Dead");
                        super.setDeadNode(nodeInfo);
                    } else if (!super.isNodeArrival(nodeInfo)) {

                        // 停止していたノードが復帰した場合
                        // 停止中に登録予定であったデータを登録する

                        logger.info("Node Name [" + nodeInfo +"] Reboot");
                        logger.info("Node Name [" + nodeInfo +"] Use Wait Start");

                        // ノードの使用中断を要求
                        super.setNodeWaitStatus(nodeInfo);
                        while(true) {
                            // 使用停止まで待機
                            if(super.getNodeUseStatus(nodeInfo) == 0) break;
                            Thread.sleep(5);
                        }

                        // 該当ノードのデータをリカバーする場合は該当ノードと対になるノードの使用停止を要求し、
                        // 遂になるノードの使用数が0になってからリカバーを開始する。
                        if (subNodeList != null && i < subNodeList.size()) {
                            super.setNodeWaitStatus((String)subNodeList.get(i));
                            logger.info("Node Name [" + (String)subNodeList.get(i) +"] Use Wait Start");
                            while(true) {
                                // 使用停止まで待機
                                if(super.getNodeUseStatus((String)subNodeList.get(i)) == 0) break;
                                Thread.sleep(10);
                            }
                        }

                        logger.info(nodeInfo + " - Recover Start");


                        // 復旧開始
                        if(this.nodeDataRecover(nodeInfo, (String)subNodeList.get(i))) {

                            // リカバー成功
                            // 該当ノードの復帰を登録
                            logger.info(nodeInfo + " - Recover Success");
                            super.setArriveNode(nodeInfo);
                        } else {
                            logger.info(nodeInfo + " - Recover Miss");
                        }

                        logger.info(nodeInfo + " - Recover End");

                        // 該当ノードの一時停止を解除
                        super.removeNodeWaitStatus((String)subNodeList.get(i));
                        super.removeNodeWaitStatus(nodeInfo);
                        
                    } else {
                        logger.info(nodeDt[0] + ":" +  nodeDt[1] + " Node Check Arrival");
						logger.info(nodeDt[0] + ":" +  nodeDt[1] + " Server Status [" + this.nodeStatusStr + "]");
                    }
                    logger.info(nodeDt[0] + ":" +  nodeDt[1] + " Node Check End");


                    logger.info("------------------------------------------------------------");
                    // ノードチェック(Sub)
                    if (subNodeList != null && i < subNodeList.size()) {
                        String subNodeInfo = (String)subNodeList.get(i);
                        String[] subNodeDt = subNodeInfo.split(":");

                        logger.info(subNodeDt[0] + ":" +  subNodeDt[1] + " Sub Node Check Start");
                        if(!this.execNodePing(subNodeDt[0], new Integer(subNodeDt[1]).intValue())) {
                            // ノードダウン
                            logger.info(subNodeDt[0] + ":" +  subNodeDt[1] + " SubNode Check Dead");
                            super.setDeadNode(subNodeInfo);
                        } else if (!super.isNodeArrival(subNodeInfo)) {

                            // 停止していたノードが復帰した場合
                            // 停止中に登録予定であったデータを登録する
                            logger.info("Node Name [" + subNodeInfo +"] Reboot");
                            logger.info("Node Name [" + subNodeInfo +"] Use Wait Start");

                            // ノードの使用中断を要求
                            super.setNodeWaitStatus(subNodeInfo);
                            while(true) {
                                // 使用停止まで待機
                                if(super.getNodeUseStatus(subNodeInfo) == 0) break;
                                Thread.sleep(10);
                            }

                            // 該当ノードのデータをリカバーする場合は該当ノードと対になるノードの使用停止を要求し、
                            // 遂になるノードの使用数が0になってからリカバーを開始する。
                            super.setNodeWaitStatus(nodeInfo);
                            logger.info("Node Name [" + nodeInfo +"] Use Wait Start");
                            while(true) {
                                // 使用停止まで待機
                                if(super.getNodeUseStatus(nodeInfo) == 0) break;
                                Thread.sleep(5);
                            }

                            logger.info(subNodeInfo + " - Recover Start");

                            // 復旧開始
                            if(this.nodeDataRecover(subNodeInfo, nodeInfo)) {

                                // リカバー成功
                                // 該当ノードの復帰を登録
                                logger.info(subNodeInfo + " - Recover Success");
                                super.setArriveNode(subNodeInfo);
                            } else {
                                logger.info(subNodeInfo + " - Recover Miss");
                            }

                            logger.info(subNodeInfo + " - Recover End");

                            // 一時停止を解除
                            super.removeNodeWaitStatus(subNodeInfo);
                            super.removeNodeWaitStatus(nodeInfo);
                        } else {
                            logger.info(subNodeDt[0] + ":" +  subNodeDt[1] + " Sub Node Check Arrival");
							logger.info(subNodeDt[0] + ":" +  subNodeDt[1] + " Server Status [" + this.nodeStatusStr + "]");
                        }
                        logger.info(subNodeDt[0] + ":" +  subNodeDt[1] + " Sub Node Check End");
                        logger.info("************************************************************");
                    }
                }
            }
        } catch(Exception e) {
            logger.error("KeyNodeWatchHelper - executeHelper - Error", e);
            throw new BatchException(e);
        }

        logger.debug("KeyNodeWatchHelper - executeHelper - end");
        return ret;
    }


    /**
     * Helper後処理.<br>
     *
     */
    public void endHelper() {
    }

    /**
     * ノードに対して生存確認用のPingを行う
     *
     */
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
            buf.append("Check");

            // 送信
            pw.println(buf.toString());
            pw.flush();

            // 返却値取得
            String retParam = br.readLine();

            retParams = retParam.split(ImdstDefine.keyHelperClientParamSep);

            if (!retParams[1].equals("true")) {
                ret = false;
            } else {
				// 生存の場合ステータス情報文字列を格納
				this.nodeStatusStr = retParams[2];
			}
            pw.println(ImdstDefine.imdstConnectExitRequest);
            pw.flush();
        } catch(Exception e) {
            ret = false;
            logger.info("Node Ping Chekc Error Node Name = [" + nodeName + "] Port [" + port + "]");
            logger.info(e);
        } finally {
            try {

                if (br != null) br.close();
                if (pw != null) {
                    pw.close();
                }
                if (socket != null) socket.close();
            } catch(Exception e2) {
                // 無視
                logger.error(e2);
            }
        }
        return ret;
    }


    /**
     * ダウン状態から復帰したノードに対して、ペアーのノードのデータをコピーする.<br>
     *
     *
     *
     */
    private boolean nodeDataRecover(String nodeInfo, String masterNodeInfo) throws BatchException {
        boolean ret = true;

        String retParam = null;
        String[] retParams = null;

        String[] nodeDt = nodeInfo.split(":");
        String[] masterNodeDt = masterNodeInfo.split(":");

        String nodeName = nodeDt[0];
        int nodePort = new Integer(nodeDt[1]).intValue();

        String masterNodeName = masterNodeDt[0];
        int masterNodePort = new Integer(masterNodeDt[1]).intValue();

        PrintWriter pw = null;
        BufferedReader br = null;
        Socket socket = null;
        ObjectOutputStream oos = null;

        PrintWriter mpw = null;
        BufferedReader mbr = null;
        Socket msocket = null;
        ObjectInputStream mois = null;

        try {
            // コピー先KeyNodeとの接続を確立
            socket = new Socket(nodeName, nodePort);

            OutputStreamWriter osw = new OutputStreamWriter(socket.getOutputStream() , ImdstDefine.keyHelperClientParamEncoding);
            pw = new PrintWriter(new BufferedWriter(osw));

            InputStreamReader isr = new InputStreamReader(socket.getInputStream(), ImdstDefine.keyHelperClientParamEncoding);
            br = new BufferedReader(isr);

            // コピー元KeyNodeとの接続を確立
            msocket = new Socket(masterNodeName, masterNodePort);
            OutputStreamWriter mosw = new OutputStreamWriter(msocket.getOutputStream() , ImdstDefine.keyHelperClientParamEncoding);
            mpw = new PrintWriter(new BufferedWriter(mosw));

            InputStreamReader misr = new InputStreamReader(msocket.getInputStream(), ImdstDefine.keyHelperClientParamEncoding);
            mbr = new BufferedReader(misr);


            // TODO:一度に全てのデータを1行文字列で読み込むので改良の余地あり

            // コピー元からデータ読み込み
            StringBuffer buf = new StringBuffer();
            // 処理番号20
            buf.append("20");
            buf.append(ImdstDefine.keyHelperClientParamSep);
            buf.append("true");
            // 送信
            mpw.println(buf.toString());
            mpw.flush();

            // データ取得
            retParam = mbr.readLine();

            // 取得したデータをコピー先に書き出し
            // 処理番号21
            buf = new StringBuffer();
            buf.append("21");
            buf.append(ImdstDefine.keyHelperClientParamSep);
            buf.append("true");
            // 送信
            pw.println(buf.toString());
            pw.flush();

            // 値を書き出し
            pw.println(retParam);
            pw.flush();

        } catch (Exception e) {

            logger.error(e);
            ret = false;
        } finally {
            try {

                // コネクション切断
                if (pw != null) {
                    pw.println(ImdstDefine.imdstConnectExitRequest);
                    pw.flush();
                    pw.close();
                }
                if (oos != null) oos.close();
                if (socket != null) socket.close();


                // コネクション切断
                if (mpw != null) {
                    mpw.println(ImdstDefine.imdstConnectExitRequest);
                    mpw.flush();
                    mpw.close();
                }
                if (mois != null) mois.close();
                if (msocket != null) msocket.close();

            } catch(Exception e2) {
                // 無視
                logger.error(e2);
            }
        }
        return ret;
    }
}