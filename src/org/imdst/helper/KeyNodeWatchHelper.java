package org.imdst.helper;

import java.io.*;
import java.util.*;
import java.net.*;

import org.batch.lang.BatchException;
import org.batch.job.AbstractHelper;
import org.batch.job.IJob;
import org.batch.util.ILogger;
import org.batch.util.LoggerFactory;
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
                HashMap allNodeInfo = DataDispatcher.getAllDataNodeInfo();
                this.mainNodeList = (ArrayList)allNodeInfo.get("main");
                this.subNodeList = (ArrayList)allNodeInfo.get("sub");

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

                    // MainのMasterNodeの場合のみ実行
                    if (StatusUtil.isMainMasterNode()) {

                        // ノードチェック(メイン)
                        String nodeInfo = (String)mainNodeList.get(i);

                        String[] nodeDt = nodeInfo.split(":");

                        logger.info("************************************************************");
                        logger.info(nodeDt[0] + ":" +  nodeDt[1] + " Node Check Start");

                        if(!this.execNodePing(nodeDt[0], new Integer(nodeDt[1]).intValue())) {
                            // ノードダウン
                            logger.info(nodeDt[0] + ":" +  nodeDt[1] + " Node Check Dead");
                            super.setDeadNode(nodeInfo);
                            StatusUtil.setNodeStatusDt(nodeDt[0] + ":" +  nodeDt[1], "Node Check Dead");
                        } else if (!super.isNodeArrival(nodeInfo)) {

                            // ノードが復旧
                            logger.info("Node Name [" + nodeInfo +"] Reboot");

                            // Subノードが存在する場合はデータ復元、存在しない場合はそのまま起動
                            if (subNodeList != null && (subNodeList.get(i) != null)) {

                                // 停止していたノードが復帰した場合
                                // 停止中に登録予定であったデータを登録する
                                logger.info("Node Name [" + nodeInfo +"] Use Wait Start");

                                // ノードの使用中断を要求
                                super.setNodeWaitStatus(nodeInfo);
                                while(true) {
                                    // 使用停止まで待機
                                    if(super.getNodeUseStatus(nodeInfo) == 0) break;
                                    Thread.sleep(10);
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
                                StatusUtil.setNodeStatusDt(nodeInfo, "Recover Start");

                                // 復旧開始
                                if(this.nodeDataRecover(nodeInfo, (String)subNodeList.get(i), DataDispatcher.ruleInt, DataDispatcher.oldRules, i)) {

                                    // リカバー成功
                                    // 該当ノードの復帰を登録
                                    logger.info(nodeInfo + " - Recover Success");
                                    StatusUtil.setNodeStatusDt(nodeInfo, "Recover Success");
                                    super.setArriveNode(nodeInfo);
                                } else {
                                    logger.info(nodeInfo + " - Recover Miss");
                                    StatusUtil.setNodeStatusDt(nodeInfo, "Recover Miss");
                                }

                                logger.info(nodeInfo + " - Recover End");

                                // 該当ノードの一時停止を解除
                                super.removeNodeWaitStatus((String)subNodeList.get(i));
                                super.removeNodeWaitStatus(nodeInfo);
                            } else {
                                // ノードの復旧を記録
                                super.setArriveNode(nodeInfo);
                            }
                        } else {
                            logger.info(nodeDt[0] + ":" +  nodeDt[1] + " Node Check Arrival");
                            logger.info(nodeDt[0] + ":" +  nodeDt[1] + " Server Status [" + this.nodeStatusStr + "]");
                            StatusUtil.setNodeStatusDt(nodeDt[0] + ":" +  nodeDt[1], "[" + this.nodeStatusStr + "]");
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
                                StatusUtil.setNodeStatusDt(subNodeDt[0] + ":" +  subNodeDt[1], "SubNode Check Dead");
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
                                StatusUtil.setNodeStatusDt(subNodeInfo, "Recover Start");
                                // 復旧開始
                                if(this.nodeDataRecover(subNodeInfo, nodeInfo, DataDispatcher.ruleInt, DataDispatcher.oldRules, i)) {

                                    // リカバー成功
                                    // 該当ノードの復帰を登録
                                    logger.info(subNodeInfo + " - Recover Success");
                                    StatusUtil.setNodeStatusDt(subNodeInfo, "Recover Success");
                                    super.setArriveNode(subNodeInfo);
                                } else {
                                    logger.info(subNodeInfo + " - Recover Miss");
                                    StatusUtil.setNodeStatusDt(subNodeInfo, "Recover Miss");
                                }

                                logger.info(subNodeInfo + " - Recover End");

                                // 一時停止を解除
                                super.removeNodeWaitStatus(subNodeInfo);
                                super.removeNodeWaitStatus(nodeInfo);
                            } else {
                                logger.info(subNodeDt[0] + ":" +  subNodeDt[1] + " Sub Node Check Arrival");
                                logger.info(subNodeDt[0] + ":" +  subNodeDt[1] + " Server Status [" + this.nodeStatusStr + "]");
                                StatusUtil.setNodeStatusDt(subNodeDt[0] + ":" +  subNodeDt[1], "[" + this.nodeStatusStr + "]");
                            }
                            logger.info(subNodeDt[0] + ":" +  subNodeDt[1] + " Sub Node Check End");
                            logger.info("************************************************************");
                        }
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
            // 接続
            socket = new Socket(nodeName, port);

            // Timeout設定
            socket.setSoTimeout(ImdstDefine.nodeConnectionTimeout);
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
     * コピー元のデータをコピー先へ.<br>
     * 本メソッドを呼び出す前に必ず両ノードの使用を一時中断していること
     *
     * @param コピー先ノード(予定)
     * @param コピー元ノード(予定)
     *
     * @return boolean 成否
     */
    private boolean nodeDataRecover(String nodeInfo, String masterNodeInfo, int rule, int[] oldRules, int matchNo) throws BatchException {

        boolean ret = true;

        String retParam = null;
        String[] retParams = null;
        String lineCount = null;

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

            logger.info("Data Recover Schedule [" + masterNodeInfo + " => " + nodeInfo + "]");
            // コピー先KeyNodeとの接続を確立
            socket = new Socket(nodeName, nodePort);
            socket.setSoTimeout(ImdstDefine.recoverConnectionTimeout);

            OutputStreamWriter osw = new OutputStreamWriter(socket.getOutputStream() , ImdstDefine.keyHelperClientParamEncoding);
            pw = new PrintWriter(new BufferedWriter(osw));

            InputStreamReader isr = new InputStreamReader(socket.getInputStream(), ImdstDefine.keyHelperClientParamEncoding);
            br = new BufferedReader(isr);

            // コピー元KeyNodeとの接続を確立
            msocket = new Socket(masterNodeName, masterNodePort);
            msocket.setSoTimeout(ImdstDefine.recoverConnectionTimeout);

            OutputStreamWriter mosw = new OutputStreamWriter(msocket.getOutputStream() , ImdstDefine.keyHelperClientParamEncoding);
            mpw = new PrintWriter(new BufferedWriter(mosw));

            InputStreamReader misr = new InputStreamReader(msocket.getInputStream(), ImdstDefine.keyHelperClientParamEncoding);
            mbr = new BufferedReader(misr);


            // TODO:ここでそれぞれのノードの最終更新時間を見て新しいほうのデータで上書き
            //      するが微妙かも

            // コピー元予定から最終更新時刻取得
            StringBuffer buf = new StringBuffer();
            // 処理番号11
            buf.append("11");
            buf.append(ImdstDefine.keyHelperClientParamSep);
            buf.append("true");
            // 送信
            mpw.println(buf.toString());
            mpw.flush();
            // データ取得
            retParam = mbr.readLine();

            String[] updateDate = retParam.split(ImdstDefine.keyHelperClientParamSep);

            long masterDate = new Long(updateDate[2]).longValue();


            // コピー先予定から最終更新時刻取得
            buf = new StringBuffer();
            // 処理番号11
            buf.append("11");
            buf.append(ImdstDefine.keyHelperClientParamSep);
            buf.append("true");
            // 送信
            pw.println(buf.toString());
            pw.flush();
            // データ取得
            retParam = br.readLine();

            updateDate = retParam.split(ImdstDefine.keyHelperClientParamSep);

            long nodeDate = new Long(updateDate[2]).longValue();

            // どちらが新しいか比べる
            if (masterDate >= nodeDate) {

                // 予定どうり
                logger.info("Data Recover Actually [" + masterNodeInfo + " => " + nodeInfo + "]");

                // もともと生存していたノードを差分モードOnにする
                buf = new StringBuffer();
                // 処理番号22
                buf.append("22").append(ImdstDefine.keyHelperClientParamSep).append("true");
                // 送信
                mpw.println(buf.toString());
                mpw.flush();

                // コピー元の一時停止を解除
                super.removeNodeWaitStatus(masterNodeInfo);


                // コピー元からデータ読み込み
                buf = new StringBuffer();
                // 処理番号20
                buf.append("20");
                buf.append(ImdstDefine.keyHelperClientParamSep);
                buf.append("true");
                // 送信
                mpw.println(buf.toString());
                mpw.flush();

                // データ行数取得
                // 1行にメモリに乗るのに十分余裕のあるサイズが送られてくる
                lineCount = mbr.readLine();


                // 取得したデータをコピー先に書き出し
                // 処理番号21
                buf = new StringBuffer();
                buf.append("21");
                buf.append(ImdstDefine.keyHelperClientParamSep);
                buf.append(lineCount);
                // 送信
                pw.println(buf.toString());
                pw.flush();

                for (int i = 0; i < Integer.parseInt(lineCount); i++) {
                    // 値を書き出し
                    retParam = mbr.readLine();
                    pw.println(retParam);
                    pw.flush();
                }


                // コピー元を一時停止にする
                super.setNodeWaitStatus(masterNodeInfo);
                while(true) {
                    // 使用停止まで待機
                    if(super.getNodeUseStatus(masterNodeInfo) == 0) break;
                    Thread.sleep(10);
                }

                // 停止完了後差分データを取得
                buf = new StringBuffer();
                // 処理番号24
                buf.append("24");
                buf.append(ImdstDefine.keyHelperClientParamSep);
                buf.append("true");
                // 送信
                mpw.println(buf.toString());
                mpw.flush();

                // 差分データを送る
                buf = new StringBuffer();
                buf.append("25");
                buf.append(ImdstDefine.keyHelperClientParamSep);
                buf.append(mbr.readLine());
                pw.println(buf.toString());
                pw.flush();

                // もともと生存していたノードを差分モードOffにする
                buf = new StringBuffer();
                // 処理番号23
                buf.append("23").append(ImdstDefine.keyHelperClientParamSep).append("true");
                // 送信
                mpw.println(buf.toString());
                mpw.flush();

            } else {

                // 当初の予定から逆転
                logger.info("Data Recover Actually [" + nodeInfo + " => " + masterNodeInfo + "]");

                // 最終更新日付が新しいノードを差分モードOnにする
                buf = new StringBuffer();
                // 処理番号22
                buf.append("22").append(ImdstDefine.keyHelperClientParamSep).append("true");
                // 送信
                pw.println(buf.toString());

                pw.flush();

                // コピー元の一時停止を解除
                super.removeNodeWaitStatus(nodeInfo);


                // コピー元からデータ読み込み
                buf = new StringBuffer();
                // 処理番号20
                buf.append("20");
                buf.append(ImdstDefine.keyHelperClientParamSep);
                buf.append("true");
                // 送信
                pw.println(buf.toString());

                pw.flush();

                // データ行数取得
                // 1行にメモリに乗るのに十分余裕のあるサイズが送られてくる
                lineCount = br.readLine();


                // 取得したデータをコピー先に書き出し
                // 処理番号21
                buf = new StringBuffer();
                buf.append("21");
                buf.append(ImdstDefine.keyHelperClientParamSep);
                buf.append(lineCount);

                // 送信
                mpw.println(buf.toString());
                mpw.flush();

                for (int i = 0; i < Integer.parseInt(lineCount); i++) {

                    // データ取得
                    retParam = br.readLine();

                    // 値を書き出し
                    mpw.println(retParam);

                    mpw.flush();
                }


                // コピー元を一時停止にする
                super.setNodeWaitStatus(nodeInfo);

                while(true) {

                    // 使用停止まで待機
                    if(super.getNodeUseStatus(nodeInfo) == 0) break;
                    Thread.sleep(10);
                }


                // 停止完了後差分データを取得
                buf = new StringBuffer();
                // 処理番号24
                buf.append("24");
                buf.append(ImdstDefine.keyHelperClientParamSep);
                buf.append("true");
                // 送信
                pw.println(buf.toString());

                pw.flush();

                // 差分データを送る
                buf = new StringBuffer();
                buf.append("25");
                buf.append(ImdstDefine.keyHelperClientParamSep);
                buf.append(br.readLine());
                mpw.println(buf.toString());

                mpw.flush();

                // もともと生存していたノードを差分モードOffにする
                buf = new StringBuffer();
                // 処理番号23
                buf.append("23").append(ImdstDefine.keyHelperClientParamSep).append("true");
                // 送信
                pw.println(buf.toString());

                pw.flush();

            }
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