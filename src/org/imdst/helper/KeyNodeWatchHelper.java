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

    private ArrayList thirdNodeList = null;

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

        String[] pingRet = null;


            while (serverRunning) {
                try{
                    HashMap allNodeInfo = DataDispatcher.getAllDataNodeInfo();
                    this.mainNodeList = (ArrayList)allNodeInfo.get("main");
                    this.subNodeList = (ArrayList)allNodeInfo.get("sub");
                    this.thirdNodeList = (ArrayList)allNodeInfo.get("third");

                    // サードノードが存在する場合はマージする
                    if (this.thirdNodeList != null && this.thirdNodeList.size() > 0) {
                        ArrayList newMainNodeList = new ArrayList();
                        ArrayList newSubNodeList = new ArrayList();

                        for (int i = 0; i < this.mainNodeList.size(); i++) {
                            newMainNodeList.add(this.mainNodeList.get(i));
                            newSubNodeList.add(this.subNodeList.get(i));
                        }
                        for (int i = 0; i < this.mainNodeList.size(); i++) {
                            newMainNodeList.add(this.mainNodeList.get(i));
                            newSubNodeList.add(this.thirdNodeList.get(i));
                            newMainNodeList.add(this.subNodeList.get(i));
                            newSubNodeList.add(this.thirdNodeList.get(i));
                        }
                        this.mainNodeList = newMainNodeList;
                        this.subNodeList = newSubNodeList;
                    }


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

                            pingRet = this.execNodePing(nodeDt[0], new Integer(nodeDt[1]).intValue(), logger);
                            if (pingRet[1] != null) this.nodeStatusStr = pingRet[1];

                            if(pingRet[0].equals("false")) {
                                // ノードダウン
                                logger.info(nodeDt[0] + ":" +  nodeDt[1] + " Node Check Dead");
                                super.setDeadNode(nodeInfo, 1, null);
                                StatusUtil.setNodeStatusDt(nodeDt[0] + ":" +  nodeDt[1], "Node Check Dead");
                            } else if (!super.isNodeArrival(nodeInfo)) {

                                // ノードが復旧
                                logger.info("Node Name [" + nodeInfo +"] Reboot");

                                // Subノードが存在する場合はデータ復元、存在しない場合はそのまま起動
                                if (subNodeList != null && (subNodeList.get(i) != null)) {

                                    // 停止していたノードが復帰した場合
                                    // 停止中に登録予定であったデータを登録する
                                    logger.info("Node Name [" + nodeInfo +"] Use Wait 1-1 Start");

                                    // 復旧前に現在稼働中のMasterNodeに再度停止ノードと、リカバー開始を伝える
                                    super.setDeadNode(nodeInfo, 1, null);
                                    super.setRecoverNode(true, (String)subNodeList.get(i));

                                    logger.info(nodeInfo + " - Recover Start");
                                    StatusUtil.setNodeStatusDt(nodeInfo, "Recover Start");

                                    // 復旧開始
                                    if(nodeDataRecover(nodeInfo, (String)subNodeList.get(i), logger)) {

                                        // リカバー成功
                                        // 該当ノードの復帰を登録
                                        logger.info(nodeInfo + " - Recover Success");
                                        StatusUtil.setNodeStatusDt(nodeInfo, "Recover Success");
                                    } else {
                                        logger.info(nodeInfo + " - Recover Miss");
                                        // リカバー終了を伝える
                                        super.setRecoverNode(false, "");
                                        StatusUtil.setNodeStatusDt(nodeInfo, "Recover Miss");
                                    }

                                    logger.info(nodeInfo + " - Recover End");
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

                                pingRet = super.execNodePing(subNodeDt[0], new Integer(subNodeDt[1]).intValue(), logger);
                                if (pingRet[1] != null) this.nodeStatusStr = pingRet[1];

                                if(pingRet[0].equals("false")) {
                                    // ノードダウン
                                    logger.info(subNodeDt[0] + ":" +  subNodeDt[1] + " SubNode Check Dead");
                                    super.setDeadNode(subNodeInfo, 2 ,null);
                                    StatusUtil.setNodeStatusDt(subNodeDt[0] + ":" +  subNodeDt[1], "SubNode Check Dead");
                                } else if (!super.isNodeArrival(subNodeInfo)) {

                                    // 停止していたノードが復帰した場合
                                    // 停止中に登録予定であったデータを登録する
                                    logger.info("Node Name [" + subNodeInfo +"] Reboot");

                                    // 復旧前に現在稼働中のMasterNodeに再度停止ノードと、リカバー開始を伝える
                                    super.setDeadNode(subNodeInfo, 1, null);
                                    super.setRecoverNode(true, nodeInfo);

                                    logger.info(subNodeInfo + " - Recover Start");
                                    StatusUtil.setNodeStatusDt(subNodeInfo, "Recover Start");
                                    // 復旧開始
                                    if(nodeDataRecover(subNodeInfo, nodeInfo, logger)) {

                                        // リカバー成功
                                        // 該当ノードの復帰を登録
                                        logger.info(subNodeInfo + " - Recover Success");
                                        StatusUtil.setNodeStatusDt(subNodeInfo, "Recover Success");
                                    } else {

                                        logger.info(subNodeInfo + " - Recover Miss");
                                        // リカバー終了を伝える
                                        super.setRecoverNode(false, "");
                                        StatusUtil.setNodeStatusDt(subNodeInfo, "Recover Miss");
                                    }

                                    logger.info(subNodeInfo + " - Recover End");
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
                } catch(Exception e) {
                    logger.error("KeyNodeWatchHelper - executeHelper - Error", e);
                }
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
     * ダウン状態から復帰したノードに対して、ペアーのノードのデータをコピーする.<br>
     * コピー元のデータをコピー先へ.<br>
     * 本メソッドを呼び出す前に必ず両ノードの使用を一時中断していること
     *
     * @param コピー先ノード(予定)
     * @param コピー元ノード(予定)
     *
     * @return boolean 成否
     */
    protected boolean nodeDataRecover(String nodeInfo, String masterNodeInfo, ILogger logger) throws BatchException {
        return this.nodeDataRecover(nodeInfo, masterNodeInfo, false, logger);
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
    protected boolean nodeDataRecover(String nodeInfo, String masterNodeInfo, boolean noDataCheck, ILogger logger) throws BatchException {

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

        String sendRet = null;
        String diffDataInputRet = null;
        String diffModeOffRet = null;

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
            if (noDataCheck == true || masterDate >= nodeDate) {

                // 予定どうり
                logger.info("Data Recover Actually [" + masterNodeInfo + " => " + nodeInfo + "]");
                logger.info("Recover Step - 1");

                // もともと生存していたノードを差分モードOnにする
                buf = new StringBuffer();
                // 処理番号22
                buf.append("22").append(ImdstDefine.keyHelperClientParamSep).append("true");
                // 送信
                mpw.println(buf.toString());
                mpw.flush();
                String diffModeOn = mbr.readLine();

                logger.info("Recover Step - 2");

                // コピー元からデータ読み込み
                buf = new StringBuffer();
                // 処理番号20
                buf.append("20");
                buf.append(ImdstDefine.keyHelperClientParamSep);
                buf.append("true");

                // 送信
                mpw.println(buf.toString());
                mpw.flush();
                logger.info("Recover Step - 3");

                // データ行数取得
                // 1行にメモリに乗るのに十分余裕のあるサイズが送られてくる
                lineCount = mbr.readLine();
                logger.info("Recover Step - 4");

                // 取得したデータをコピー先に書き出し
                // 処理番号21
                buf = new StringBuffer();
                buf.append("21");
                buf.append(ImdstDefine.keyHelperClientParamSep);
                buf.append(lineCount);
                // 送信
                pw.println(buf.toString());
                pw.flush();
                logger.info("Recover Step - 5");

                for (int i = 0; i < Integer.parseInt(lineCount); i++) {
                    // 値を書き出し
                    logger.info("Recover Step - 6");
                    retParam = mbr.readLine();
                    pw.println(retParam);
                    pw.flush();
                    logger.info("Recover Step - 7");
                }


                // 送信結果を受信
                if((sendRet = br.readLine()) != null) {
                    logger.info("Recover Step - 8");
                    if(!sendRet.equals("1")) throw new Exception("send Data Error Ret=[" + sendRet + "]");
                } else {
                    logger.info("Recover Step - 9");
                    throw new Exception("send Data Error Ret=[" + sendRet + "]");
                }

                logger.info("Recover Step - 10");
                // 停止完了後差分データを取得
                // この瞬間登録、削除は一時的に停止する。
                buf = new StringBuffer();
                // 処理番号24
                buf.append("24");
                buf.append(ImdstDefine.keyHelperClientParamSep);
                buf.append("true");

                // 送信
                mpw.println(buf.toString());
                mpw.flush();

                logger.info("Recover Step - 11");
                // 差分データを送る
                buf = new StringBuffer();
                buf.append("25");
                buf.append(ImdstDefine.keyHelperClientParamSep);
                pw.println(buf.toString());
                pw.flush();

                // データを送信
                String str = mbr.readLine();

                pw.println(str);
                pw.flush();

                logger.info("Recover Step - 12");
                // 取り込み完了を確認
                if((diffDataInputRet = br.readLine()) != null) {

                    logger.info("Recover Step - 13");
                    if(!diffDataInputRet.equals("1")) throw new Exception("Diff Data Input Error Ret=[" + diffDataInputRet + "]");
                } else {

                    logger.info("Recover Step - 14");
                    throw new Exception("Diff Data Input Error Ret=[" + diffDataInputRet + "]");
                }

                logger.info("Recover Step - 15");

                // リカバー完了を全MasterNodeへ送信
                super.setRecoverNode(false, "");
                super.setArriveNode(nodeInfo);


                // 差分読み込み中のノードの差分データ反映完了を送信
                mpw.println("1");
                mpw.flush();
                if((diffModeOffRet = mbr.readLine()) != null) {

                    logger.info("Recover Step - 16");
                    if (!diffModeOffRet.equals("1")) throw new Exception("Diff Mode Off Error Ret=[" + diffModeOffRet + "]");
                } else {

                    logger.info("Recover Step - 17");
                    throw new Exception("Diff Mode Off Error Ret=[" + diffModeOffRet + "]");
                }

                logger.info("Recover Step - 18");
            } else {

                // 当初の予定から逆転
                logger.info("Data Recover Actually [" + nodeInfo + " => " + masterNodeInfo + "]");

                // もともと生存していたノードを差分モードOnにする
                buf = new StringBuffer();
                // 処理番号22
                buf.append("22").append(ImdstDefine.keyHelperClientParamSep).append("true");
                // 送信
                pw.println(buf.toString());
                pw.flush();
                String diffModeOn = br.readLine();

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
                    // 値を書き出し
                    retParam = br.readLine();
                    mpw.println(retParam);
                    mpw.flush();
                }

                // 送信結果を受信
                if((sendRet = mbr.readLine()) != null) {
                    if(!sendRet.equals("1")) throw new Exception("send Data Error Ret=[" + sendRet + "]");
                } else {
                    throw new Exception("send Data Error Ret=[" + sendRet + "]");
                }

                // 停止完了後差分データを取得
                // この瞬間が登録、削除は一時的に停止する。
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
                mpw.println(buf.toString());
                mpw.flush();
                // データを送信
                mpw.println(br.readLine());
                mpw.flush();
                // 取り込み完了を確認
                if((diffDataInputRet = mbr.readLine()) != null) {
                    if(!diffDataInputRet.equals("1")) throw new Exception("Diff Data Input Error Ret=[" + diffDataInputRet + "]");
                } else {
                    throw new Exception("Diff Data Input Error Ret=[" + diffDataInputRet + "]");
                }

                // リカバー完了を全MasterNodeへ送信
                super.setRecoverNode(false, "");
                super.setArriveNode(nodeInfo);

                // 差分読み込み中のノードの差分データ反映完了を送信
                pw.println("1");
                pw.flush();
                if((diffModeOffRet = br.readLine()) != null) {
                    if (!diffModeOffRet.equals("1")) throw new Exception("Diff Mode Off Error Ret=[" + diffModeOffRet + "]");
                } else {
                    throw new Exception("Diff Mode Off Error Ret=[" + diffModeOffRet + "]");
                }
            }
        } catch (Exception e) {

            logger.error(e);
            ret = false;
        } finally {
            try {

                // コネクション切断
                if (pw != null) {
                    pw.println("23" + ImdstDefine.keyHelperClientParamSep + "true");
                    pw.flush();
                    pw.println(ImdstDefine.imdstConnectExitRequest);
                    pw.flush();
                    pw.close();
                }
                if (oos != null) oos.close();
                if (socket != null) socket.close();


                // コネクション切断
                if (mpw != null) {
                    mpw.println("23" + ImdstDefine.keyHelperClientParamSep + "true");
                    mpw.flush();
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