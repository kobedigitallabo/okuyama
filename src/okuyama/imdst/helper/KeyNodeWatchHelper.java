package okuyama.imdst.helper;

import java.io.*;
import java.util.*;
import java.net.*;

import okuyama.base.lang.BatchException;
import okuyama.base.job.AbstractHelper;
import okuyama.base.job.IJob;
import okuyama.base.util.ILogger;
import okuyama.base.util.LoggerFactory;
import okuyama.imdst.util.ImdstDefine;
import okuyama.imdst.util.DataDispatcher;
import okuyama.imdst.util.StatusUtil;

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

    /**
     * 初期化メソッド定義.<br>
     *
     * @param initValue
     */
    public void initHelper(String initValue) {
    }


    /**
     * Jobメイン処理定義.<br>
     *
     * @param optionParam
     * @return String 
     * @throw BatchException
     */
    public String executeHelper(String optionParam) throws BatchException {
        logger.debug("KeyNodeWatchHelper - executeHelper - start");
        String ret = SUCCESS;

        File serverStopMarkerFile = null;

        boolean serverRunning = true;

        Object[] shareKeys = null;
        ServerSocket serverSocket = null;

        String[] pingRet = null;

        HashMap rebootNodeMap = new HashMap(128);

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
                            super.setDeadNode(nodeInfo, 1, null, true);
                            StatusUtil.setNodeStatusDt(nodeDt[0] + ":" +  nodeDt[1], "Node Check Dead");
                        } else if (!super.isNodeArrival(nodeInfo)) {

                            // ノードが復旧
                            logger.info("Node Name [" + nodeInfo +"] Reboot");

                            // ノード復旧処理を記録
                            if (rebootNodeMap.containsKey(nodeInfo)) {
                                Integer recoverCount = (Integer)rebootNodeMap.get(nodeInfo);
                                int recoverCountInt = recoverCount.intValue();
                                recoverCountInt++;
                                rebootNodeMap.put(nodeInfo, new Integer(recoverCountInt));
                            } else {
                                rebootNodeMap.put(nodeInfo, new Integer(1));
                            }

                            // Subノードが存在する場合はデータ復元、存在しない場合はそのまま起動
                            if (subNodeList != null && (subNodeList.get(i) != null)) {

                                // 停止していたノードが復帰した場合
                                // 停止中に登録予定であったデータを登録する
                                logger.info("Node Name [" + nodeInfo +"] Use Wait 1-1 Start");

                                // 復旧前に現在稼働中のMasterNodeに再度停止ノードと、リカバー開始を伝える
                                super.setDeadNode(nodeInfo, 1, null, true);
                                super.setRecoverNode(true, (String)subNodeList.get(i));

                                logger.info(nodeInfo + " - Recover Start");
                                StatusUtil.setNodeStatusDt(nodeInfo, "Recover Start");

                                // 復旧開始
                                if(nodeDataRecover(nodeInfo, (String)subNodeList.get(i), logger)) {

                                    // リカバー成功
                                    // 該当ノードの復帰を登録
                                    logger.info(nodeInfo + " - Recover Success");

                                    // 復旧処理ノードから記録を消す
                                    rebootNodeMap.remove(nodeInfo);
                                    StatusUtil.setNodeStatusDt(nodeInfo, "Recover Success");
                                } else {
                                    // リカバー失敗
                                    logger.info(nodeInfo + " - Recover Miss");
                                    // リカバー終了を伝える
                                    super.setRecoverNode(false, "");
                                    StatusUtil.setNodeStatusDt(nodeInfo, "Recover Miss");
                                }

                                logger.info(nodeInfo + " - Recover End");
                            } else {
                                // ノードの復旧を記録
                                // 復旧処理ノードから記録を消す
                                rebootNodeMap.remove(nodeInfo);
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
                                super.setDeadNode(subNodeInfo, 2,null, true);
                                StatusUtil.setNodeStatusDt(subNodeDt[0] + ":" +  subNodeDt[1], "SubNode Check Dead");
                            } else if (!super.isNodeArrival(subNodeInfo)) {

                                // 停止していたノードが復帰した場合
                                // 停止中に登録予定であったデータを登録する
                                logger.info("Node Name [" + subNodeInfo +"] Reboot");

                                // ノード復旧処理を記録
                                if (rebootNodeMap.containsKey(subNodeInfo)) {
                                    Integer recoverCount = (Integer)rebootNodeMap.get(subNodeInfo);
                                    int recoverCountInt = recoverCount.intValue();
                                    recoverCountInt++;
                                    rebootNodeMap.put(subNodeInfo, new Integer(recoverCountInt));
                                } else {
                                    rebootNodeMap.put(subNodeInfo, new Integer(1));
                                }

                                // 復旧前に現在稼働中のMasterNodeに再度停止ノードと、リカバー開始を伝える
                                super.setDeadNode(subNodeInfo, 1, null, true);
                                super.setRecoverNode(true, nodeInfo);

                                logger.info(subNodeInfo + " - Recover Start");
                                StatusUtil.setNodeStatusDt(subNodeInfo, "Recover Start");
                                // 復旧開始
                                if(nodeDataRecover(subNodeInfo, nodeInfo, logger)) {

                                    // リカバー成功
                                    // 該当ノードの復帰を登録
                                    logger.info(subNodeInfo + " - Recover Success");

                                    // 復旧処理ノードから記録を消す
                                    rebootNodeMap.remove(subNodeInfo);
                                    StatusUtil.setNodeStatusDt(subNodeInfo, "Recover Success");
                                } else {

                                    // リカバー失敗
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


                // リブートされたが遂になるノードが存在せずにリカバリーに失敗したノードを復旧扱いとする
                Set rebootNodeMapSet = rebootNodeMap.entrySet();
                Iterator rebootNodeMapIte = rebootNodeMapSet.iterator();

                while(rebootNodeMapIte.hasNext()) {

                    Map.Entry obj = (Map.Entry)rebootNodeMapIte.next();
                    String nodeInfo = (String)obj.getKey();
                    Integer recoverTryCount = (Integer)obj.getValue();

                    if (nodeInfo != null && recoverTryCount.intValue() > 3) {
                        // リカバー完了を全MasterNodeへ送信
                        super.setArriveNode(nodeInfo);
                        rebootNodeMapIte.remove();

                        logger.info("Node Name [" + nodeInfo +"] Reboot Register");
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

            // データコピー開始
            StringBuilder buf = new StringBuilder();


            logger.info("Recover Step - 1");

            // もともと生存していたノードを差分モードOnにする
            buf = new StringBuilder();
            // 処理番号22
            buf.append("22").append(ImdstDefine.keyHelperClientParamSep).append("true");
            // 送信
            mpw.println(buf.toString());
            mpw.flush();
            String diffModeOn = mbr.readLine();

            logger.info("Recover Step - 2");

            // コピー元からデータ読み込み
            buf = new StringBuilder();
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
            buf = new StringBuilder();
            buf.append("21");
            buf.append(ImdstDefine.keyHelperClientParamSep);
            buf.append(lineCount);
            // 送信
            pw.println(buf.toString());
            pw.flush();
            logger.info("Recover Step - 5 Recover Data Schedule Line Count =[" + lineCount + "]");

            for (int i = 0; i < Integer.MAX_VALUE; i++) {
                // 値を書き出し
                logger.info("Recover Step - 6 [" + i + "]");
                retParam = mbr.readLine();

                if (retParam == null || retParam.trim().equals("") || (retParam.length() < 3 && retParam.trim().equals("-1"))) {
                    pw.println("-1");
                    pw.flush();
                    break;
                } else {
                    pw.println(retParam);
                }

                logger.info("Recover Step - 7 [" + i + "]");
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
            buf = new StringBuilder();
            // 処理番号24
            buf.append("24");
            buf.append(ImdstDefine.keyHelperClientParamSep);
            buf.append("true");

            // 送信
            mpw.println(buf.toString());
            mpw.flush();


            logger.info("Recover Step - 11");
            // 差分データを送る
            buf = new StringBuilder();
            buf.append("25");
            buf.append(ImdstDefine.keyHelperClientParamSep);
            pw.println(buf.toString());
            pw.flush();

            // データを送信
            String diffDataStr = null;
            while((diffDataStr = mbr.readLine()) != null) {

                if (diffDataStr.equals("-1")) break;
                pw.println(diffDataStr);
                pw.flush();
            }

            pw.println("-1");
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