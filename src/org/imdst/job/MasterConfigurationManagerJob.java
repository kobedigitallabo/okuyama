package org.imdst.job;

import java.io.*;
import java.net.*;

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
import org.imdst.util.StatusUtil;

/**
 * MasterNodeの設定関係を管理するJob<br>
 * 主に設定ファイルの初期読み込み及び、データノード上に存在する設定情報を監視して<br>
 * 変更があった場合は取り込む.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class MasterConfigurationManagerJob extends AbstractJob implements IJob {


    /**
     * Logger.<br>
     */
    private static ILogger logger = LoggerFactory.createLogger(MasterConfigurationManagerJob.class);

    // 初期化メソッド定義
    public void initJob(String initValue) {
        logger.debug("MasterConfigurationManagerJob - initJob - start");
        logger.debug("MasterConfigurationManagerJob - initJob - end");
    }

    // Jobメイン処理定義
    public String executeJob(String optionParam) throws BatchException {
        logger.debug("MasterConfigurationManagerJob - executeJob - start");
        String ret = SUCCESS;
        int checkCycle = 2500;

        String serverStopMarkerFileName = null;
        File serverStopMarkerFile = null;

        boolean serverRunning = true;

        String reloadKeys[] = new String[12];
        reloadKeys[0] = ImdstDefine.Prop_KeyMapNodesInfo;
        reloadKeys[1] = ImdstDefine.Prop_SubKeyMapNodesInfo;
        reloadKeys[2] = ImdstDefine.Prop_KeyMapNodesRule;
        reloadKeys[3] = ImdstDefine.Prop_MainMasterNodeMode;
        reloadKeys[4] = ImdstDefine.Prop_SlaveMasterNodes;
        reloadKeys[5] = ImdstDefine.Prop_LoadBalanceMode;
        reloadKeys[6] = ImdstDefine.Prop_SystemConfigMode;
        reloadKeys[7] = ImdstDefine.Prop_MyNodeInfo;
        reloadKeys[8] = ImdstDefine.Prop_TransactionMode;
        reloadKeys[9] = ImdstDefine.Prop_TransactionManagerInfo;
        reloadKeys[10] = ImdstDefine.Prop_MainMasterNodeInfo;
        reloadKeys[11] = ImdstDefine.Prop_AllMasterNodeInfo;


        try{
            // 起動時は設定ファイルから情報取得
            this.parseAllNodesInfo();

            while (serverRunning) {

                // 停止ファイル関係チェック
                if (StatusUtil.getStatus() == 1) {
                    serverRunning = false;
                    logger.info("MasterConfigurationManagerJob - 状態異常です Msg = [" + StatusUtil.getStatusMessage() + "]");
                }

                if (StatusUtil.getStatus() == 2) {
                    serverRunning = false;
                    logger.info("MasterConfigurationManagerJob - 終了状態です");
                }

                serverStopMarkerFileName = super.getPropertiesValue("ServerStopFile");

                serverStopMarkerFile = new File(new File(serverStopMarkerFileName).getAbsolutePath());
                if (serverStopMarkerFile.exists()) {
                    serverRunning = false;
                    logger.info("MasterConfigurationManagerJob - Server停止ファイルが存在します");
                    StatusUtil.setStatus(2);
                }

                // 設定情報を設定ファイルから常に取得するモードとデータノードから取得する設定で処理分岐
                if (super.getPropertiesValue(ImdstDefine.Prop_SystemConfigMode) == null ||
                        super.getPropertiesValue(ImdstDefine.Prop_SystemConfigMode).equals(ImdstDefine.configModeFile)) {

                    // ファイルモード
                    // 設定ファイルの変更をチェック
                    if (super.isJobFileChange()) {
                        // 変更あり
                        logger.info("MasterNode Config File Change");
                        super.reloadJobFileParameter(reloadKeys);
                        this.parseAllNodesInfo();
                    } else {
                        // 変更なし
                        logger.info("MasterNode Config File No Change");
                    }
                } else if (super.getPropertiesValue(ImdstDefine.Prop_SystemConfigMode).equals(ImdstDefine.configModeNode)) {

                    // DataNode
                    
                }

                Thread.sleep(checkCycle);
            }
        } catch(Exception e) {
            logger.error("MasterConfigurationManagerJob - executeJob - Error", e);
            throw new BatchException(e);
        }

        //logger.debug("MasterConfigurationManagerJob - executeJob - end");
        return ret;
    }


    /**
     * KeyMapNodes,DataNodesの情報をパースする<br>
     * <br>
     * 以下の要素を設定する.<br>
     * KeyMapNodesRule=ルール値(2,9,99,999)<br>
     * KeyMapNodesInfo=Keyノードの設定(KeyNodeName1:11111, KeyNodeName2:22222)<br>
     * SubKeyMapNodesInfo=Keyノードの設定(KeyNodeName1:11111, KeyNodeName2:22222)<br>
     * <br>
     * 記述の決まり.<br>
     */
    private void parseAllNodesInfo() {
        String[] mainKeyNodes = null;
        String[] subKeyNodes = new String[0];
        String[] allNodeInfos = null;
        int allNodeCounter = 0;

        // データノードの設定
        String keyMapNodesStr = super.getPropertiesValue(ImdstDefine.Prop_KeyMapNodesInfo);
        String subKeyMapNodesStr = super.getPropertiesValue(ImdstDefine.Prop_SubKeyMapNodesInfo);
        String ruleStrProp = super.getPropertiesValue(ImdstDefine.Prop_KeyMapNodesRule);

        // トランザクションノードの設定
        String transactionModeStr = super.getPropertiesValue(ImdstDefine.Prop_TransactionMode);
        String transactionManagerStr = super.getPropertiesValue(ImdstDefine.Prop_TransactionManagerInfo);

        // マスターノードの設定
        // 自身がMasterNodeかの判定情報(旧設定)
        String mainMasterNodeModeStr = super.getPropertiesValue(ImdstDefine.Prop_MainMasterNodeMode);
        // Slaveマスターノードの接続情報(旧設定)
        String slaveMasterNodeInfoStr = super.getPropertiesValue(ImdstDefine.Prop_SlaveMasterNodes);

        // 自身の情報
        String myNodeInfoStr = super.getPropertiesValue(ImdstDefine.Prop_MyNodeInfo);
        // メインマスターノード接続情報
        String mainMasterNodeInfoStr = super.getPropertiesValue(ImdstDefine.Prop_MainMasterNodeInfo);
        // 全てのマスターノードの接続情報
        String allMasterNodeInfoStr = super.getPropertiesValue(ImdstDefine.Prop_AllMasterNodeInfo);



        // MainMasterNodeの情報解析

        // 旧情報から解析
        // 自身がMainMasterNodeか解析
        if (mainMasterNodeModeStr != null && 
                mainMasterNodeModeStr.equals("true")) {
            StatusUtil.setMainMasterNode(true);
        } else {
            StatusUtil.setMainMasterNode(false);
        }

        StatusUtil.setSlaveMasterNodes(slaveMasterNodeInfoStr);

        // 新設定を解析
        // 新設定が設定されている場合はこちらを優先とする
        if (myNodeInfoStr != null) {

            if (mainMasterNodeInfoStr != null) {

                if (mainMasterNodeInfoStr.trim().equals(myNodeInfoStr.trim())) {

                    // 自身がメインマスターノード
                    StatusUtil.setMainMasterNode(true);
                    if (allMasterNodeInfoStr != null) {

                        String[] allMasterNodeInfos = allMasterNodeInfoStr.trim().split(",");
                        StringBuffer slaveMasterNodeInfoBuf = new StringBuffer();
                        String sep  = "";
                        for (int idx = 0; idx < allMasterNodeInfos.length; idx++) {

                            if ((allMasterNodeInfos[idx].trim().equals(myNodeInfoStr.trim())) == false) {

                                slaveMasterNodeInfoBuf.append(sep);
                                slaveMasterNodeInfoBuf.append(allMasterNodeInfos[idx]);
                                sep = ",";
                            }
                        }

                        if (slaveMasterNodeInfoBuf.toString().equals("")) {
                            StatusUtil.setSlaveMasterNodes(null);
                        } else {
                            StatusUtil.setSlaveMasterNodes(slaveMasterNodeInfoBuf.toString());
                        }
                    } else{

                        StatusUtil.setSlaveMasterNodes(null);
                    }

                } else {

                    // 自身はメインマスターノードではない
                    StatusUtil.setMainMasterNode(false);
                    StatusUtil.setSlaveMasterNodes(null);
                }
            }
        }


        //DataNode情報解析

        // ノード追加によりルールが変更されている可能性があるのでパース
        // ルールは最新ルールが先頭に来るように設定される想定なので、先頭文字列を取得
        String[] ruleStrs = ruleStrProp.split(",") ;
        // 過去ルールを保存
        int[] oldRules = null;
        if (ruleStrs.length > 1) {
            oldRules = new int[ruleStrs.length - 1];
            for (int i = 1; i < ruleStrs.length; i++) {
                oldRules[i - 1] = new Integer(ruleStrs[i].trim()).intValue();
            }
        }

        mainKeyNodes = keyMapNodesStr.split(",");

        allNodeCounter = mainKeyNodes.length;

        if (subKeyMapNodesStr != null && !subKeyMapNodesStr.equals("")) {
            subKeyNodes = subKeyMapNodesStr.split(",");
            allNodeCounter = allNodeCounter + subKeyNodes.length;
        }

        allNodeInfos = new String[allNodeCounter];

        for (int i = 0; i < mainKeyNodes.length; i++) {
            allNodeInfos[i] = mainKeyNodes[i];
        }

        for (int i = 0; i < subKeyNodes.length; i++) {
            allNodeInfos[i + mainKeyNodes.length] = subKeyNodes[i];
        }

        // DataNodeの情報を初期化
        StatusUtil.initNodeExecMap(allNodeInfos);


        // TransactionNodeの情報を初期化
        if (transactionModeStr != null) {
            StatusUtil.setTransactionMode(new Boolean(transactionModeStr).booleanValue());
            if (StatusUtil.isTransactionMode()) {
                StatusUtil.setTransactionNode(transactionManagerStr.trim().split(":"));
            }
        } else {
            StatusUtil.setTransactionMode(false);
        }


        // DataDispatcher初期化
        DataDispatcher.init(ruleStrs[0], oldRules, keyMapNodesStr, subKeyMapNodesStr, transactionManagerStr);
    }
}