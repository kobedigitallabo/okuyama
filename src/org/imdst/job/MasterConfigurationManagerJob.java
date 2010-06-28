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

import org.imdst.client.ImdstKeyValueClient;

/**
 * MasterNodeの設定関係を管理するJob<br>
 * 主に設定ファイルの初期読み込み及び、データノード上に存在する設定情報を監視して<br>
 * 変更があった場合は取り込む.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class MasterConfigurationManagerJob extends AbstractJob implements IJob {

    private String keyMapNodesStr = null;
    private String subKeyMapNodesStr = null;
    private String ruleStrProp = null;

    private String loadBalanceStr = null;

    // トランザクションノードの設定
    private String transactionModeStr = null;
    private String transactionManagerStr = null;

    // マスターノードの設定
    // 自身がMasterNodeかの判定情報(旧設定)
    private String mainMasterNodeModeStr = null;
    // Slaveマスターノードの接続情報(旧設定)
    private String slaveMasterNodeInfoStr = null;

    // 自身の情報
    private String myNodeInfoStr = null;
    // メインマスターノード接続情報
    private String mainMasterNodeInfoStr = null;
    // 全てのマスターノードの接続情報
    private String allMasterNodeInfoStr = null;


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
                if (super.getPropertiesValue(ImdstDefine.Prop_SystemConfigMode) != null &&
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
                } else {

                    // DataNode
                    parseAllNodesInfo4Node();
                }


                // 自身がチェックしなければいけないMasterNodeに対して生存確認を行う
                boolean arrivalFlg = false;
                String[] checkMasterNodes = null;
                String[] checkMasterNodeInfo = null;
                ImdstKeyValueClient imdstKeyValueClient = null;
                String checkMasterNodeStr = StatusUtil.getCheckTargetMasterNodes();

                if (checkMasterNodeStr != null && !checkMasterNodeStr.trim().equals("")) {

                    checkMasterNodes = checkMasterNodeStr.split(",");

                    arrivalFlg = false;
                    for (int idx = 0; idx < checkMasterNodes.length; idx++) {

                        
                        try {
                            checkMasterNodeInfo = checkMasterNodes[idx].split(":");

                            String node = checkMasterNodeInfo[0];
                            String port = checkMasterNodeInfo[1];

                            imdstKeyValueClient = new ImdstKeyValueClient();
                            imdstKeyValueClient.connect(node, Integer.parseInt(port));

                            if(imdstKeyValueClient.arrivalMasterNode()) {
                                arrivalFlg = true;
                            }
                        } catch(Exception e) {
                            logger.info("Master Node = [" + checkMasterNodes[idx] +  "] Check Error");
                        } finally {
                            if (imdstKeyValueClient != null) {
                                imdstKeyValueClient.close();
                                imdstKeyValueClient = null;
                            }
                        }
                    }

                    if (!arrivalFlg) {

                        if (super.getPropertiesValue(ImdstDefine.Prop_SystemConfigMode) != null &&
                                super.getPropertiesValue(ImdstDefine.Prop_SystemConfigMode).equals(ImdstDefine.configModeFile)) {

                            // ファイルモード
                            // 自身がメインマスターノード
                            StatusUtil.setMainMasterNode(true);
                        } else {

                            // Nodeモード
                            // 自身がメインマスターノード
                            // メインマスターノードの項目に自身の情報を登録
                            // そうすることで自動的に設定は変わる
                            String myInfo = StatusUtil.getMyNodeInfo();
                            String[] myInfos = myInfo.split(":");
                            String node = myInfos[0];
                            String port = myInfos[1];

                            try {

                                // ノードに登録
                                imdstKeyValueClient = new ImdstKeyValueClient();
                                imdstKeyValueClient.connect(node, Integer.parseInt(port));
                                imdstKeyValueClient.setValue(ImdstDefine.ConfigSaveNodePrefix + ImdstDefine.Prop_MainMasterNodeInfo, StatusUtil.getMyNodeInfo());
                            } catch(Exception e) {
                                logger.error(node + ":" + port + " MasterNode Regist Error" + e.toString());
                            } finally {
                                if (imdstKeyValueClient != null) {
                                    imdstKeyValueClient.close();
                                    imdstKeyValueClient = null;
                                }
                            }
                        }
                    } else {

                        if (super.getPropertiesValue(ImdstDefine.Prop_SystemConfigMode) != null &&
                                super.getPropertiesValue(ImdstDefine.Prop_SystemConfigMode).equals(ImdstDefine.configModeFile)) {

                            // ファイルモード
                            // 自身がメインマスターノードではない
                            StatusUtil.setMainMasterNode(false);
                        } else {

                            // Nodeモード
                            // 自身がメインマスターノードではない
                            StatusUtil.setMainMasterNode(false);
                        }
                    }
                } else {

                    // 調べるMasterNodeがない場合は自身がMainMasterNode
                    if (super.getPropertiesValue(ImdstDefine.Prop_SystemConfigMode) != null &&
                            super.getPropertiesValue(ImdstDefine.Prop_SystemConfigMode).equals(ImdstDefine.configModeFile)) {

                        // ファイルモード
                        // 自身がメインマスターノード
                        StatusUtil.setMainMasterNode(true);
                    } else {

                        // Nodeモード
                        // 自身がメインマスターノード
                        // メインマスターノードの項目に自身の情報を登録
                        // そうすることで自動的に設定は変わる
                        String myInfo = StatusUtil.getMyNodeInfo();
                        String[] myInfos = myInfo.split(":");
                        String node = myInfos[0];
                        String port = myInfos[1];

                        try {

                            // ノードに登録
                            imdstKeyValueClient = new ImdstKeyValueClient();
                            imdstKeyValueClient.connect(node, Integer.parseInt(port));
                            imdstKeyValueClient.setValue(ImdstDefine.ConfigSaveNodePrefix + ImdstDefine.Prop_MainMasterNodeInfo, StatusUtil.getMyNodeInfo());
                        } catch(Exception e) {
                            logger.error(node + ":" + port + " MasterNode Regist Error" + e.toString());
                        } finally {
                            if (imdstKeyValueClient != null) {
                                imdstKeyValueClient.close();
                                imdstKeyValueClient = null;
                            }
                        }
                    }
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
        // データノードの設定
        keyMapNodesStr = super.getPropertiesValue(ImdstDefine.Prop_KeyMapNodesInfo);
        subKeyMapNodesStr = super.getPropertiesValue(ImdstDefine.Prop_SubKeyMapNodesInfo);
        ruleStrProp = super.getPropertiesValue(ImdstDefine.Prop_KeyMapNodesRule);
        loadBalanceStr = super.getPropertiesValue(ImdstDefine.Prop_LoadBalanceMode);

        // トランザクションノードの設定
        transactionModeStr = super.getPropertiesValue(ImdstDefine.Prop_TransactionMode);
        transactionManagerStr = super.getPropertiesValue(ImdstDefine.Prop_TransactionManagerInfo);

        // マスターノードの設定
        // 自身がMasterNodeかの判定情報(旧設定)
        mainMasterNodeModeStr = super.getPropertiesValue(ImdstDefine.Prop_MainMasterNodeMode);
        // Slaveマスターノードの接続情報(旧設定)
        slaveMasterNodeInfoStr = super.getPropertiesValue(ImdstDefine.Prop_SlaveMasterNodes);

        // 自身の情報
        myNodeInfoStr = super.getPropertiesValue(ImdstDefine.Prop_MyNodeInfo);
        StatusUtil.setMyNodeInfo(myNodeInfoStr);

        // メインマスターノード接続情報
        mainMasterNodeInfoStr = super.getPropertiesValue(ImdstDefine.Prop_MainMasterNodeInfo);
        // 全てのマスターノードの接続情報
        allMasterNodeInfoStr = super.getPropertiesValue(ImdstDefine.Prop_AllMasterNodeInfo);

        this.infomationSetter();
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
    private void parseAllNodesInfo4Node() {
        ImdstKeyValueClient imdstKeyValueClient = null;
        String[] nodeRet = null;
        boolean setterFlg = false;
        try {
            // 設定上のメインマスターノードにまず接続
            if (mainMasterNodeInfoStr != null) {
                try {
                    String[] connectMainMasterNodeInfo = mainMasterNodeInfoStr.split(":");
                    imdstKeyValueClient = new ImdstKeyValueClient();
                    imdstKeyValueClient.setConnectionInfos(allMasterNodeInfoStr.split(","));
                    imdstKeyValueClient.connect(connectMainMasterNodeInfo[0], Integer.parseInt(connectMainMasterNodeInfo[1]));
                } catch (Throwable e){
                    imdstKeyValueClient = null;
                }
            } 

            if (imdstKeyValueClient == null) {
                imdstKeyValueClient = new ImdstKeyValueClient();
                imdstKeyValueClient.setConnectionInfos(allMasterNodeInfoStr.split(","));
                imdstKeyValueClient.autoConnect();
            }

            // データノードの設定
            nodeRet = imdstKeyValueClient.getValue(ImdstDefine.ConfigSaveNodePrefix + ImdstDefine.Prop_KeyMapNodesInfo);
            if(nodeRet[0].equals("true") && nodeRet[1] != null) {
                if(!nodeRet[1].equals(keyMapNodesStr)) {
                    keyMapNodesStr = nodeRet[1];
                    setterFlg = true;
                }
            } else if (nodeRet[0].equals("false") && StatusUtil.isMainMasterNode()) {
                // 設定情報の枠がない場合は自身の情報を登録
                imdstKeyValueClient.setValue(ImdstDefine.ConfigSaveNodePrefix + ImdstDefine.Prop_KeyMapNodesInfo, keyMapNodesStr);
            }

            nodeRet = imdstKeyValueClient.getValue(ImdstDefine.ConfigSaveNodePrefix + ImdstDefine.Prop_SubKeyMapNodesInfo);
            if(nodeRet[0].equals("true") && nodeRet[1] != null) {
                if(!nodeRet[1].equals(subKeyMapNodesStr)) {
                    subKeyMapNodesStr = nodeRet[1];
                    setterFlg = true;
                }
            } else if (nodeRet[0].equals("false") && StatusUtil.isMainMasterNode()) {
                // 設定情報の枠がない場合は自身の情報を登録
                imdstKeyValueClient.setValue(ImdstDefine.ConfigSaveNodePrefix + ImdstDefine.Prop_SubKeyMapNodesInfo, subKeyMapNodesStr);
            }


            nodeRet = imdstKeyValueClient.getValue(ImdstDefine.ConfigSaveNodePrefix + ImdstDefine.Prop_KeyMapNodesRule);
            if(nodeRet[0].equals("true") && nodeRet[1] != null) {
                if(!nodeRet[1].equals(ruleStrProp)) {
                    ruleStrProp = nodeRet[1];
                    setterFlg = true;
                }
            } else if (nodeRet[0].equals("false") && StatusUtil.isMainMasterNode()) {
                // 設定情報の枠がない場合は自身の情報を登録
                imdstKeyValueClient.setValue(ImdstDefine.ConfigSaveNodePrefix + ImdstDefine.Prop_KeyMapNodesRule, ruleStrProp);
            }

            nodeRet = imdstKeyValueClient.getValue(ImdstDefine.ConfigSaveNodePrefix + ImdstDefine.Prop_LoadBalanceMode);
            if(nodeRet[0].equals("true") && nodeRet[1] != null) {
                if(!nodeRet[1].equals(loadBalanceStr)) {
                    loadBalanceStr = nodeRet[1];
                    setterFlg = true;
                }
            } else if (nodeRet[0].equals("false") && StatusUtil.isMainMasterNode()) {
                // 設定情報の枠がない場合は自身の情報を登録
                imdstKeyValueClient.setValue(ImdstDefine.ConfigSaveNodePrefix + ImdstDefine.Prop_LoadBalanceMode, loadBalanceStr);
            }




            // トランザクションノードの設定
            nodeRet = imdstKeyValueClient.getValue(ImdstDefine.ConfigSaveNodePrefix + ImdstDefine.Prop_TransactionMode);
            if(nodeRet[0].equals("true") && nodeRet[1] != null) {
                if(!nodeRet[1].equals(transactionModeStr)) {
                    transactionModeStr = nodeRet[1];
                    setterFlg = true;
                }
            } else if (nodeRet[0].equals("false") && StatusUtil.isMainMasterNode()) {
                // 設定情報の枠がない場合は自身の情報を登録
                imdstKeyValueClient.setValue(ImdstDefine.ConfigSaveNodePrefix + ImdstDefine.Prop_TransactionMode, transactionModeStr);
            }

            nodeRet = imdstKeyValueClient.getValue(ImdstDefine.ConfigSaveNodePrefix + ImdstDefine.Prop_TransactionManagerInfo);
            if(nodeRet[0].equals("true") && nodeRet[1] != null) {
                if(!nodeRet[1].equals(transactionManagerStr)) {
                    transactionManagerStr = nodeRet[1];
                    setterFlg = true;
                }
            } else if (nodeRet[0].equals("false") && StatusUtil.isMainMasterNode()) {
                // 設定情報の枠がない場合は自身の情報を登録
                imdstKeyValueClient.setValue(ImdstDefine.ConfigSaveNodePrefix + ImdstDefine.Prop_TransactionManagerInfo, transactionManagerStr);
            }



            // マスターノードの設定
            // 自身がMasterNodeかの判定情報(旧設定)
            nodeRet = imdstKeyValueClient.getValue(ImdstDefine.ConfigSaveNodePrefix + ImdstDefine.Prop_MainMasterNodeMode);
            if(nodeRet[0].equals("true") && nodeRet[1] != null) {
                if(!nodeRet[1].equals(mainMasterNodeModeStr)) {
                    mainMasterNodeModeStr = nodeRet[1];
                    setterFlg = true;
                }
            } else if (nodeRet[0].equals("false") && StatusUtil.isMainMasterNode()) {
                // 設定情報の枠がない場合は自身の情報を登録
                imdstKeyValueClient.setValue(ImdstDefine.ConfigSaveNodePrefix + ImdstDefine.Prop_MainMasterNodeMode, mainMasterNodeModeStr);
            }

            // Slaveマスターノードの接続情報(旧設定)
            nodeRet = imdstKeyValueClient.getValue(ImdstDefine.ConfigSaveNodePrefix + ImdstDefine.Prop_SlaveMasterNodes);
            if(nodeRet[0].equals("true") && nodeRet[1] != null) {
                if(!nodeRet[1].equals(slaveMasterNodeInfoStr)) {
                    slaveMasterNodeInfoStr = nodeRet[1];
                    setterFlg = true;
                }
            } else if (nodeRet[0].equals("false") && StatusUtil.isMainMasterNode()) {
                // 設定情報の枠がない場合は自身の情報を登録
                imdstKeyValueClient.setValue(ImdstDefine.ConfigSaveNodePrefix + ImdstDefine.Prop_SlaveMasterNodes, slaveMasterNodeInfoStr);
            }




            // メインマスターノード接続情報
            nodeRet = imdstKeyValueClient.getValue(ImdstDefine.ConfigSaveNodePrefix + ImdstDefine.Prop_MainMasterNodeInfo);
            if(nodeRet[0].equals("true") && nodeRet[1] != null) {
                if(!nodeRet[1].equals(mainMasterNodeInfoStr)) {
                    mainMasterNodeInfoStr = nodeRet[1];
                    setterFlg = true;
                }
            } else if (nodeRet[0].equals("false") && StatusUtil.isMainMasterNode()) {
                // 設定情報の枠がない場合は自身の情報を登録
                imdstKeyValueClient.setValue(ImdstDefine.ConfigSaveNodePrefix + ImdstDefine.Prop_MainMasterNodeInfo, mainMasterNodeInfoStr);
            }

            // 全てのマスターノードの接続情報
            nodeRet = imdstKeyValueClient.getValue(ImdstDefine.ConfigSaveNodePrefix + ImdstDefine.Prop_AllMasterNodeInfo);
            if(nodeRet[0].equals("true") && nodeRet[1] != null) {
                if(!nodeRet[1].equals(allMasterNodeInfoStr)) {
                    allMasterNodeInfoStr = nodeRet[1];
                    setterFlg = true;
                }
            } else if (nodeRet[0].equals("false") && StatusUtil.isMainMasterNode()) {
                // 設定情報の枠がない場合は自身の情報を登録
                imdstKeyValueClient.setValue(ImdstDefine.ConfigSaveNodePrefix + ImdstDefine.Prop_AllMasterNodeInfo, allMasterNodeInfoStr);
            }


            if (setterFlg) {
                this.infomationSetter();
            }
        } catch (Exception e) {
            logger.error(e);
        } finally {
            try {
                if (imdstKeyValueClient != null) imdstKeyValueClient.close();
            } catch(Exception e) {
            }
        }
    }

    // 情報を反映する
    private void infomationSetter() {

        String[] mainKeyNodes = null;
        String[] subKeyNodes = new String[0];
        String[] allNodeInfos = null;
        int allNodeCounter = 0;

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



                // 自身がチェックしなければいけないMasterノードを登録
                StatusUtil.setCheckTargetMasterNodes("");
                if (allMasterNodeInfoStr != null) {

                    String checkTargetMasterNodes = null;
                    if (allMasterNodeInfoStr.indexOf(myNodeInfoStr) > 0) {
                        String[] workStrs = allMasterNodeInfoStr.split(myNodeInfoStr);

                        if (workStrs.length > 0) {
                            if(!workStrs[0].trim().equals("")) {
                                StatusUtil.setCheckTargetMasterNodes(workStrs[0]);
                            }
                        }
                    }
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