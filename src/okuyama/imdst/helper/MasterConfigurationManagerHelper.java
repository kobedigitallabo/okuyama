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

import okuyama.imdst.client.ImdstKeyValueClient;

/**
 * KeyNodeの監視を行うHelperクラス<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class MasterConfigurationManagerHelper extends AbstractMasterManagerHelper {

    private String keyMapNodesStr = null;
    private String subKeyMapNodesStr = null;
    private String thirdKeyMapNodesStr = null;
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

    // ConsistentHashモード時にノードを追加する場合の登録値
    private String[] addNodeInfos = null;

    // 分散方式 デフォルトはmode
    // 他にはconsistenthash
    private String dispatchMode = ImdstDefine.dispatchModeMod;

    // 監視サイクル(秒)
    private int checkCycle = 1000 * 5;

    /**
     * Logger.<br>
     */
    private static ILogger logger = LoggerFactory.createLogger(MasterConfigurationManagerHelper.class);


    // 初期化メソッド定義
    public void initHelper(String initValue) {
        logger.debug("MasterConfigurationManagerHelper - initHelper - start");
        logger.debug("MasterConfigurationManagerHelper - initHelper - end");
    }

    // Jobメイン処理定義
    public String executeHelper(String optionParam) throws BatchException {
        logger.debug("MasterConfigurationManagerHelper - executeHelper - start");
        String ret = SUCCESS;


        String serverStopMarkerFileName = null;
        File serverStopMarkerFile = null;

        boolean serverRunning = true;

        String reloadKeys[] = new String[14];
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
        reloadKeys[12] = ImdstDefine.Prop_DistributionAlgorithm;
        reloadKeys[13] = ImdstDefine.Prop_ThirdKeyMapNodesInfo;

        if(super.getPropertiesValue(ImdstDefine.Prop_DistributionAlgorithm) != null) {
            if (super.getPropertiesValue(ImdstDefine.Prop_DistributionAlgorithm).equals(ImdstDefine.dispatchModeConsistentHash)) {
                dispatchMode = ImdstDefine.dispatchModeConsistentHash;
            }
        }

        // 振り分けモード設定
        StatusUtil.setDistributionAlgorithm(dispatchMode);
        DataDispatcher.setDispatchMode(dispatchMode);


        try{
            // 起動時は設定ファイルから情報取得
            this.parseAllNodesInfo();

            while (serverRunning) {

                // 停止ファイル関係チェック
                if (StatusUtil.getStatus() == 1) {
                    serverRunning = false;
                    logger.info("MasterConfigurationManagerHelper - 状態異常です Msg = [" + StatusUtil.getStatusMessage() + "]");
                }

                if (StatusUtil.getStatus() == 2) {
                    serverRunning = false;
                    logger.info("MasterConfigurationManagerHelper - 終了状態です");
                }

                serverStopMarkerFileName = super.getPropertiesValue("ServerStopFile");

                serverStopMarkerFile = new File(new File(serverStopMarkerFileName).getAbsolutePath());
                if (serverStopMarkerFile.exists()) {
                    serverRunning = false;
                    logger.info("MasterConfigurationManagerHelper - Server停止ファイルが存在します");
                    StatusUtil.setStatus(2);
                }

                try {
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

                                    // エラーが発生した場合は例外としノードに設定せずに自身の設定を変更

                                    StatusUtil.setMainMasterNode(true);
                                    mainMasterNodeModeStr = StatusUtil.getMyNodeInfo();
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

                                // エラーが発生した場合は例外としノードに設定せずに自身の設定を変更
                                StatusUtil.setMainMasterNode(true);
                                mainMasterNodeModeStr = StatusUtil.getMyNodeInfo();
                            } finally {
                                if (imdstKeyValueClient != null) {
                                    imdstKeyValueClient.close();
                                    imdstKeyValueClient = null;
                                }
                            }
                        }
                    }
                } catch (Exception innerE) {
                    logger.error("MasterConfigurationManagerHelper - executeHelper - Inner Error", innerE);
                }
                Thread.sleep(this.checkCycle);
            }
        } catch(Exception e) {
            logger.error("MasterConfigurationManagerHelper - executeHelper - Error", e);
            throw new BatchException(e);
        }

        //logger.debug("MasterConfigurationManagerHelper - executeHelper - end");
        return ret;
    }


    /**
     * Helper後処理.<br>
     *
     */
    public void endHelper() {
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
        thirdKeyMapNodesStr = super.getPropertiesValue(ImdstDefine.Prop_ThirdKeyMapNodesInfo);
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

        // DataDispatcher初期化
        if (dispatchMode.equals(ImdstDefine.dispatchModeConsistentHash)) {
            this.infomationSetterConsistentHash();
        } else {
            this.infomationSetter();
        }
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
            logger.info("[" + ImdstDefine.ConfigSaveNodePrefix + ImdstDefine.Prop_KeyMapNodesInfo + "] Get Method Call [" + System.nanoTime() + "]");
            nodeRet = imdstKeyValueClient.getValue(ImdstDefine.ConfigSaveNodePrefix + ImdstDefine.Prop_KeyMapNodesInfo);
            if(nodeRet[0].equals("true") && nodeRet[1] != null) {
                if(!nodeRet[1].equals(keyMapNodesStr)) {
                    keyMapNodesStr = nodeRet[1];
                    setterFlg = true;
                }
            } else if (nodeRet[0].equals("false") && StatusUtil.isMainMasterNode()) {

                // 設定情報の枠がない場合は自身の情報を登録
                imdstKeyValueClient.setValue(ImdstDefine.ConfigSaveNodePrefix + ImdstDefine.Prop_KeyMapNodesInfo, keyMapNodesStr);
            } else if (nodeRet[0].equals("error")) {
                // 何もしない
            }

            // サブデータノードの設定
            logger.info("[" + ImdstDefine.ConfigSaveNodePrefix + ImdstDefine.Prop_SubKeyMapNodesInfo + "] Get Method Call [" + System.nanoTime() + "]");
            nodeRet = imdstKeyValueClient.getValue(ImdstDefine.ConfigSaveNodePrefix + ImdstDefine.Prop_SubKeyMapNodesInfo);
            if(nodeRet[0].equals("true") && nodeRet[1] != null) {
                if(!nodeRet[1].equals(subKeyMapNodesStr)) {
                    subKeyMapNodesStr = nodeRet[1];
                    setterFlg = true;
                }
            } else if (nodeRet[0].equals("false") && StatusUtil.isMainMasterNode()) {

                if (subKeyMapNodesStr != null && !subKeyMapNodesStr.trim().equals("")) {
                    // 設定情報の枠がない場合は自身の情報を登録
                    imdstKeyValueClient.setValue(ImdstDefine.ConfigSaveNodePrefix + ImdstDefine.Prop_SubKeyMapNodesInfo, subKeyMapNodesStr);
                }
            } else if (nodeRet[0].equals("error")) {
                // 何もしない
            }


            // サードデータノードの設定
            nodeRet = imdstKeyValueClient.getValue(ImdstDefine.ConfigSaveNodePrefix + ImdstDefine.Prop_ThirdKeyMapNodesInfo);

            if(nodeRet[0].equals("true") && nodeRet[1] != null) {

                if(!nodeRet[1].equals(thirdKeyMapNodesStr)) {

                    thirdKeyMapNodesStr = nodeRet[1];
                    setterFlg = true;
                }
            } else if (nodeRet[0].equals("false") && StatusUtil.isMainMasterNode()) {

                // 設定情報の枠がない場合は自身の情報を登録
                if (thirdKeyMapNodesStr != null && !thirdKeyMapNodesStr.trim().equals("")) {
                    imdstKeyValueClient.setValue(ImdstDefine.ConfigSaveNodePrefix + ImdstDefine.Prop_ThirdKeyMapNodesInfo, thirdKeyMapNodesStr);
                }
            } else if (nodeRet[0].equals("error")) {
                // 何もしない
            }



            nodeRet = imdstKeyValueClient.getValue(ImdstDefine.ConfigSaveNodePrefix + ImdstDefine.Prop_KeyMapNodesRule);
            if(nodeRet[0].equals("true") && nodeRet[1] != null) {
                if(!nodeRet[1].equals(ruleStrProp)) {
                    ruleStrProp = nodeRet[1];
                    setterFlg = true;
                }
            } else if (nodeRet[0].equals("false") && StatusUtil.isMainMasterNode()) {

                if (ruleStrProp != null && !ruleStrProp.trim().equals("")) {
                    // 設定情報の枠がない場合は自身の情報を登録
                    imdstKeyValueClient.setValue(ImdstDefine.ConfigSaveNodePrefix + ImdstDefine.Prop_KeyMapNodesRule, ruleStrProp);
                }
            } else if (nodeRet[0].equals("error")) {
                // 何もしない
            }


            nodeRet = imdstKeyValueClient.getValue(ImdstDefine.ConfigSaveNodePrefix + ImdstDefine.Prop_LoadBalanceMode);
            if(nodeRet[0].equals("true") && nodeRet[1] != null) {
                if(!nodeRet[1].equals(loadBalanceStr)) {
                    loadBalanceStr = nodeRet[1];
                    setterFlg = true;
                }
            } else if (nodeRet[0].equals("false") && StatusUtil.isMainMasterNode()) {

                if (loadBalanceStr != null && !loadBalanceStr.trim().equals("")) {
                    // 設定情報の枠がない場合は自身の情報を登録
                    imdstKeyValueClient.setValue(ImdstDefine.ConfigSaveNodePrefix + ImdstDefine.Prop_LoadBalanceMode, loadBalanceStr);
                }
            } else if (nodeRet[0].equals("error")) {
                // 何もしない
            }


            // トランザクションノードの設定
            nodeRet = imdstKeyValueClient.getValue(ImdstDefine.ConfigSaveNodePrefix + ImdstDefine.Prop_TransactionMode);
            if(nodeRet[0].equals("true") && nodeRet[1] != null) {
                if(!nodeRet[1].equals(transactionModeStr)) {
                    transactionModeStr = nodeRet[1];
                    setterFlg = true;
                }
            } else if (nodeRet[0].equals("false") && StatusUtil.isMainMasterNode()) {

                if (transactionModeStr != null && !transactionModeStr.trim().equals("")) {
                    // 設定情報の枠がない場合は自身の情報を登録
                    imdstKeyValueClient.setValue(ImdstDefine.ConfigSaveNodePrefix + ImdstDefine.Prop_TransactionMode, transactionModeStr);
                }
            } else if (nodeRet[0].equals("error")) {
                // 何もしない
            }


            nodeRet = imdstKeyValueClient.getValue(ImdstDefine.ConfigSaveNodePrefix + ImdstDefine.Prop_TransactionManagerInfo);
            if(nodeRet[0].equals("true") && nodeRet[1] != null) {
                if(!nodeRet[1].equals(transactionManagerStr)) {
                    transactionManagerStr = nodeRet[1];
                    setterFlg = true;
                }
            } else if (nodeRet[0].equals("false") && StatusUtil.isMainMasterNode()) {

                if (transactionManagerStr != null && !transactionManagerStr.trim().equals("")) {
                    // 設定情報の枠がない場合は自身の情報を登録
                    imdstKeyValueClient.setValue(ImdstDefine.ConfigSaveNodePrefix + ImdstDefine.Prop_TransactionManagerInfo, transactionManagerStr);
                }
            } else if (nodeRet[0].equals("error")) {
                // 何もしない
            }


            // マスターノードの設定
            // 自身がMasterNodeかの判定情報(旧設定)
            nodeRet = imdstKeyValueClient.getValue(ImdstDefine.ConfigSaveNodePrefix + ImdstDefine.Prop_MainMasterNodeMode);
            if(nodeRet[0].equals("true") && nodeRet[1] != null) {
                if (mainMasterNodeModeStr != null) {
                    if(!nodeRet[1].equals(mainMasterNodeModeStr)) {
                        mainMasterNodeModeStr = nodeRet[1];
                        setterFlg = true;
                    }
                }
            } else if (nodeRet[0].equals("false") && StatusUtil.isMainMasterNode()) {

                if (mainMasterNodeModeStr != null && !mainMasterNodeModeStr.trim().equals("")) {
                    // 設定情報の枠がない場合は自身の情報を登録
                    imdstKeyValueClient.setValue(ImdstDefine.ConfigSaveNodePrefix + ImdstDefine.Prop_MainMasterNodeMode, mainMasterNodeModeStr);
                }
            } else if (nodeRet[0].equals("error")) {
                // 何もしない
            }


            // Slaveマスターノードの接続情報(旧設定)
            nodeRet = imdstKeyValueClient.getValue(ImdstDefine.ConfigSaveNodePrefix + ImdstDefine.Prop_SlaveMasterNodes);
            if(nodeRet[0].equals("true") && nodeRet[1] != null) {
                if (slaveMasterNodeInfoStr != null) {
                    if(!nodeRet[1].equals(slaveMasterNodeInfoStr)) {
                        slaveMasterNodeInfoStr = nodeRet[1];
                        setterFlg = true;
                    }
                }
            } else if (nodeRet[0].equals("false") && StatusUtil.isMainMasterNode()) {

                if (slaveMasterNodeInfoStr != null && !slaveMasterNodeInfoStr.trim().equals("")) {
                    // 設定情報の枠がない場合は自身の情報を登録
                    imdstKeyValueClient.setValue(ImdstDefine.ConfigSaveNodePrefix + ImdstDefine.Prop_SlaveMasterNodes, slaveMasterNodeInfoStr);
                }
            } else if (nodeRet[0].equals("error")) {
                // 何もしない
            }


            // メインマスターノード接続情報
            nodeRet = imdstKeyValueClient.getValue(ImdstDefine.ConfigSaveNodePrefix + ImdstDefine.Prop_MainMasterNodeInfo);
            if(nodeRet[0].equals("true") && nodeRet[1] != null) {
                if(!nodeRet[1].equals(mainMasterNodeInfoStr)) {
                    mainMasterNodeInfoStr = nodeRet[1];
                    setterFlg = true;
                }
            } else if (nodeRet[0].equals("false") && StatusUtil.isMainMasterNode()) {

                if (mainMasterNodeInfoStr != null && !mainMasterNodeInfoStr.trim().equals("")) {

                    // 設定情報の枠がない場合は自身の情報を登録
                    imdstKeyValueClient.setValue(ImdstDefine.ConfigSaveNodePrefix + ImdstDefine.Prop_MainMasterNodeInfo, mainMasterNodeInfoStr);
                }
            } else if (nodeRet[0].equals("error")) {
                // 何もしない
            }


            // 全てのマスターノードの接続情報
            nodeRet = imdstKeyValueClient.getValue(ImdstDefine.ConfigSaveNodePrefix + ImdstDefine.Prop_AllMasterNodeInfo);
            if(nodeRet[0].equals("true") && nodeRet[1] != null) {
                if(!nodeRet[1].equals(allMasterNodeInfoStr)) {
                    allMasterNodeInfoStr = nodeRet[1];
                    setterFlg = true;
                }
            } else if (nodeRet[0].equals("false") && StatusUtil.isMainMasterNode()) {
                if (allMasterNodeInfoStr != null && !allMasterNodeInfoStr.trim().equals("")) {
                    // 設定情報の枠がない場合は自身の情報を登録
                    imdstKeyValueClient.setValue(ImdstDefine.ConfigSaveNodePrefix + ImdstDefine.Prop_AllMasterNodeInfo, allMasterNodeInfoStr);
                }
            } else if (nodeRet[0].equals("error")) {
                // 何もしない
            }


            // 全てのマスターノードの接続情報
            imdstKeyValueClient.setValue(ImdstDefine.ConfigSaveNodePrefix + ImdstDefine.Prop_DistributionAlgorithm, dispatchMode);


            // ConsistentHashModeの場合はノードの追加要望がないかを調べる
            // サークルへの追加アルゴリズムはまずノード一覧を設定ファイルもしくはノードから取り出す。
            // ノード一覧からサークルを作成する。
            // そのノードに追加対象ノードを追加してoldCircleと現行サークルの2つをDataDispatcher上に作り上げる
            // この時の戻り値のデータ範囲を利用して、メインマスターノードの場合はデータ移行を行う。
            // メインマスターノードはデータ移行が終了したタイミングで追加対象ノードの情報を
            // データノード上から消しこむ
            // 消えたタイミングで、DataDispatcherからもoldCircleを削除する
            if (dispatchMode.equals(ImdstDefine.dispatchModeConsistentHash)) {

                // 取得できる値のフォーマットは"192.168.1.5:5553,192.168.2.5:5553,192.168.2.5:5553"のようなフォーマット
                // 値が1つならメインノードのみ、2つならメインとスレーブ、3つならメイン、スレーブ、サードとなる
                String[] addNodeRequest = imdstKeyValueClient.getValue(ImdstDefine.ConfigSaveNodePrefix + ImdstDefine.addNode4ConsistentHashMode);

                if (addNodeRequest[0].equals("true")) {

                    if (this.addNodeInfos == null) {
                        setterFlg = true;
                        this.addNodeInfos = addNodeRequest[1].split(",");
                    } else {

                        // データ移行処理依頼中
                        // データ移行中はなにもしない
                        if (super.getConsistentHashMoveData() == null) {

                            // データ移行終了
                            // ノード中から移行依頼を消す
                            String[] removeRet = imdstKeyValueClient.removeValue(ImdstDefine.ConfigSaveNodePrefix + ImdstDefine.addNode4ConsistentHashMode);
                            if(removeRet[0].equals("true")) {
                                setterFlg = true;
                                this.addNodeInfos = null;
                            }
                        }
                    }
                }
            }

            // DataDispatcher初期化
            if (setterFlg) {
                if (dispatchMode.equals(ImdstDefine.dispatchModeConsistentHash)) {
                    this.infomationSetterConsistentHash();
                } else {
                    this.infomationSetter();
                }
            }

        } catch (Exception e) {

            logger.error(e);
        } finally {
            try {

                if (imdstKeyValueClient != null) {
                    imdstKeyValueClient.close();
                }
            } catch(Exception e) {
                logger.error(e);
            }
        }
    }

    // 情報を反映する
    private void infomationSetter() {

        String[] mainKeyNodes = null;
        String[] subKeyNodes = new String[0];
        String[] thirdKeyNodes = new String[0];
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
                        StringBuilder slaveMasterNodeInfoBuf = new StringBuilder();
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

        if (thirdKeyMapNodesStr != null && !thirdKeyMapNodesStr.equals("")) {
            thirdKeyNodes = thirdKeyMapNodesStr.split(",");
            allNodeCounter = allNodeCounter + thirdKeyNodes.length;
        }

        allNodeInfos = new String[allNodeCounter];

        for (int i = 0; i < mainKeyNodes.length; i++) {
            allNodeInfos[i] = mainKeyNodes[i];
        }

        for (int i = 0; i < subKeyNodes.length; i++) {
            allNodeInfos[i + mainKeyNodes.length] = subKeyNodes[i];
        }

        for (int i = 0; i < thirdKeyNodes.length; i++) {
            allNodeInfos[i + mainKeyNodes.length + subKeyNodes.length] = thirdKeyNodes[i];
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
        DataDispatcher.init(ruleStrs[0], oldRules, keyMapNodesStr, subKeyMapNodesStr, thirdKeyMapNodesStr, transactionManagerStr);

        super.executeKeyNodeOptimization(true);
    }


    // 情報を反映する
    // ConsistentHash用
    private void infomationSetterConsistentHash() {

        String[] mainKeyNodes = null;
        String[] subKeyNodes = new String[0];
        String[] thirdKeyNodes = new String[0];        
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
                        StringBuilder slaveMasterNodeInfoBuf = new StringBuilder();
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
        mainKeyNodes = keyMapNodesStr.split(",");

        allNodeCounter = mainKeyNodes.length;

        if (subKeyMapNodesStr != null && !subKeyMapNodesStr.equals("")) {
            subKeyNodes = subKeyMapNodesStr.split(",");
            allNodeCounter = allNodeCounter + subKeyNodes.length;
        }

        if (thirdKeyMapNodesStr != null && !thirdKeyMapNodesStr.equals("")) {
            thirdKeyNodes = thirdKeyMapNodesStr.split(",");
            allNodeCounter = allNodeCounter + thirdKeyNodes.length;
        }


        allNodeInfos = new String[allNodeCounter];

        for (int i = 0; i < mainKeyNodes.length; i++) {
            allNodeInfos[i] = mainKeyNodes[i];
        }

        for (int i = 0; i < subKeyNodes.length; i++) {
            allNodeInfos[i + mainKeyNodes.length] = subKeyNodes[i];
        }

        for (int i = 0; i < thirdKeyNodes.length; i++) {
            allNodeInfos[i + mainKeyNodes.length + subKeyNodes.length] = thirdKeyNodes[i];
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


        // ConsistentHashの場合は都度初期化は出来ないので、確認
        if (!DataDispatcher.getInitFlg()) {
            DataDispatcher.initConsistentHashMode(keyMapNodesStr, subKeyMapNodesStr, thirdKeyMapNodesStr, transactionManagerStr);
        }

        // 追加データノードの情報がnullの場合はDataDispatcherから削除
        if (addNodeInfos == null) {

            DataDispatcher.clearConsistentHashOldCircle();
        } else {

            HashMap moveDataMap = null;
            // 移行対象が存在する場合のみ、moveDataMapはnullではなくなる
            if (addNodeInfos.length == 1) {
                moveDataMap = DataDispatcher.addNode4ConsistentHash(addNodeInfos[0], null, null);
            } else if (addNodeInfos.length == 2) {
                moveDataMap = DataDispatcher.addNode4ConsistentHash(addNodeInfos[0], addNodeInfos[1], null);
            } else if (addNodeInfos.length == 3) {
                moveDataMap = DataDispatcher.addNode4ConsistentHash(addNodeInfos[0], addNodeInfos[1], addNodeInfos[2]);
            }


            // MainMasterNodeの場合のみデータ移行を実行
            // ここでsuperのconsistentHashMoveDataに登録
            // KeyNodeOptimizationConsistentHashHelper側でこのデータを監視して、登録されたら、
            // 移行処理を開始する。
            // 移行完了後、superのconsistentHashMoveDataを削除する
            if (StatusUtil.isMainMasterNode()) {
                if (moveDataMap != null) {
                    super.setConsistentHashMoveData(moveDataMap);
                }
            }
        }
    }
}