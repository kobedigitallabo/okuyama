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
 * 主に設定ファイルを監視して変更時の読み込み、設定を行う.<br>
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
        String serverStopMarkerFileName = null;
        File serverStopMarkerFile = null;

        boolean serverRunning = true;

        String reloadKeys[] = new String[5];
        reloadKeys[0] = ImdstDefine.Prop_KeyMapNodesInfo;
        reloadKeys[1] = ImdstDefine.Prop_SubKeyMapNodesInfo;
        reloadKeys[2] = ImdstDefine.Prop_KeyMapNodesRule;
        reloadKeys[3] = ImdstDefine.Prop_MainMasterNodeMode;
        reloadKeys[4] = ImdstDefine.Prop_SlaveMasterNodes;

        int checkCycle = 5000;

        try{
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


        String keyMapNodesStr = super.getPropertiesValue(ImdstDefine.Prop_KeyMapNodesInfo);
        String subKeyMapNodesStr = super.getPropertiesValue(ImdstDefine.Prop_SubKeyMapNodesInfo);
        String ruleStrProp = super.getPropertiesValue(ImdstDefine.Prop_KeyMapNodesRule);

        // ノード追加によりルールが変更されている可能性があるのパース
        // ルールは最新ルールが先頭に来るように設定される想定なので、先頭文字列を取得
        String[] ruleStrs = ruleStrProp.split(",") ;
        // 過去ルールを自身に保存
        int[] oldRules = null;
        if (ruleStrs.length > 1) {
            oldRules = new int[ruleStrs.length - 1];
            for (int i = 1; i < ruleStrs.length; i++) {
                oldRules[i - 1] = new Integer(ruleStrs[i].trim()).intValue();
            }
        }

        // DataDispatcher初期化
        DataDispatcher.init(ruleStrs[0], oldRules, keyMapNodesStr, subKeyMapNodesStr);

        // StatusUtilを初期化
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

        StatusUtil.initNodeExecMap(allNodeInfos);
    }
}