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
 * MasterNodeの監視を行うJob<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class MasterNodeCheckManagerJob extends AbstractJob implements IJob {


    /**
     * Logger.<br>
     */
    private static ILogger logger = LoggerFactory.createLogger(MasterNodeCheckManagerJob.class);

    // 初期化メソッド定義
    public void initJob(String initValue) {
        logger.debug("MasterNodeCheckManagerJob - initJob - start");
        logger.debug("MasterNodeCheckManagerJob - initJob - end");
    }

    // Jobメイン処理定義
    public String executeJob(String optionParam) throws BatchException {
        logger.debug("MasterNodeCheckManagerJob - executeJob - start");
        String ret = SUCCESS;
        int checkCycle = 5000;

        String serverStopMarkerFileName = null;
        File serverStopMarkerFile = null;

        boolean serverRunning = true;

        String checkMasterNodeStr = null;
        String[] checkMasterNodes = null;
        ImdstKeyValueClient imdstKeyValueClient = null;
        boolean arrivalFlg = false;
        try{
            while (serverRunning) {

                // 停止ファイル関係チェック
                if (StatusUtil.getStatus() == 1) {
                    serverRunning = false;
                    logger.info("MasterNodeCheckManagerJob - 状態異常です Msg = [" + StatusUtil.getStatusMessage() + "]");
                }

                if (StatusUtil.getStatus() == 2) {
                    serverRunning = false;
                    logger.info("MasterNodeCheckManagerJob - 終了状態です");
                }

                serverStopMarkerFileName = super.getPropertiesValue("ServerStopFile");

                serverStopMarkerFile = new File(new File(serverStopMarkerFileName).getAbsolutePath());
                if (serverStopMarkerFile.exists()) {
                    serverRunning = false;
                    logger.info("MasterNodeCheckManagerJob - Server停止ファイルが存在します");
                    StatusUtil.setStatus(2);
                }

                checkMasterNodeStr = StatusUtil.getCheckTargetMasterNodes();
                if (checkMasterNodeStr != null && !checkMasterNodeStr.trim().equals("")) {

                    checkMasterNodes = checkMasterNodeStr.split(",");
                    arrivalFlg = false;
                    for (int idx = 0; idx < checkMasterNodes.length; idx++) {
                        try {
                            String node = checkMasterNodes[0];
                            String port = checkMasterNodes[1];

                            imdstKeyValueClient = new ImdstKeyValueClient();
                            imdstKeyValueClient.connect(node, Integer.parseInt(port));
                            if(imdstKeyValueClient.arrivalMasterNode()) {
                                arrivalFlg = true;
                            }
                        } catch(Exception e) {
                        } finally {
                            if (imdstKeyValueClient != null) {
                                imdstKeyValueClient.close();
                                imdstKeyValueClient = null;
                            }
                        }
                    }

                    if (!arrivalFlg) {

                        if (super.getPropertiesValue(ImdstDefine.Prop_SystemConfigMode) == null ||
                                super.getPropertiesValue(ImdstDefine.Prop_SystemConfigMode).equals(ImdstDefine.configModeFile)) {

                            // ファイルモード
                            // 自身がメインマスターノード
                            StatusUtil.setMainMasterNode(true);
                            // スレーブマスターノードを登録
                        } else {

                            // Nodeモード
                            // 自身がメインマスターノード
                            StatusUtil.setMainMasterNode(true);
                            // 自身がメインマスターノードであることを登録するのとスレーブマスターノードの登録
                            String myInfo = StatusUtil.getMyNodeInfo();
                            String[] myInfos = myInfo.split(":");
                            String node = myInfos[0];
                            String port = myInfos[1];

                            imdstKeyValueClient = new ImdstKeyValueClient();
                            imdstKeyValueClient.connect(node, Integer.parseInt(port));
                            imdstKeyValueClient.setValue("","");

                        }
                    } else {

                        if (super.getPropertiesValue(ImdstDefine.Prop_SystemConfigMode) == null ||
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
                }
                Thread.sleep(checkCycle);
            }
        } catch(Exception e) {
            logger.error("MasterNodeCheckManagerJob - executeJob - Error", e);
            throw new BatchException(e);
        }

        //logger.debug("MasterNodeCheckManagerJob - executeJob - end");
        return ret;
    }
}