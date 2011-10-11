package test.job;

import java.io.*;
import java.net.*;

import okuyama.base.lang.BatchException;
import okuyama.base.job.AbstractJob;
import okuyama.base.job.AbstractHelper;
import okuyama.base.job.IJob;
import okuyama.base.util.ILogger;
import okuyama.base.util.LoggerFactory;
import okuyama.imdst.util.KeyMapManager;
import okuyama.imdst.util.StatusUtil;
import okuyama.imdst.util.JavaSystemApi;

import okuyama.imdst.util.FileBaseDataMap;
import okuyama.imdst.client.*;


/**
 * QueueClient用のテストを実行するJob.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class QueueClientTestJob extends AbstractJob implements IJob {

    // 停止ファイルの監視サイクル時間(ミリ秒)
    private int checkCycle = 5000;

    private String masterNodeInfo = null;


    /**
     * Logger.<br>
     */
    private static ILogger logger = LoggerFactory.createLogger(QueueClientTestJob.class);

    // 初期化メソッド定義
    // initValueはMasterNodeの定義
    // フォーマット
    // IP:ポート,IP:ポート,...
    public void initJob(String initValue) {
        logger.debug("QueueClientTestJob - initJob - start");
        this.masterNodeInfo = initValue;
        logger.debug("QueueClientTestJob - initJob - end");
    }

    // Jobメイン処理定義
    // 想定しているoptionParamは、"テスト用のPutHelper数","テスト用のTakeHelper数","テスト用のQueue名","テスト時の1クライアントの投入数"
    public String executeJob(String optionParam) throws BatchException {
        logger.debug("QueueClientTestJob - executeJob - start");
        String ret = SUCCESS;

        String[] parameterList = optionParam.split(",");
        try{
            OkuyamaQueueClient queueClient = new OkuyamaQueueClient();
            queueClient.setConnectionInfos(masterNodeInfo.split(","));
            queueClient.autoConnect();
            queueClient.createQueueSpace(parameterList[2]);
            queueClient.setValue("key", "0");
            queueClient.close();

            int putHelperCount = Integer.parseInt(parameterList[0]);
            int takeHelperCount = Integer.parseInt(parameterList[1]);

            for (int idx = 0; idx < putHelperCount; idx++) {
                queueClient = new OkuyamaQueueClient();
                queueClient.setConnectionInfos(masterNodeInfo.split(","));
                queueClient.autoConnect();

                // Queueへの投入のHelperを起動
                Object[] queuePutHelperParams = new Object[4];
                queuePutHelperParams[0] = queueClient; // Client
                queuePutHelperParams[1] = parameterList[2]; // QueueName
                queuePutHelperParams[2] = new Integer(parameterList[3]); // Data Count
                queuePutHelperParams[3] = parameterList[2] + "_" + "p" + idx; // Prefix
                super.executeHelper("QueueClientPutTestHelper", queuePutHelperParams);
            }

            // データ投入を少し先行させるために一旦停止
            Thread.sleep(10000);

            // QueueTake用のHelper
            for (int idx = 0; idx < takeHelperCount; idx++) {
                queueClient = new OkuyamaQueueClient();
                queueClient.setConnectionInfos(masterNodeInfo.split(","));
                queueClient.autoConnect();

                // Queueへの投入のHelperを起動
                Object[] queueTakeHelperParams = new Object[2];
                queueTakeHelperParams[0] = queueClient; // Client
                queueTakeHelperParams[1] = parameterList[2]; // QueueName
                super.executeHelper("QueueClientTakeTestHelper", queueTakeHelperParams);
            }

        } catch(Exception e) {
            logger.error("QueueClientTestJob - executeJob - Error", e);
            throw new BatchException(e);
        }

        logger.debug("QueueClientTestJob - executeJob - end");
        return ret;
    }

}