package org.imdst.job;

import java.io.*;
import java.net.*;
import java.util.*;

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
import org.imdst.util.JavaSystemApi;

/**
 * DataNodeのデータ整合性維持を行う.<br>
 *
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class DataConsistencyAdjustmentJob extends AbstractJob implements IJob {

    /**
     * Logger.<br>
     */
    private static ILogger logger = LoggerFactory.createLogger(DataConsistencyAdjustmentJob.class);

    // 初期化メソッド定義
    public void initJob(String initValue) {
        logger.debug("DataConsistencyAdjustmentJob - initJob - start");
        logger.debug("DataConsistencyAdjustmentJob - initJob - end");
    }

    // Jobメイン処理定義
    public String executeJob(String optionParam) throws BatchException {
        logger.debug("DataConsistencyAdjustmentJob - executeJob - start");
        String ret = SUCCESS;
        
        try{
            // 起動してから一定時間停止する
            // DataNodeを同時立ち上げた場合の対応
            // デフォルトは20秒
            if (optionParam != null && !optionParam.trim().equals("")) { 
                Thread.sleep(Integer.parseInt(optionParam));
            } else {
                Thread.sleep(20000);
            }

            // 起動準備完了まで待つ
            StatusUtil.isStandby();
 
            int helperCode = 0;

            helperCode = super.executeHelper("DataConsistencyAdjustmentHelper", null);

            Object[] helperRet = null;
            while(helperRet == null) {
                 helperRet = super.waitGetHelperReturnParam(helperCode, 10);
            }
        } catch(Exception e) {
            logger.error("DataConsistencyAdjustmentJob - executeJob - Error", e);
            throw new BatchException(e);
        }

        logger.debug("DataConsistencyAdjustmentJob - executeJob - end");
        return ret;
    }
}