package org.okuyama.imdst.job;

import java.io.*;
import java.net.*;
import java.util.*;

import org.okuyama.base.lang.BatchException;
import org.okuyama.base.job.AbstractJob;
import org.okuyama.base.job.AbstractHelper;
import org.okuyama.base.job.IJob;
import org.okuyama.base.util.ILogger;
import org.okuyama.base.util.LoggerFactory;
import org.okuyama.imdst.util.KeyMapManager;
import org.okuyama.imdst.util.StatusUtil;
import org.okuyama.imdst.util.ImdstDefine;
import org.okuyama.imdst.util.DataDispatcher;
import org.okuyama.imdst.util.JavaSystemApi;

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