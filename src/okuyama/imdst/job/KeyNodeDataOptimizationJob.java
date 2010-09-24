package okuyama.imdst.job;

import java.io.*;
import java.net.*;
import java.util.*;

import okuyama.base.lang.BatchException;
import okuyama.base.job.AbstractJob;
import okuyama.base.job.AbstractHelper;
import okuyama.base.job.IJob;
import okuyama.base.util.ILogger;
import okuyama.base.util.LoggerFactory;
import okuyama.imdst.util.KeyMapManager;
import okuyama.imdst.util.StatusUtil;
import okuyama.imdst.util.ImdstDefine;
import okuyama.imdst.util.DataDispatcher;
import okuyama.imdst.util.JavaSystemApi;

/**
 * KeyNodeから該当Keyノードが保持しているKey値を取り出、Getを実行しデータの最適化を行う.<br>
 *
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class KeyNodeDataOptimizationJob extends AbstractJob implements IJob {

    /**
     * Logger.<br>
     */
    private static ILogger logger = LoggerFactory.createLogger(KeyNodeDataOptimizationJob.class);

    // 初期化メソッド定義
    public void initJob(String initValue) {
        logger.debug("KeyNodeDataOptimizationJob - initJob - start");
        logger.debug("KeyNodeDataOptimizationJob - initJob - end");
    }

    // Jobメイン処理定義
    public String executeJob(String optionParam) throws BatchException {
        logger.debug("KeyNodeDataOptimizationJob - executeJob - start");
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

            if (ImdstDefine.dispatchModeConsistentHash.equals(StatusUtil.getDistributionAlgorithm())) {
                helperCode = super.executeHelper("KeyNodeOptimizationConsistentHashHelper", null);
            } else {
                helperCode = super.executeHelper("KeyNodeOptimizationHelper", null);
            }

            Object[] helperRet = null;
            while(helperRet == null) {
                 helperRet = super.waitGetHelperReturnParam(helperCode, 10);
            }
        } catch(Exception e) {
            logger.error("KeyNodeDataOptimizationJob - executeJob - Error", e);
            throw new BatchException(e);
        }

        logger.debug("KeyNodeDataOptimizationJob - executeJob - end");
        return ret;
    }
}