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
 * KeyNodeを監視して生存確認を行う.<br>
 * KeyNodeが停止している場合はそのNodeの復帰を待ち、復帰後、そのNodeとペアーのノードからデータ転送を行う.<br>
 *
 * Serverソケット関係の終了を監視.<br>
 * Parameterファイルに設定されているマーカーファイル郡を使用して管理を行う.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class KeyNodeWatchJob extends AbstractJob implements IJob {

    /**
     * Logger.<br>
     */
    private static ILogger logger = LoggerFactory.createLogger(KeyNodeWatchJob.class);

    // 初期化メソッド定義
    public void initJob(String initValue) {
        logger.debug("KeyNodeWatchJob - initJob - start");
        logger.debug("KeyNodeWatchJob - initJob - end");
    }

    // Jobメイン処理定義
    public String executeJob(String optionParam) throws BatchException {
        logger.debug("KeyNodeWatchJob - executeJob - start");
        String ret = SUCCESS;
		
        try{
			// 起動してから一定時間停止する
			// DataNodeを同時立ち上げた場合の対応
			// デフォルトは30秒
			if (optionParam != null && !optionParam.trim().equals("")) { 
				Thread.sleep(Integer.parseInt(optionParam));
			} else {
				Thread.sleep(30000);
			}
            int helperCode = super.executeHelper("KeyNodeWatchHelper", null);

            Object[] helperRet = null;
            while(helperRet == null) {
                 helperRet = super.waitGetHelperReturnParam(helperCode, 10);
            }
        } catch(Exception e) {
            logger.error("MasterManagerJob - executeJob - Error", e);
            throw new BatchException(e);
        }

        logger.debug("MasterManagerJob - executeJob - end");
        return ret;
    }
}