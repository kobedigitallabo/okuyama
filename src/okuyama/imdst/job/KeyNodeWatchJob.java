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
            // デフォルトは20秒
            if (optionParam != null && !optionParam.trim().equals("")) { 
                Thread.sleep(Integer.parseInt(optionParam));
            } else {
                Thread.sleep(20000);
            }

            int helperCode = super.executeHelper("KeyNodeWatchHelper", null, true);

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