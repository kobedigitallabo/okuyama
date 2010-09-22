package test.job;

import java.io.*;
import java.net.*;

import org.okuyama.base.lang.BatchException;
import org.okuyama.base.job.AbstractJob;
import org.okuyama.base.job.AbstractHelper;
import org.okuyama.base.job.IJob;
import org.okuyama.base.util.ILogger;
import org.okuyama.base.util.LoggerFactory;
import org.okuyama.imdst.util.KeyMapManager;
import org.okuyama.imdst.util.StatusUtil;
import org.okuyama.imdst.util.JavaSystemApi;

/**
 * Serverのリソース全般を管理する.<br>
 * メモリの使用状況の管理.<br>
 * Serverソケット関係の終了を監視.<br>
 * Parameterファイルに設定されているマーカーファイル郡を使用して管理を行う.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class TestJob extends AbstractJob implements IJob {

    // 停止ファイルの監視サイクル時間(ミリ秒)
    private int checkCycle = 5000;

    /**
     * Logger.<br>
     */
    private static ILogger logger = LoggerFactory.createLogger(TestJob.class);

    // 初期化メソッド定義
    public void initJob(String initValue) {
        logger.debug("TestJob - initJob - start");
        logger.debug("TestJob - initJob - end");
    }

    // Jobメイン処理定義
    public String executeJob(String optionParam) throws BatchException {
        logger.debug("TestJob - executeJob - start");
        String ret = SUCCESS;

        try{
            int helperCode = super.executeHelper("TestHelper", null);

            Object[] helperRet = null;
            while(helperRet == null) {
                 helperRet = super.waitGetHelperReturnParam(helperCode, 10);
            }

        } catch(Exception e) {
            logger.error("TestJob - executeJob - Error", e);
            throw new BatchException(e);
        }

        logger.debug("TestJob - executeJob - end");
        return ret;
    }

}