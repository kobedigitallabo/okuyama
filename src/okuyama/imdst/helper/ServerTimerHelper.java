package okuyama.imdst.helper;

import java.io.*;
import java.util.*;
import java.net.*;

import okuyama.base.JavaMain;
import okuyama.base.lang.BatchException;
import okuyama.base.job.AbstractHelper;
import okuyama.base.job.IJob;
import okuyama.base.util.ILogger;
import okuyama.base.util.LoggerFactory;
import okuyama.imdst.util.KeyMapManager;
import okuyama.imdst.util.ImdstDefine;
import okuyama.imdst.util.DataDispatcher;
import okuyama.imdst.util.StatusUtil;
import okuyama.imdst.util.JavaSystemApi;

/**
 * <br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class ServerTimerHelper extends AbstractHelper {

    private int timerInterval = 100;


    /**
     * Logger.<br>
     */
    private static ILogger logger = LoggerFactory.createLogger(ServerTimerHelper.class);


    // 初期化メソッド定義
    public void initHelper(String initValue) {
    }


    // メイン処理定義
    public String executeHelper(String optionParam) throws BatchException {
        logger.debug("ServerTimerHelper - executeHelper - start");
        String ret = SUCCESS;
        try{

            while (true) {
                Thread.sleep(this.timerInterval);
                // 現在のミリ秒をセット
                JavaSystemApi.currentTimeMillis = System.currentTimeMillis();

                // 現在の時間(Hour)をセット
                Calendar cal = Calendar.getInstance(); 
                int hour = cal.get(cal.HOUR_OF_DAY);
                JavaSystemApi.currentDateHour = hour;

                if(StatusUtil.getStatus() != 0) break;
            }
        } catch(Exception e) {
            e.printStackTrace();
            logger.error("ServerTimerHelper - executeHelper - Error", e);
            throw new BatchException(e);
        }

        logger.debug("ServerTimerHelper - executeHelper - end");
        return ret;
    }


    /**
     * Helper後処理.<br>
     *
     */
    public void endHelper() {
    }

}