package org.imdst.helper;

import java.io.*;
import java.util.*;
import java.net.*;

import org.batch.lang.BatchException;
import org.batch.job.AbstractHelper;
import org.batch.job.IJob;
import org.batch.util.ILogger;
import org.batch.util.LoggerFactory;
import org.imdst.util.KeyMapManager;
import org.imdst.util.ImdstDefine;
import org.imdst.util.DataDispatcher;
import org.imdst.util.StatusUtil;

/**
 * <br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class KeyManagerConnectHelper extends AbstractHelper {

    private String queuePrefix = null;

    /**
     * Logger.<br>
     */
    private static ILogger logger = LoggerFactory.createLogger(KeyManagerConnectHelper.class);


    // 初期化メソッド定義
    public void initHelper(String initValue) {
    }


    // Jobメイン処理定義
    public String executeHelper(String optionParam) throws BatchException {
        logger.debug("KeyManagerConnectHelper - executeHelper - start");
        String ret = SUCCESS;
        String serverStopMarkerFileName = null;
        File serverStopMarkerFile = null;
        String pollQueueName = "KeyManagerConnectHelper";
        String addQueueName = "KeyManagerAcceptHelper";

        boolean serverRunning = true;

        try{

            Object[] parameters = super.getParameters();
            this.queuePrefix = (String)parameters[0];
            pollQueueName = pollQueueName + this.queuePrefix;
            addQueueName = addQueueName + this.queuePrefix;

            while (serverRunning) {

                // 停止ファイル関係チェック
                if (StatusUtil.getStatus() == 1) {
                    serverRunning = false;
                    logger.info("KeyManagerConnectHelper - 状態異常です");
                }

                if (StatusUtil.getStatus() == 2) {
                    serverRunning = false;
                    logger.info("KeyManagerConnectHelper - 終了状態です");
                }
                Object[] param = super.pollSpecificationParameterQueue(pollQueueName);

                if (param == null || param.length < 1) continue;

                Socket socket = (Socket)param[0];
                PrintWriter pw = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), ImdstDefine.keyHelperClientParamEncoding)));
                BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream(), ImdstDefine.keyHelperClientParamEncoding));

                Object[] clientMap = new Object[5];
                clientMap[ImdstDefine.paramSocket] = socket;
                clientMap[ImdstDefine.paramPw] = pw;
                clientMap[ImdstDefine.paramBr] = br;
                clientMap[ImdstDefine.paramStart] = new Long(System.currentTimeMillis());
                clientMap[ImdstDefine.paramLast] = new Long(System.currentTimeMillis());

                Object[] queueParam = new Object[1];
                queueParam[0] = clientMap;
                super.addSpecificationParameterQueue(addQueueName, queueParam);
            }
        } catch(Exception e) {
            logger.error("KeyManagerConnectHelper - executeHelper - Error", e);
            throw new BatchException(e);
        }

        logger.debug("KeyManagerConnectHelper - executeHelper - end");
        return ret;
    }


    /**
     * Helper後処理.<br>
     *
     */
    public void endHelper() {
    }

}