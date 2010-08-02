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
public class MasterManagerAcceptHelper extends AbstractMasterManagerHelper {

    // ノードの監視サイクル時間(ミリ秒)
    private long connetionTimeout = 30000;

    /**
     * Logger.<br>
     */
    private static ILogger logger = LoggerFactory.createLogger(MasterManagerAcceptHelper.class);

    // 初期化メソッド定義
    public void initHelper(String initValue) {
    }

    // Jobメイン処理定義
    public String executeHelper(String optionParam) throws BatchException {
        logger.debug("MasterManagerAcceptHelper - executeHelper - start");
        String ret = SUCCESS;
        String serverStopMarkerFileName = null;
        File serverStopMarkerFile = null;

        boolean serverRunning = true;


        try{
            while (serverRunning) {

                // 停止ファイル関係チェック
                if (StatusUtil.getStatus() == 1) {
                    serverRunning = false;
                    logger.info("MasterManagerAcceptHelper - 状態異常です");
                }

                if (StatusUtil.getStatus() == 2) {
                    serverRunning = false;
                    logger.info("MasterManagerAcceptHelper - 終了状態です");
                }

/*
                serverStopMarkerFileName = super.getPropertiesValue("ServerStopFile");

                serverStopMarkerFile = new File(new File(serverStopMarkerFileName).getAbsolutePath());
                if (serverStopMarkerFile.exists()) {
                    serverRunning = false;
                    logger.info("MasterManagerAcceptHelper - Server停止ファイルが存在します");
                    StatusUtil.setStatus(2);
                }
*/
                Object[] param = super.pollSpecificationParameterQueue("MasterManagerAcceptHelper");
                if (param == null || param.length < 1) continue;

                HashMap clientMap = (HashMap)param[0];
                BufferedReader br = (BufferedReader)clientMap.get("br");
                Socket socket = (Socket)clientMap.get("socket");

                if(br.ready()) {

                    clientMap.put("last", new Long(System.currentTimeMillis()));
                    Object[] queueParam = new Object[1];
                    queueParam[0] = clientMap;
                    super.addSpecificationParameterQueue("MasterManagerHelper", queueParam);
                } else {

                    try {
                        int test = 0;
                        br.mark(2);
    
                        long start = ((Long)clientMap.get("start")).longValue();
                        long last = ((Long)clientMap.get("last")).longValue();
                        

                        if ((System.currentTimeMillis() - last) < connetionTimeout) {
                            socket.setSoTimeout(1);

                            test = br.read();

                            br.reset(); 
                        } else {
                            test = -1;
                        }

                        if (test == -1) {
                            br.close();
                            socket.close();
                            br = null;
                            socket = null;
                        }

                    } catch (SocketTimeoutException se) {
                    } catch (Exception e) {
                        try {
                            br.close();
                            socket.close();
                            br = null;
                            socket = null;
                        } catch (Exception ee) {
                            br = null;
                            socket = null;
                        }
                    } 

                    if (socket != null) {
                        Object[] queueParam = new Object[1];
                        queueParam[0] = clientMap;
                        super.addSpecificationParameterQueue("MasterManagerAcceptHelper", queueParam);
                    }
                }
            }
        } catch(Exception e) {
            logger.error("MasterManagerAcceptHelper - executeHelper - Error", e);
            throw new BatchException(e);
        }

        logger.debug("MasterManagerAcceptHelper - executeHelper - end");
        return ret;
    }


    /**
     * Helper後処理.<br>
     *
     */
    public void endHelper() {
    }

}