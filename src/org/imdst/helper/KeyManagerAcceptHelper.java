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
 * KeyManagerの使用する接続ソケットを監視し、読み込み待ちのソケットを見つけ出し、<br>
 * 処理対象のキューに登録する.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class KeyManagerAcceptHelper extends AbstractHelper {

    // 無操作上限時間 -1は切断なし
    private long connetionTimeout = 60000 * 5;

    private String queuePrefix = null;

    /**
     * Logger.<br>
     */
    private static ILogger logger = LoggerFactory.createLogger(KeyManagerAcceptHelper.class);

    // 初期化メソッド定義
    public void initHelper(String initValue) {
    }

    // Jobメイン処理定義
    public String executeHelper(String optionParam) throws BatchException {
        logger.debug("KeyManagerAcceptHelper - executeHelper - start");

        String ret = SUCCESS;
        String serverStopMarkerFileName = null;
        File serverStopMarkerFile = null;
        String pollQueueName = "KeyManagerAcceptHelper";
        String addExecQueueName = "KeyManagerHelper";
        String addCheckQueueName = "KeyManagerAcceptHelper";

        boolean serverRunning = true;

        try{
            Object[] parameters = super.getParameters();
            this.queuePrefix = (String)parameters[0];
            pollQueueName = pollQueueName + this.queuePrefix;
            addExecQueueName = addExecQueueName + this.queuePrefix;
            addCheckQueueName = addCheckQueueName + this.queuePrefix;

            while (serverRunning) {

                // 停止ファイル関係チェック
                if (StatusUtil.getStatus() == 1) {
                    serverRunning = false;
                    logger.info("KeyManagerAcceptHelper - 状態異常です");
                }

                if (StatusUtil.getStatus() == 2) {
                    serverRunning = false;
                    logger.info("KeyManagerAcceptHelper - 終了状態です");
                }

                
                Object[] param = super.pollSpecificationParameterQueue(pollQueueName);
                if (param == null || param.length < 1) continue;

                Object[] clientMap = (Object[])param[0];
                BufferedReader br = (BufferedReader)clientMap[ImdstDefine.paramBr];
                Socket socket = (Socket)clientMap[ImdstDefine.paramSocket];

                // 読み込みのデータがバッファに存在するかをチェック
                if(br.ready()) {
                    // 読み込みのデータがバッファに存在する
                    clientMap[ImdstDefine.paramLast] = new Long(System.currentTimeMillis());
                    Object[] queueParam = new Object[1];
                    queueParam[0] = clientMap;

                    super.addSpecificationParameterQueue(addExecQueueName, queueParam, true);
                } else {

                    // 読み込みのデータがバッファに存在しない
                    try {
                        int test = 0;
                        br.mark(1);

                        // 無操作時間が上限に達していないかを確認
                        long start = ((Long)clientMap[ImdstDefine.paramStart]).longValue();
                        long last = ((Long)clientMap[ImdstDefine.paramLast]).longValue();

                        if (connetionTimeout == -1 || (System.currentTimeMillis() - last) < connetionTimeout) {

                            // 上限に達していない
                            // 既にコネクションが切断されていないかを確認
                            socket.setSoTimeout(1);
                            test = br.read();

                            br.reset(); 
                        } else {

                            // 上限に達している
                            test = -1;
                        }

                        // 無操作時間の上限もしくは、コネクション切断済み
                        if (test == -1) {

                            // クローズ
                            br.close();
                            socket.close();
                            br = null;
                            socket = null;
                        }

                    } catch (SocketTimeoutException se) {
                    } catch (Exception e) {
                        try {

                            // エラーの場合はクローズ
                            br.close();
                            socket.close();
                            br = null;
                            socket = null;
                        } catch (Exception ee) {
                            br = null;
                            socket = null;
                        }
                    } 

                    // 無操作時間が上限に達せず切断もされていない場合は再度確認キューに登録
                    if (socket != null) {
                        Object[] queueParam = new Object[1];
                        queueParam[0] = clientMap;
                        super.addSpecificationParameterQueue(addCheckQueueName, queueParam);
                    }
                }
            }
        } catch(Exception e) {
            logger.error("KeyManagerAcceptHelper - executeHelper - Error", e);
            throw new BatchException(e);
        }

        logger.debug("KeyManagerAcceptHelper - executeHelper - end");
        return ret;
    }


    /**
     * Helper後処理.<br>
     *
     */
    public void endHelper() {
    }

}