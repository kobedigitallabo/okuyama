package okuyama.imdst.helper;

import java.io.*;
import java.util.*;
import java.net.*;

import okuyama.base.lang.BatchException;
import okuyama.base.job.AbstractHelper;
import okuyama.base.job.IJob;
import okuyama.base.util.ILogger;
import okuyama.base.util.LoggerFactory;
import okuyama.imdst.util.KeyMapManager;
import okuyama.imdst.util.ImdstDefine;
import okuyama.imdst.util.DataDispatcher;
import okuyama.imdst.util.StatusUtil;
import okuyama.imdst.util.SystemUtil;
import okuyama.imdst.util.JavaSystemApi;
import okuyama.imdst.util.io.CustomReader;

/**
 * <br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class MasterManagerConnectHelper extends AbstractMasterManagerHelper {

    /**
     * Logger.<br>
     */
    private static ILogger logger = LoggerFactory.createLogger(MasterManagerConnectHelper.class);


    // 初期化メソッド定義
    public void initHelper(String initValue) {
    }


    // Jobメイン処理定義
    public String executeHelper(String optionParam) throws BatchException {
        logger.debug("MasterManagerConnectHelper - executeHelper - start");
        String ret = SUCCESS;
        String serverStopMarkerFileName = null;
        File serverStopMarkerFile = null;
        String pollQueueName = null;
        String[] addQueueNames = null;

        boolean serverRunning = true;

        try{
            int i = 0;
            Object[] parameters = super.getParameters();

            // Queue名取得
            pollQueueName = (String)parameters[0];
            addQueueNames = (String[])parameters[1];

            while (serverRunning) {

                // キューから取り出し
                Object[] param = super.pollSpecificationParameterQueue(pollQueueName);
                if (param == null || param.length < 1) continue;

                Socket socket = (Socket)param[0];

                // ログ書き出し
                logger.info("Accept : " + socket.toString());
                SystemUtil.debugLine("Accept : " + socket.toString());

                BufferedOutputStream bos = new BufferedOutputStream(socket.getOutputStream());

                PrintWriter pw = new PrintWriter(
                                    new BufferedWriter(
                                        new OutputStreamWriter(socket.getOutputStream(), 
                                                                ImdstDefine.keyHelperClientParamEncoding)));
                InputStream is = socket.getInputStream();


                CustomReader cr = new CustomReader(is);

                Object[] clientMap = new Object[9];
                clientMap[ImdstDefine.paramSocket] = socket;
                clientMap[ImdstDefine.paramPw] = pw;
                clientMap[ImdstDefine.paramBr] = cr;
                clientMap[ImdstDefine.paramStart] = new Long(JavaSystemApi.currentTimeMillis);
                clientMap[ImdstDefine.paramLast] = new Long(JavaSystemApi.currentTimeMillis);
                clientMap[ImdstDefine.paramBalance] = param[1];
                clientMap[ImdstDefine.paramCheckCountMaster] = new Integer(0);
                clientMap[ImdstDefine.paramBis] = cr;
                clientMap[ImdstDefine.paramBos] = bos;
                Object[] queueParam = new Object[1];
                queueParam[0] = clientMap;

                // キューに追加
                super.addSmallSizeParameterQueue(addQueueNames, queueParam);
            }
        } catch(Exception e) {
            e.printStackTrace();
            logger.error("MasterManagerConnectHelper - executeHelper - Error", e);
            throw new BatchException(e);
        }

        logger.debug("MasterManagerConnectHelper - executeHelper - end");
        return ret;
    }


    /**
     * Helper後処理.<br>
     *
     */
    public void endHelper() {
    }

}