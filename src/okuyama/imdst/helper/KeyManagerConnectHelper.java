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
import okuyama.imdst.util.JavaSystemApi;

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
        String pollQueueName = null;
        String[] addQueueNames = null;

        boolean serverRunning = true;

        try{

            Object[] parameters = super.getParameters();
            pollQueueName = (String)parameters[0];
            addQueueNames = (String[])parameters[1];

            while (serverRunning) {

                Object[] param = super.pollSpecificationParameterQueue(pollQueueName);

                if (param == null || param.length < 1) continue;

                Socket socket = (Socket)param[0];
                PrintWriter pw = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), ImdstDefine.keyHelperClientParamEncoding)));
                BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream(), ImdstDefine.keyHelperClientParamEncoding));

                Object[] clientMap = new Object[6];
                clientMap[ImdstDefine.paramSocket] = socket;
                clientMap[ImdstDefine.paramPw] = pw;
                clientMap[ImdstDefine.paramBr] = br;
                clientMap[ImdstDefine.paramStart] = new Long(JavaSystemApi.currentTimeMillis);
                clientMap[ImdstDefine.paramLast] = new Long(JavaSystemApi.currentTimeMillis);
                clientMap[ImdstDefine.paramCheckCount] = new Integer(0);

                Object[] queueParam = new Object[1];
                queueParam[0] = clientMap;
                super.addSmallSizeParameterQueue(addQueueNames, queueParam);
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