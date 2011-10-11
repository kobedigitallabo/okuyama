package test.helper;

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
import okuyama.imdst.util.io.KeyNodeConnector;
import okuyama.imdst.client.*;

/**
 * QueueClientのPutTest用Helper <br>
 * 
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class QueueClientTakeTestHelper extends AbstractHelper {


    /**
     * Logger.<br>
     */
    private static ILogger logger = LoggerFactory.createLogger(QueueClientTakeTestHelper.class);

    // 初期化メソッド定義
    public void initHelper(String initValue) {
    }

    // Jobメイン処理定義
    public String executeHelper(String optionParam) throws BatchException {
        logger.debug("QueueClientTakeTestHelper - executeHelper - start");
        String ret = SUCCESS;

        int counter = 0;
        Object[] parameters = super.getParameters();

        OkuyamaQueueClient queueClient = (OkuyamaQueueClient)parameters[0];

        String queueName = (String)parameters[1]; // QueueName


        try{
            System.out.println("Queue Take Test Start QueueName [" + queueName + "]");
            long start = System.nanoTime();
            /*for (int idx = 0; idx < 50000; idx++) {



                while (true) {
                    String[] tmp = queueClient.getValueVersionCheck("key");
                    if (tmp[0].equals("true")) {
                        String tmpData = tmp[1];
                        tmpData = new Integer(new Integer(tmpData).intValue() + 1).toString();
                        String[] tmpUpdate = queueClient.setValueVersionCheck("key", tmpData, tmp[2]);

                        if (tmpUpdate[0].equals("true")) break;
                    }
                }
            }*/

            while (true) {

                String data = queueClient.take(queueName, 10000);
                if(data != null) {
                    String chk = (String)super.removeHelperShareParam(data);
                    if (chk == null) 
                        System.out.println("Queue Take Error --------------------------------------- Data[" + data + "]");
                } else {
                    break;
                }
            }
            //String[] tmp = queueClient.getValue("key");
            //System.out.println("IncrVal[" + tmp[1] + "]");
            queueClient.close();
            long end = System.nanoTime();
            System.out.println("Queue Take Test Take End Data Count [" + super.sizeHelperShareParam() + "]");
            System.out.println("Queue Take Time=" + ((end - start) / 1000 / 1000) + " milli");

        } catch(Exception e) {
            logger.error("QueueClientTakeTestHelper - executeHelper - Error", e);
            throw new BatchException(e);
        }

        logger.debug("QueueClientTakeTestHelper - executeHelper - end");
        return ret;
    }


    /**
     * Helper後処理.<br>
     *
     */
    public void endHelper() {
    }
}