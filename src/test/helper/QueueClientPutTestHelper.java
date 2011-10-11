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
public class QueueClientPutTestHelper extends AbstractHelper {


    /**
     * Logger.<br>
     */
    private static ILogger logger = LoggerFactory.createLogger(QueueClientPutTestHelper.class);

    // 初期化メソッド定義
    public void initHelper(String initValue) {
    }

    // Jobメイン処理定義
    public String executeHelper(String optionParam) throws BatchException {
        logger.debug("QueueClientPutTestHelper - executeHelper - start");
        String ret = SUCCESS;

        int counter = 0;
        Object[] parameters = super.getParameters();

        OkuyamaQueueClient queueClient = (OkuyamaQueueClient)parameters[0];

        String queueName = (String)parameters[1]; // QueueName
        int dataCount = ((Integer)parameters[2]).intValue(); // Data Count
        String prefix = (String)parameters[3]; // UniquePrefix

        try{
            System.out.println("Queue Put Test Start QueueName [" + queueName + "] UniqueName[" + prefix + "]");
            long start = System.nanoTime();
            for (int i = 0; i < dataCount; i++) {
                String data = prefix + "_" + i;
                if(queueClient.put(queueName, data)) {
                    super.setHelperShareParam(data, "");
                } else {
                    System.out.println("Queue Put Error --------------------------------------- Data[" + data + "]");
                }
            }
            queueClient.close();
            long end = System.nanoTime();
            System.out.println("Queue Put Time=" + ((end - start) / 1000 / 1000) + " milli");
        } catch(Exception e) {
            logger.error("QueueClientPutTestHelper - executeHelper - Error", e);
            throw new BatchException(e);
        }

        logger.debug("QueueClientPutTestHelper - executeHelper - end");
        return ret;
    }


    /**
     * Helper後処理.<br>
     *
     */
    public void endHelper() {
    }
}