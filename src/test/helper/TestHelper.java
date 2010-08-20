package test.helper;

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
import org.imdst.util.io.KeyNodeConnector;


/**
 * TestHelperクラス<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class TestHelper extends AbstractHelper {

    /**
     * Logger.<br>
     */
    private static ILogger logger = LoggerFactory.createLogger(TestHelper.class);

    // 初期化メソッド定義
    public void initHelper(String initValue) {
    }

    // Jobメイン処理定義
    public String executeHelper(String optionParam) throws BatchException {
        logger.debug("TestHelper - executeHelper - start");
        String ret = SUCCESS;

        try{
            String keyMapNodes = "192.168.1.1:5553,192.168.1.2:5553,192.168.1.3:5553";
            String subKeyMapNodes = "192.168.2.1:6663,192.168.2.2:6663,192.168.2.3:6663";
            String thirdKeyMapNodes = "192.168.3.1:7773,192.168.3.2:7773,192.168.3.3:7773";
            String transactionManagerStr = "";
            StatusUtil.initNodeExecMap(keyMapNodes.split(","));
            StatusUtil.initNodeExecMap(subKeyMapNodes.split(","));
            StatusUtil.initNodeExecMap(thirdKeyMapNodes.split(","));
            StatusUtil.setDistributionAlgorithm("consistenthash");
            DataDispatcher.setDispatchMode("consistenthash");
            DataDispatcher.initConsistentHashMode(keyMapNodes, subKeyMapNodes, thirdKeyMapNodes, null);
//          DataDispatcher.init("3", null, keyMapNodes, subKeyMapNodes, thirdKeyMapNodes, null);

            for (int i = 0; i < 10; i++) {
                String[] ret1 = DataDispatcher.dispatchKeyNode("key---" + i, false);

                System.out.println("ret1[0] = " + ret1[0]);
                System.out.println("ret1[1] = " + ret1[1]);
                System.out.println("ret1[2] = " + ret1[2]);
                System.out.println("ret1[3] = " + ret1[3]);
                System.out.println("ret1[4] = " + ret1[4]);
                System.out.println("ret1[5] = " + ret1[5]);
                System.out.println("ret1[6] = " + ret1[6]);
                System.out.println("ret1[7] = " + ret1[7]);
                System.out.println("ret1[8] = " + ret1[8]);
                System.out.println("---------------------------------");
            }

            System.out.println("---------------------------------");
            System.out.println("---------------------------------");
            System.out.println("---------------------------------");

            keyMapNodes = "192.168.1.1:5553,192.168.1.2:5553,192.168.1.3:5553,192.168.1.4:5553";
            subKeyMapNodes = "192.168.2.1:6663,192.168.2.2:6663,192.168.2.3:6663,192.168.2.4:6663";
            thirdKeyMapNodes = "192.168.3.1:7773,192.168.3.2:7773,192.168.3.3:7773,192.168.3.4:7773";
            transactionManagerStr = "";

            HashMap moveDataMap = null;
            // 移行対象が存在する場合のみ、moveDataMapはnullではなくなる
            moveDataMap = DataDispatcher.addNode4ConsistentHash("192.168.1.4:5553", "192.168.2.4:6663", "192.168.3.4:7773");
            StatusUtil.initNodeExecMap(keyMapNodes.split(","));
            StatusUtil.initNodeExecMap(subKeyMapNodes.split(","));
            StatusUtil.initNodeExecMap(thirdKeyMapNodes.split(","));

            System.out.println(moveDataMap);

            System.out.println("---------------------------------");
            System.out.println("---------------------------------");
            System.out.println("---------------------------------");

            for (int i = 0; i < 10; i++) {
                String[] ret1 = DataDispatcher.dispatchKeyNode("key---" + i, false, 1);

                System.out.println("ret1[0] = " + ret1[0]);
                System.out.println("ret1[1] = " + ret1[1]);
                System.out.println("ret1[2] = " + ret1[2]);
                System.out.println("ret1[3] = " + ret1[3]);
                System.out.println("ret1[4] = " + ret1[4]);
                System.out.println("ret1[5] = " + ret1[5]);
                System.out.println("ret1[6] = " + ret1[6]);
                System.out.println("ret1[7] = " + ret1[7]);
                System.out.println("ret1[8] = " + ret1[8]);
                System.out.println("---------------------------------");
            }


        } catch(Exception e) {
            logger.error("TestHelper - executeHelper - Error", e);
            throw new BatchException(e);
        }

        logger.debug("TestHelper - executeHelper - end");
        return ret;
    }


    /**
     * Helper後処理.<br>
     *
     */
    public void endHelper() {
        logger.debug("TestHelper - endHelper - start");
        logger.debug("TestHelper - endHelper - end");
    }
}