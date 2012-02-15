package test.job;

import java.io.*;
import java.net.*;
import java.util.*;

import okuyama.base.lang.BatchException;
import okuyama.base.job.AbstractJob;
import okuyama.base.job.AbstractHelper;
import okuyama.base.job.IJob;
import okuyama.base.util.ILogger;
import okuyama.base.util.LoggerFactory;
import okuyama.imdst.util.KeyMapManager;
import okuyama.imdst.util.StatusUtil;
import okuyama.imdst.util.JavaSystemApi;
import okuyama.imdst.client.*;

/**
 * DataNodeの停止、起動し、データの復元をテストする.<br>
 * このテストケースはWindows環境にCygwinをインストールし、PATHを設定した想定です.<br>
 * Linuxの場合はjpsが利用できれば稼働します<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class DataNodeRebootTestJob extends AbstractJob implements IJob {

    private int execCount = 5;
    private String testScriptPath = "C:";


    // 初期化メソッド定義
    public void initJob(String initValue) {
        // 実行回数を設定
        if(initValue != null && !initValue.equals("")) {
            execCount = Integer.parseInt(initValue);
        }
    }


    // Jobメイン処理定義
    public String executeJob(String optionParam) throws BatchException {

        String ret = SUCCESS;

        String stopCmd1 = null;
        String stopCmd2 = null;

        String startCmd1 = null;
        String startCmd2 = null;

        long nodeRebootWaitTime = 300000;
        try{
            // パス初期化
            if(optionParam != null && !optionParam.equals("")) {
                testScriptPath = optionParam;
            }

            Thread.sleep(10000);
            String recoverTimeStr = super.getPropertiesValue("RebootTime");
            if (recoverTimeStr != null && !recoverTimeStr.trim().equals("")) {
                nodeRebootWaitTime = Long.parseLong(recoverTimeStr);
            }

            for (int t = 0; t < this.execCount; t++) {
                String[] keyList = {"p" + t, "p_x" + t, "p_y" + t, "p_z" + t};
                execRestart(keyList, nodeRebootWaitTime);
            }


        } catch(Exception e) {
            throw new BatchException(e);
        }

        return ret;
    }


    private void execRestart(String[] keyPrefixList, long nodeRebootWaitTime) throws Exception {
        System.out.println("execStop - Start");
        String result = null;
        String osName = super.getPropertiesValue("OSName");

        String[] stop = new String[1];
        String[] start = new String[1];

        if (osName == null || osName.equals("") || osName.trim().equals("windows")) {
            stop[0] = testScriptPath + "\\execTestStopDataNode.bat";

            start[0] = testScriptPath + "\\execTestDataNode.bat";
        } else {
            stop[0] = testScriptPath + "/execTestStopDataNode.sh";

            start[0] = testScriptPath + "/execTestDataNode.sh";
        }


        String killCmd = "kill -9 ";
        if (osName == null || osName.equals("") || osName.trim().equals("windows")) {
            killCmd = "taskkill /F /PID ";
        }
        String stopCmd = "";
        String startCmd = "";

        try {

            OkuyamaClient client = new OkuyamaClient();
            client.connect("127.0.0.1", 8888);
            for (int idx = 0; idx < keyPrefixList.length; idx++) {
                for (int i = 0; i < 1000; i++) {
                    if(!client.setValue("RebootTestKey_" + keyPrefixList[idx] + "_" + i, "RebootTestValue_" + keyPrefixList[idx] + "_" + i)) {
                        System.out.println("Reboot test set error Key=" + "RebootTestKey_" + keyPrefixList[idx] + "_" + i);
                    }
                }
            }

            int exec = 0;

            stopCmd = stop[exec];
            startCmd = start[exec];
            System.out.println(stopCmd);
            Runtime rt = Runtime.getRuntime();
            Process p = rt.exec(stopCmd);

            InputStream is = p.getInputStream();    
            InputStreamReader isr = new InputStreamReader(is);
            BufferedReader br = new BufferedReader(isr);
            while ((result = br.readLine()) != null) {
                System.out.println(result);
                result = result.trim();
                if (!result.equals("")) {
                    try {
                        int pid = Integer.parseInt(result);
                        killCmd = killCmd + pid;
                    } catch (Exception ee) {}
                }
            }

            rt = Runtime.getRuntime();
            System.out.println(killCmd);
            p = rt.exec(killCmd);
            Thread.sleep(10000);

            rt = Runtime.getRuntime();
            System.out.println(startCmd);
            p = rt.exec(startCmd);

            Thread.sleep(nodeRebootWaitTime);

            for (int idx = 0; idx < keyPrefixList.length; idx++) {
                for (int i = 0; i < 1000; i++) {
                    String[] getRet = client.getValue("RebootTestKey_" + keyPrefixList[idx] + "_" + i);
                    if (!getRet[0].equals("true")) {
                        System.out.println(new Date().toString() + " Reboot test get error type=" + getRet[0] + " Key=" + "RebootTestKey_" + keyPrefixList[idx] + "_" + i);
                    } else {
                        if (!getRet[1].equals("RebootTestValue_" + keyPrefixList[idx] + "_" + i)) {
                            System.out.println(new Date().toString() + " Reboot test get error type=" + getRet[0] + " Key=" + "RebootTestKey_" + keyPrefixList[idx] + "_" + i + " Value=" + getRet[1]);
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw e;
        }
        System.out.println("execStop - End");
    }
}