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
 * DataNodeの停止、起動のテストを実行.<br>
 * このテストケースはWindows環境にCygwinをインストールし、PATHを設定した想定です.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class NodeStatusChangeJob extends AbstractJob implements IJob {

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

        try{
            // パス初期化
            if(optionParam != null && !optionParam.equals("")) {
                testScriptPath = optionParam;
            }

            Thread.sleep(30000);

            for (int t = 0; t < this.execCount; t++) {
                execRestart();
                Thread.sleep(1200000);
            }


        } catch(Exception e) {
            throw new BatchException(e);
        }

        return ret;
    }


    private void execRestart() throws Exception {
        System.out.println("execStop - Start");
        String result = null;

        String[] stop = new String[3];
        stop[0] = testScriptPath + "\\execTestStopDataNode.bat";
        stop[1] = testScriptPath + "\\execTestStopSlaveDataNode.bat";
        stop[2] = testScriptPath + "\\execTestStopThirdDataNode.bat";

        String[] start = new String[3];
        start[0] = testScriptPath + "\\execTestDataNode.bat";
        start[1] = testScriptPath + "\\execTestSlaveDataNode.bat";
        start[2] = testScriptPath + "\\execTestThirdDataNode.bat";

        String killCmd = "taskkill /F /PID ";
        String stopCmd = "";
        String startCmd = "";

        try {
            Random rnd = new Random();
            int exec = rnd.nextInt(3);

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
            Thread.sleep(30000);

            rt = Runtime.getRuntime();
            System.out.println(startCmd);
            p = rt.exec(startCmd);
        } catch (Exception e) {
            throw e;
        }
        System.out.println("execStop - End");
    }
}