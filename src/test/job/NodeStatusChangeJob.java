package test.job;

import java.io.*;
import java.net.*;
import java.util.*;

import org.batch.lang.BatchException;
import org.batch.job.AbstractJob;
import org.batch.job.AbstractHelper;
import org.batch.job.IJob;
import org.batch.util.ILogger;
import org.batch.util.LoggerFactory;
import org.imdst.util.KeyMapManager;
import org.imdst.util.StatusUtil;
import org.imdst.util.JavaSystemApi;
import org.imdst.client.*;

/**
 * 登録、取得、Tag登録、Tag取得、削除、Script実行、一意登録、のテストを実行
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class NodeStatusChangeJob extends AbstractJob implements IJob {

    private int execCount = 10;

    // 初期化メソッド定義
    public void initJob(String initValue) {
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


            Thread.sleep(20000);

            for (int t = 0; t < this.execCount; t++) {
                execRestart();
                Thread.sleep(120000);
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
        stop[0] = "C:\\desktop\\tools\\java\\okuyama\\trunk\\execTestStopDataNode.bat";
        stop[1] = "C:\\desktop\\tools\\java\\okuyama\\trunk\\execTestStopSlaveDataNode.bat";
        stop[2] = "C:\\desktop\\tools\\java\\okuyama\\trunk\\execTestStopThirdDataNode.bat";

        String[] start = new String[3];
        start[0] = "C:\\desktop\\tools\\java\\okuyama\\trunk\\execTestDataNode.bat";
        start[1] = "C:\\desktop\\tools\\java\\okuyama\\trunk\\execTestSlaveDataNode.bat";
        start[2] = "C:\\desktop\\tools\\java\\okuyama\\trunk\\execTestThirdDataNode.bat";

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
            Thread.sleep(20000);

            rt = Runtime.getRuntime();
System.out.println(startCmd);
            p = rt.exec(startCmd);
        } catch (Exception e) {
            throw e;
        }
        System.out.println("execStop - End");
    }
}