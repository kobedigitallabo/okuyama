package okuyama.imdst.helper;

import java.io.*;
import java.util.*;
import java.net.*;

import okuyama.base.JavaMain;
import okuyama.base.lang.BatchException;
import okuyama.base.job.AbstractHelper;
import okuyama.base.job.IJob;
import okuyama.base.util.ILogger;
import okuyama.base.util.LoggerFactory;
import okuyama.imdst.util.KeyMapManager;
import okuyama.imdst.util.ImdstDefine;
import okuyama.imdst.util.DataDispatcher;
import okuyama.imdst.util.StatusUtil;

/**
 * <br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class ServerControllerHelper extends AbstractMasterManagerHelper {

    private String statusCommandBindIp = "127.0.0.1";

    private int statusCommandPortNo = 8881;


    /**
     * Logger.<br>
     */
    private static ILogger logger = LoggerFactory.createLogger(ServerControllerHelper.class);


    // 初期化メソッド定義
    public void initHelper(String initValue) {
        if (initValue != null && !initValue.trim().equals("")) {

            if (initValue.indexOf(":") == -1) {
                this.statusCommandPortNo = Integer.parseInt(initValue);
            } else {
                String[] bindDt = initValue.split(":");
                this.statusCommandBindIp = bindDt[0];
                this.statusCommandPortNo = Integer.parseInt(bindDt[1]);
            }
        }
    }


    // Jobメイン処理定義
    public String executeHelper(String optionParam) throws BatchException {
        logger.debug("ServerControllerHelper - executeHelper - start");
        String ret = SUCCESS;
        try{
            // コントロール用のServerSocketを起動


            InetSocketAddress bindAddress= new InetSocketAddress(this.statusCommandBindIp, this.statusCommandPortNo);
            ServerSocket svSoc = new ServerSocket();
            svSoc.bind(bindAddress);

            while (true) {

                Socket soc = svSoc.accept();
                soc.close();
                break;

/*
                BufferedReader br = new BufferedReader(new InputStreamReader(soc.getInputStream(), "UTF-8"));
                PrintWriter pw = new PrintWriter(new BufferedWriter(new OutputStreamWriter(soc.getOutputStream() , "UTF-8")));

                String command = br.readLine();

                if (command.equals("shutdown")) {
                    pw.println("Shutdown processing beginning");
                    pw.flush();
                    pw.close();
                    br.close();
                    soc.close();

                    JavaMain.shutdownMainProccess();
                } else if (command.equals("status")) {

                    pw.println("Starting Server ...");
                    pw.flush();
                } else {
                    pw.println(command + " Command Not Found");
                    pw.flush();
                }
                pw.close();
                br.close();
                soc.close();
*/
            }
            JavaMain.shutdownMainProccess();
        } catch(Exception e) {
            e.printStackTrace();
            logger.error("ServerControllerHelper - executeHelper - Error", e);
            throw new BatchException(e);
        }

        logger.debug("ServerControllerHelper - executeHelper - end");
        return ret;
    }


    /**
     * Helper後処理.<br>
     *
     */
    public void endHelper() {
    }

}