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
import okuyama.imdst.util.JavaSystemApi;
import okuyama.imdst.util.SystemUtil;
import okuyama.imdst.util.DataDispatcher;
import okuyama.imdst.util.StatusUtil;
import okuyama.imdst.util.io.NodeDnsUtil;


/**
 * サーバの管理コマンドを受付て実行するHelper.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class ServerControllerHelper extends AbstractMasterManagerHelper {

    private String statusCommandBindIp = null;

    private int statusCommandPortNo = 8881;

    private ServerSocket svSoc = null;

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

            InetSocketAddress bindAddress  = null;
            if (this.statusCommandBindIp != null) {

                bindAddress = new InetSocketAddress(this.statusCommandBindIp, this.statusCommandPortNo);
            } else {
                bindAddress = new InetSocketAddress(this.statusCommandPortNo);
            }
            svSoc = new ServerSocket();
            svSoc.bind(bindAddress);

            while (true) {

                Socket soc = svSoc.accept();
                try {
                    BufferedReader br = new BufferedReader(new InputStreamReader(soc.getInputStream(), "UTF-8"));
                    PrintWriter pw = new PrintWriter(new BufferedWriter(new OutputStreamWriter(soc.getOutputStream() , "UTF-8")));

                    String command = br.readLine();
                    if (command.equals("shutdown")) {

                        pw.println("Commond Success");
                        pw.flush();
                        pw.println("Shutdown ...");
                        pw.flush();
                        Thread.sleep(500);
                        soc.close();
                        JavaMain.shutdownMainProcess();
                        break;
                    } else if (command.equals("debug")) {

                        StatusUtil.setDebugOption(true);
                        pw.println(command + " Success");
                        pw.flush();

                        Thread.sleep(500);

                        br.close();
                        pw.close();
                        soc.close();
                    } else if (command.equals("nodebug")) {

                        StatusUtil.setDebugOption(false);
                        pw.println(command + " Success");
                        pw.flush();

                        br.close();
                        pw.close();
                        Thread.sleep(500);
                        soc.close();
                    } else if (command.equals("cname")) {

                        String nameLine = br.readLine();

                        String[] nameInfo = nameLine.split("=");

                        if (nameInfo.length != 2) {

                            pw.println(nameLine + " Error");
                        } else {
                            NodeDnsUtil.setNameMap(nameInfo[0], nameInfo[1]);
                            pw.println(command + " Success Setting.. [" + nameInfo[0] + "] to [" + nameInfo[1] + "]");
                        }

                        pw.flush();
                        br.close();
                        pw.close();
                        Thread.sleep(500);
                        soc.close();
                    } else if (command.equals("rname")) {

                        String name = br.readLine();

                        NodeDnsUtil.removeNameMap(name);
                        pw.println(command + " Success Remove Setting.. [" + name + "]");

                        pw.flush();
                        br.close();
                        pw.close();
                        Thread.sleep(500);
                        soc.close();
                    } else if (command.equals("jobs")) {

                        pw.println(command + " Success");
                        pw.println(StatusUtil.getMethodExecuteCount());
                        pw.flush();

                        br.close();
                        pw.close();
                        Thread.sleep(500);
                        soc.close();

                    } else if (command.equals("allsize")) {


                        pw.println(command + " Success");

                        pw.println(((Map)StatusUtil.getNodeDataSize()).toString());
                        pw.flush();

                        br.close();
                        pw.close();
                        Thread.sleep(500);
                        soc.close();
                    } else if (command.equals("size")) {

                        String name = br.readLine();
                        pw.println(command + " Success");
                        Map sizeMap = StatusUtil.getNodeDataSize();
                        String size = "";
                        if (sizeMap != null && sizeMap.containsKey(name)) {
                            size = ((Long)sizeMap.get(name)).toString();
                        } else {
                            size = name + " NOT_FOUND";
                        }

                        pw.println(size);
                        pw.flush();

                        br.close();
                        pw.close();
                        Thread.sleep(500);
                        soc.close();
                    } else if (command.equals("fullgc")) {

                        pw.println(command + " Success");
                        JavaSystemApi.manualGc();
                        
                        pw.println("Execute GC");
                        pw.flush();

                        br.close();
                        pw.close();
                        Thread.sleep(500);
                        soc.close();
                    } else if (command.equals("netdebug")) {
                        
                        pw.println(command + " Success");
                        pw.println("");
                        pw.println("Please transmit changing line to end debugging"); 
                        pw.println("Net Debug Start ...");                        
                        pw.flush();
                        Thread.sleep(3000);

                        SystemUtil.netDebugPrinter = pw;
                        StatusUtil.setDebugOption(true);
                        try {
                            br.readLine(); 
                        } catch(Exception e){}
                        
                        StatusUtil.setDebugOption(false);
                        SystemUtil.netDebugPrinter = null;

                        br.close();
                        pw.close();
                        soc.close();
                        soc.close();
                    } else if (command.equals("maptime")) {

                        ImdstDefine.fileBaseMapTimeDebug = !ImdstDefine.fileBaseMapTimeDebug;
                        pw.println(command + " Success");
                        pw.println("MapTime = " + ImdstDefine.fileBaseMapTimeDebug);
                        pw.flush();
                        br.close();
                        pw.close();
                        Thread.sleep(500);
                        soc.close();
                    } else if (command.equals("jobstatus")) {
                    
                        Map nowStatus = StatusUtil.getNowExecuteMethodNoMap();
                        String statusStr = nowStatus.toString();
                        pw.println(command + " Success");
                        pw.println("Method status = " + statusStr);
                        pw.flush();
                        br.close();
                        pw.close();
                        Thread.sleep(500);
                        soc.close();
                    } else if (command.equals("-help")) {
                        pw.println(command + " Success");
                        pw.println("");
                        pw.println("shutdown");
                        pw.println("debug");
                        pw.println("nodebug");
                        pw.println("jobs");
                        pw.println("size");
                        pw.println("allsize");
                        pw.println("cname");
                        pw.println("rname");
                        pw.println("fullgc");
                        pw.println("netdebug");
                        pw.println("jobstatus");
                        pw.println("");

                        pw.flush();
                        
                        br.close();
                        pw.close();
                        soc.close();
                    } else {

                        pw.println(command + " Command Not Found");
                        pw.flush();

                        br.close();
                        pw.close();
                        Thread.sleep(500);
                        soc.close();
                    }
                } catch(Exception innerE) {
                    logger.info("ServerControllerHelper - executeHelper - Inner-Error", innerE);
                    innerE.printStackTrace();
                    try {
                        if(soc != null) soc.close();
                    } catch(Exception e2){}
                }
            }
        } catch(Exception e) {
            e.printStackTrace();
            logger.error("ServerControllerHelper - executeHelper - Error", e);
            throw new BatchException(e);
        } finally {
            try {
                if (svSoc != null) svSoc.close();
            } catch (Exception e) {}
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