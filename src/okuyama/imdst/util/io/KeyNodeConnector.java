package okuyama.imdst.util.io;

import java.io.*;
import java.net.*;
import java.util.*;

import okuyama.imdst.util.*;

public class KeyNodeConnector {

    private String nodeName = null;

    private int nodePort = -1;

    private String nodeFullName = null;

    private Socket socket = null;
    private PrintWriter pw = null;
    private BufferedReader br = null;
    private Long connectTime = null;

    private boolean poolConnect = false;

    private boolean retryConnectMode = true;

    private boolean retry = false;

    private static boolean recoverMode = false;
    private static String recoverTarget = "";

    public KeyNodeConnector(String nodeName, int nodePort, String nodeFullName) throws Exception {

        this.nodeName = nodeName;
        this.nodePort = nodePort;
        this.nodeFullName = nodeFullName;
    }


    public static void setRecoverMode(boolean mode, String target) {
        recoverMode = mode;
        if (mode) {
            recoverTarget = target;
        } else {
            recoverTarget = "";
        }
    }


    /**
     * Connect処理.<br>
     *
     * throws Exception
     */
    public void connect() throws Exception {
        connect(ImdstDefine.nodeConnectionOpenTimeout);
    }


    /**
     * Connect処理.<br>
     *
     * @param connectOpenTime
     * throws Exception
     */
    public void connect(int connectOpenTime) throws Exception {
        InetSocketAddress inetAddr = null;

        try {
            inetAddr = new InetSocketAddress(this.nodeName, this.nodePort);
            this.socket = new Socket();
            this.socket.connect(inetAddr, connectOpenTime);

            this.connectTime = new Long(JavaSystemApi.currentTimeMillis);

            // リカバー対象へのコネクションはタイムアウト時間を長くする
            if (recoverMode && recoverTarget.equals(this.nodeFullName)) {
                this.socket.setSoTimeout(ImdstDefine.nodeConnectionTimeout4RecoverMode);
            } else {
                this.socket.setSoTimeout(ImdstDefine.nodeConnectionTimeout);
            }

            this.pw = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream() , ImdstDefine.keyHelperClientParamEncoding)));

            this.br = new BufferedReader(new InputStreamReader(socket.getInputStream(), ImdstDefine.keyHelperClientParamEncoding));
        } catch (Exception e) {
            throw e;
        }
    }

    public String readLine() throws Exception {
        return this.readLine(null, false);
    }

    public String readLineWithReady() throws Exception {
        return this.readLine(null, true);
    }


    public String readLine(String retryStr) throws Exception {
        return this.readLine(retryStr, false);
    }

    public String readLineWithReady(String retryStr) throws Exception {
        return this.readLine(retryStr, true);
    }


    private String readLine(String retryStr, boolean isReady) throws Exception {

        String ret = null;
        try {

            if (isReady) {
                if(!this.br.ready()) Thread.sleep(500);
            }

            ret = this.br.readLine();
            if (ret == null) throw new IOException("readLine Ret = null");
            retry = false;
        } catch (Exception e) {

            if (this.retryConnectMode == true && this.retry == false) {

                this.retry = true;
                try {
                    if (this.socket != null && this.socket.isClosed() != true) {

                        this.br.close();
                        this.br = null;
                        this.pw.close();
                        this.pw = null;
                        this.socket.close();
                        this.socket = null;
                    }

                    try {

                        this.connect();
                    } catch (SocketTimeoutException ste) {

                        // 再リトライ
                        try {

                            this.connect();
                        } catch (SocketTimeoutException ste2) {

                            throw ste2;
                        }
                    }

                    // リトライフラグが有効でかつ、送信文字が指定されている場合は再送後、取得
                    if (retryStr != null) {

                        this.println(retryStr);
                        this.flush();
                    }

                    ret = this.readLine();
                } catch(Exception ee) {
                    throw e;
                }
            } else {
                throw e;
            }
        }
        return ret;
    }


    public void print(String str) throws Exception {
        try {

            this.pw.print(str);
            this.retry = false;
        } catch (Exception e) {
            throw e;
        }
    }

    public void println(String str) throws Exception {
        try {

            this.pw.println(str);
            this.retry = false;
        } catch (Exception e) {

            if (this.retryConnectMode == true && this.retry == false) {
                this.retry = true;
                try {
                    if (this.socket != null && this.socket.isClosed() != true) socket.close();
                    this.connect();
                    this.println(str);
                } catch(Exception ee) {
                    throw e;
                }
            } else {
                throw e;
            }
        }
    }

    public void flush() throws Exception {
        try {

            this.pw.flush();
            this.retry = false;
        } catch (Exception e) {
            throw e;
        }
    }

    public void setSoTimeout(int time) throws Exception {
        try {

            this.socket.setSoTimeout(time);
        } catch (Exception e) {
            throw e;
        }
    }


    public Long getConnetTime() {
        return this.connectTime;
    }

    public void setPoolConnectStatus(boolean status) {
        this.poolConnect = status;
    }

    public boolean getPoolConnectStatus() {
        return this.poolConnect;
    }

    public void initRetryFlg() {
        this.retry = false;
    }

    public void setRetryConnectMode(boolean mode) {
        this.retryConnectMode = mode;
    }


    public String getNodeFullName() {
        return this.nodeFullName;
    }

    public String getNodeName() {
        return this.nodeName;
    }

    public int getNodePort() {
        return this.nodePort;
    }

    public void close() {

        try {
            if (pw != null) {
                pw.println(ImdstDefine.imdstConnectExitRequest);
                pw.flush();
                pw.close();
                pw = null;
            }
        } catch (Exception e) {
            // 無視
        }

        try {
            if (br != null) {
                br.close();
                pw = null;
            }
        } catch (Exception e2) {
            // 無視
        }

        try {
            if (socket != null) {
                socket.close();
                socket = null;
            }
        } catch (Exception e3) {
            // 無視
        }
    }


    public String connectorDump() {
        StringBuilder dump = new StringBuilder(1024);
        dump.append(nodeName);
        dump.append(" / ");
        dump.append(nodePort);
        dump.append(" / ");
        dump.append(nodeFullName);
        dump.append(" / ");
        dump.append(socket);
        dump.append(" / ");
        dump.append(pw);
        dump.append(" / ");
        dump.append(br);
        dump.append(" / ");
        dump.append(connectTime);
        dump.append(" / ");
        dump.append(poolConnect);
        dump.append(" / ");
        dump.append(retryConnectMode);
        dump.append(" / ");
        dump.append(retry);
        dump.append(" / ");
        return dump.toString();
    }
}
