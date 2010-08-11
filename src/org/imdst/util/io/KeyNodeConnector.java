package org.imdst.util.io;

import java.io.*;
import java.net.*;
import java.util.*;

import org.imdst.util.*;

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

    public KeyNodeConnector(String nodeName, int nodePort, String nodeFullName) throws Exception {

        this.nodeName = nodeName;
        this.nodePort = nodePort;
        this.nodeFullName = nodeFullName;
        try {
            this.connect();
        } catch (Exception e) {
            throw e;
        }
    }

    public void connect() throws Exception {
        InetSocketAddress inetAddr = null;

        try {
            inetAddr = new InetSocketAddress(this.nodeName, this.nodePort);
            this.socket = new Socket();
            this.socket.connect(inetAddr, ImdstDefine.nodeConnectionOpenTimeout);

            this.connectTime = new Long(System.currentTimeMillis());
            this.socket.setSoTimeout(ImdstDefine.nodeConnectionTimeout);

            this.pw = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream() , ImdstDefine.keyHelperClientParamEncoding)));

            this.br = new BufferedReader(new InputStreamReader(socket.getInputStream(), ImdstDefine.keyHelperClientParamEncoding));
        } catch (Exception e) {
            throw e;
        }
    }

    public String readLine() throws Exception {
        String ret = null;
        try {

            ret = this.br.readLine();
            retry = false;
        } catch (Exception e) {
            if (e instanceof SocketException || e instanceof IOException) {
                if (this.retryConnectMode == true && this.retry == false) {
                    this.retry = true;
                    try {
                        if (this.socket != null) socket.close();
                        this.connect();
                        ret = this.readLine();
                    } catch(Exception ee) {
                        throw e;
                    }
                } else {
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
            if (e instanceof SocketException || e instanceof IOException) {
                if (this.retryConnectMode == true && this.retry == false) {
                    this.retry = true;
                    try {
                        if (this.socket != null) socket.close();
                        this.connect();
                        this.println(str);
                    } catch(Exception ee) {
                        throw e;
                    }
                } else {
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
}
