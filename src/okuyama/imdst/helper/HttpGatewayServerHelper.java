package okuyama.imdst.helper;

import java.io.*;
import java.util.*;
import java.net.*;
import javax.script.*;

import okuyama.base.lang.BatchException;
import okuyama.base.job.AbstractHelper;
import okuyama.base.job.IJob;
import okuyama.base.util.ILogger;
import okuyama.base.util.LoggerFactory;
import okuyama.imdst.util.KeyMapManager;
import okuyama.imdst.util.ImdstDefine;
import okuyama.imdst.util.StatusUtil;
import okuyama.imdst.util.protocol.*;
import okuyama.imdst.client.ImdstKeyValueClient;


/**
 * Http通信のGatewayサーバ.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class HttpGatewayServerHelper extends AbstractHelper {

    /**
     * Logger.<br>
     */
    private static ILogger logger = LoggerFactory.createLogger(HttpGatewayServerHelper.class);

    // 初期化メソッド定義
    public void initHelper(String initValue) {
    }

    // Jobメイン処理定義
    public String executeHelper(String optionParam) throws BatchException {
        //logger.debug("HttpGatewayServerHelper - executeHelper - start");

        String retString = null;
        Socket socket = null;
        ImdstKeyValueClient imdstKeyValueClient = null;
        BufferedWriter outStr = null;
        BufferedOutputStream bos = null;
        BufferedReader in = null;

        Object[] parameters = super.getParameters();

        try{
            imdstKeyValueClient = new ImdstKeyValueClient();
            imdstKeyValueClient.setConnectionInfos((String[])parameters[1]);
            imdstKeyValueClient.autoConnect();
            socket = (Socket)parameters[0];

            // 入力ストリームを取得
            outStr = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));  
            bos = new BufferedOutputStream(socket.getOutputStream());
      
            // 入力ストリームを取得
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));


            // 読み込んだ行をそのまま出力ストリームに書き出す
            String inputLine;
            int counter = 0;
            HashMap headerMap = new HashMap();

            String addSep = "";
            String body = null;
            int headerCounter = 0;
            while ((inputLine = in.readLine()) != null) {
                StringBuilder dataBuf = new StringBuilder(1024);
                if (inputLine.trim().equals("")) {
                    counter++;
                    if(counter == 1) {
                        // ここからはBody読みこみ
                        if (headerMap.get("content-length") != null) {

                            int contentSize = Integer.parseInt(((String)headerMap.get("content-length")).trim());
                            byte[] datas = new byte[contentSize];
                            for (int tt = 0; tt < contentSize; tt++) {
                                datas[tt] = new Integer(in.read()).byteValue();
                            }
                            body = new String(datas,"utf-8");

                        }
                        break;
                    }
                }

                if (headerCounter != 0) {
                    String[] parts = inputLine.trim().split(":");
                    addSep = "";
                    for (int i = 1; i < parts.length;i++) {

                        dataBuf.append(addSep);
                        dataBuf.append(parts[i]);
                        addSep = ":";
                    }
                    
                    headerMap.put(parts[0].trim().toLowerCase(), dataBuf.toString());
                } else {
                    String[] parts = inputLine.trim().split(" ");
                    addSep = "";
                    for (int i = 1; i < parts.length;i++) {

                        dataBuf.append(addSep);
                        dataBuf.append(parts[i]);
                        addSep = " ";
                    }
                    
                    headerMap.put(parts[0].trim().toLowerCase(), dataBuf.toString().trim());
                }
                headerCounter++;
            }

            // GETか確認
            if (headerMap.get("get") != null) {
                // Get
                Object[] ret = null;

                //imdstKeyValueClient.setCompressMode(true);
                String key  = ((String[])((String)headerMap.get("get")).split(" "))[0].trim();
                String[] types = key.split("\\.");
                System.out.println(key);
                ret = imdstKeyValueClient.getByteValue(key);
                if (ret[0].equals("true")) {
                    // データ有り
                    byte[] fileByte = null;
                    fileByte = (byte[])ret[1];
                    outStr.write("HTTP/1.1 200 OK\r\n");
                    outStr.write("Date: " + new Date().toString() + "\r\n");
                    outStr.write("Server: Server: okuyama kvs web server\r\n");
                    outStr.write("Last-Modified: " + new Date().toString() + "\r\n");
                    outStr.write("ETag: None\r\n");
                    outStr.write("Accept-Ranges: bytes\r\n");
                    outStr.write("Content-Length: " + fileByte.length + "\r\n");
                    outStr.write("Keep-Alive: timeout=5, max=100\r\n");
                    outStr.write("Connection: Keep-Alive\r\n");

                    if(types[types.length - 1].equals("gif")) {
                        outStr.write("Content-Type: image/gif\r\n");
                    } else  if(types[types.length - 1].equals("html")) {
                        outStr.write("Content-Type: text/html\r\n");
                    } else {
                        outStr.write("Content-Type: text/plain\r\n");
                    }
                    outStr.write("\r\n");
                    outStr.flush();
                    bos.write(fileByte);
                    bos.flush();
                    /*for (int idx = 0; idx < fileByte.length; idx++) {
                        outStr.write(fileByte[idx]);
                    }
                    outStr.flush();*/
                    //outStr.write("\r\n");
                } else if (ret[0].equals("false")) {
                    outStr.write("HTTP/1.1 404 Not Found\r\n");
                    outStr.write("Date: " + new Date().toString() + "\r\n");
                    outStr.write("Server: okuyama kvs web server\r\n");
                    outStr.write("Content-Length: " + "<!DOCTYPE HTML PUBLIC \"-//IETF//DTD HTML 2.0//EN\"><html><head><title>404 Not Found</title></head><body><h1>Not Found</h1><p>Not Found.</p></body></html>".length() + "\r\n");
                    outStr.write("Keep-Alive: timeout=5, max=100\r\n");
                    outStr.write("Connection: Keep-Alive\r\n");
                    outStr.write("Content-Type: text/html; charset=iso-8859-1\r\n");
                    outStr.write("\r\n");
                    outStr.write("<!DOCTYPE HTML PUBLIC \"-//IETF//DTD HTML 2.0//EN\"><html><head><title>404 Not Found</title></head><body><h1>Not Found</h1><p>Not Found.</p></body></html>\r\n");
                    outStr.write("\r\n");
                } else if (ret[0].equals("error")) {

                    outStr.write("HTTP/1.1 500 Internal Server Error\r\n");
                    outStr.write("Date: " + new Date().toString() + "\r\n");
                    outStr.write("Server: okuyama kvs web server\r\n");
                    outStr.write("Content-Length: " + "<!DOCTYPE HTML PUBLIC \"-//IETF//DTD HTML 2.0//EN\"><html><head><title>500 Internal Server Error</title></head><body><h1>Error</h1><p>Error.</p></body></html>".length() + "\r\n");
                    outStr.write("Keep-Alive: timeout=5, max=100\r\n");
                    outStr.write("Connection: Keep-Alive\r\n");
                    outStr.write("Content-Type: text/html; charset=iso-8859-1\r\n");
                    outStr.write("\r\n");
                    outStr.write("<!DOCTYPE HTML PUBLIC \"-//IETF//DTD HTML 2.0//EN\"><html><head><title>500 Internal Server Error</title></head><body><h1>Error</h1><p>Error.</p></body></html>\r\n");
                    outStr.write("\r\n");
                }
            } else {
                // Getではない

                outStr.write("HTTP/1.1 501 Not Implemented\r\n");
                outStr.write("Date: " + new Date().toString() + "\r\n");
                outStr.write("Server: okuyama kvs web server\r\n");
                outStr.write("Content-Length: " + "<!DOCTYPE HTML PUBLIC \"-//IETF//DTD HTML 2.0//EN\"><html><head><title>501 Not Implemented</title></head><body><h1>Request Type Error</h1><p>The Request Type GET Only.</p></body></html>".length() + "\r\n");
                outStr.write("Keep-Alive: timeout=5, max=100\r\n");
                outStr.write("Connection: Keep-Alive\r\n");
                outStr.write("Content-Type: text/html; charset=iso-8859-1\r\n");
                outStr.write("\r\n");
                outStr.write("<!DOCTYPE HTML PUBLIC \"-//IETF//DTD HTML 2.0//EN\"><html><head><title>501 Not Implemented</title></head><body><h1>Request Type Error</h1><p>The Request Type GET Only.</p></body></html>\r\n");
                outStr.write("\r\n");
            }

            outStr.flush();
  
            retString = super.SUCCESS;
        } catch(Exception e) {

            logger.error("HttpGatewayServerHelper - executeHelper - Error", e);
            e.printStackTrace();
            retString = super.ERROR;
            throw new BatchException(e);
        } finally {

            try {
                if (socket != null) {
                    socket.close();
                    socket = null;
                }

                if (imdstKeyValueClient != null) {
                    imdstKeyValueClient.close();
                    imdstKeyValueClient = null;
                }

            } catch(Exception e2) {
                logger.error("HttpGatewayServerHelper - executeHelper - Error2", e2);
                retString = super.ERROR;
                throw new BatchException(e2);
            }
        }

        //logger.debug("HttpGatewayServerHelper - executeHelper - end");
        return retString;
    }

    /**
     * 終了メソッド定義
     */
    public void endHelper() {
        //logger.debug("HttpGatewayServerHelper - endHelper - start");
        //logger.debug("HttpGatewayServerHelper - endHelper - end");
    }
}