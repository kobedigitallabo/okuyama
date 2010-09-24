package okuyama.imdst.client;

import java.util.*;
import java.io.*;
import java.net.*;
import java.util.zip.*;


import com.sun.mail.util.BASE64DecoderStream;
import com.sun.mail.util.BASE64EncoderStream;

import okuyama.imdst.util.ImdstDefine;

/**
 * MasterNodeと通信を行うプログラムインターフェース<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class ImdstKeyValueClient {

    // 接続先情報
    private ArrayList masterNodesList = null;
    // ソケット
    private Socket socket = null;

    // 現在接続中のMasterServer
    private String nowConnectServerInfo = "";

    // サーバへの出力用
    private PrintWriter pw = null;

    // サーバからの受信用
    private BufferedReader br = null;

    private String transactionCode = "0";

    // データセパレータ文字列
    private static final String sepStr = ImdstDefine.keyHelperClientParamSep;

    // 接続時のデフォルトのエンコーディング
    private static final String connectDefaultEncoding = ImdstDefine.keyHelperClientParamEncoding;

    // ブランク文字列の代行文字列
    private static final String blankStr = ImdstDefine.imdstBlankStrData;

    // 接続要求切断文字列
    private static final String connectExitStr = ImdstDefine.imdstConnectExitRequest;

    // Tagで取得出来るキー値のセパレータ文字列
    private static final String tagKeySep = ImdstDefine.imdstTagKeyAppendSep;

    private static final String byteDataKeysSep = ":#:";


    // バイナリデータ分割保存サイズ
    private int saveSize = ImdstDefine.saveDataMaxSize;


    // 保存できる最大長
    private int maxValueSize = ImdstDefine.saveDataMaxSize;

    // byteデータ送信時に圧縮を行うかを決定
    private boolean compressMode = false;

    /**
     * コンストラクタ
     *
     */
    public ImdstKeyValueClient() {
        // エンコーダ、デコーダの初期化に時間を使うようなので初期化
        this.dataEncoding("".getBytes());
        this.dataDecoding("".getBytes());
    }

    /**
     * 保存するデータの最大長を変更する.<br>
     *
     * @param size 保存サイズ(バイト長)
     */
    public void setSaveMaxDataSize(int size) {
        this.saveSize = size;
        this.maxValueSize = size;
    }


    /**
     * バイナリデータ分割保存サイズを変更<br>
     *
     * @param size サイズ
     */
    public void changeByteSaveSize(int size) {
        saveSize = size;
    }


    /**
     * データ保存時の圧縮指定.<br>
     *
     * @param flg true:圧縮 false:非圧縮
     */
    public void setCompressMode(boolean flg) {
        this.compressMode = flg;
    }


    /**
     * MasterNodeの接続情報を設定する.<br>
     * 本メソッドでセットし、autoConnect()メソッドを<br>
     * 呼び出すと、自動的にその時稼動しているMasterNodeにバランシングして<br>
     * 接続される。接続出来ない場合は、別のMasterNodeに再接続される.<br>
     *
     * @param masterNodes 接続情報の配列 "IP:PORT"の形式
     */
    public void setConnectionInfos(String[] masterNodes) {
        this.masterNodesList = new ArrayList(masterNodes.length);
        for (int i = 0; i < masterNodes.length; i++) {
            this.masterNodesList.add(masterNodes[i]);
        } 
    }

    /**
     * 設定されたMasterNodeの接続情報を元に自動的に接続を行う.<br>
     * 接続出来ない場合自動的に別ノードへ再接続を行う.<br>
     *
     * @param masterNodes 接続情報の配列 "IP:PORT"の形式
     */
    public void autoConnect() throws Exception {
        ArrayList tmpMasterNodeList = new ArrayList();
        ArrayList workList = (ArrayList)this.masterNodesList.clone();
        Random rnd = new Random();

        for (int idx = 0; idx < workList.size(); idx++) {
            if (!((String)workList.get(idx)).equals(this.nowConnectServerInfo)) {
                tmpMasterNodeList.add(workList.get(idx));
            }
        }

        while(tmpMasterNodeList.size() > 0) {

            int ran = rnd.nextInt(tmpMasterNodeList.size());
            try {
                try {
                    if (this.br != null) this.br.close();

                    if (this.pw != null) this.pw.close();

                    if (this.socket != null) this.socket.close();
                } catch (Exception e) {}

                String nodeStr = (String)tmpMasterNodeList.remove(ran);
                String[] nodeInfo = nodeStr.split(":");
                this.socket = new Socket();
                InetSocketAddress inetAddr = new InetSocketAddress(nodeInfo[0], Integer.parseInt(nodeInfo[1]));
                this.socket.connect(inetAddr, ImdstDefine.clientConnectionOpenTimeout);
                this.socket.setSoTimeout(ImdstDefine.clientConnectionTimeout);
                this.pw = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), ImdstKeyValueClient.connectDefaultEncoding)));
                this.br = new BufferedReader(new InputStreamReader(socket.getInputStream(), ImdstKeyValueClient.connectDefaultEncoding));
                this.initClient();
                this.nowConnectServerInfo = nodeStr;
                break;
            } catch (Exception e) {
                try {
                    if (this.br != null) {
                        this.br.close();
                        this.br = null;
                    }

                    if (this.pw != null) {
                        this.pw.close();
                        this.pw = null;
                    }

                    if (this.socket != null) {
                        this.socket.close();
                        this.socket = null;
                    }
                } catch (Exception e2) {
                    // 無視
                    this.socket = null;
                }
                if(tmpMasterNodeList.size() < 1) throw e;
            }
        }
    }


    /**
     * 設定されたMasterNodeの接続情報を元に自動的に接続を行う.<br>
     * 接続出来ない場合自動的に別ノードへ再接続を行う.<br>
     *
     * @param masterNodes 接続情報の配列 "IP:PORT"の形式
     */
    public void nextConnect() throws Exception {
        ArrayList tmpMasterNodeList = new ArrayList();
        tmpMasterNodeList = (ArrayList)this.masterNodesList.clone();

        while(tmpMasterNodeList.size() > 0) {

            try {
                try {
                    if (this.br != null) this.br.close();

                    if (this.pw != null) this.pw.close();

                    if (this.socket != null) this.socket.close();
                } catch (Exception e) {}

                String nodeStr = (String)tmpMasterNodeList.remove(0);
                String[] nodeInfo = nodeStr.split(":");
                this.socket = new Socket();
                InetSocketAddress inetAddr = new InetSocketAddress(nodeInfo[0], Integer.parseInt(nodeInfo[1]));
                this.socket.connect(inetAddr, ImdstDefine.clientConnectionOpenTimeout);
                this.socket.setSoTimeout(ImdstDefine.clientConnectionTimeout);
                this.pw = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), ImdstKeyValueClient.connectDefaultEncoding)));
                this.br = new BufferedReader(new InputStreamReader(socket.getInputStream(), ImdstKeyValueClient.connectDefaultEncoding));
                this.initClient();
                this.nowConnectServerInfo = nodeStr;
                break;
            } catch (Exception e) {
                try {
                    if (this.br != null) {
                        this.br.close();
                        this.br = null;
                    }

                    if (this.pw != null) {
                        this.pw.close();
                        this.pw = null;
                    }

                    if (this.socket != null) {
                        this.socket.close();
                        this.socket = null;
                    }
                } catch (Exception e2) {
                    // 無視
                    this.socket = null;
                }
                if(tmpMasterNodeList.size() < 1) throw e;
            }
        }
    }


    /**
     * 接続処理.<br>
     * エンコーディング指定なし.<br>
     *
     * @param server
     * @param port
     * @throws Exception
     */
    public void connect(String server, int port) throws Exception {
        this.connect(server, port, ImdstKeyValueClient.connectDefaultEncoding);
    }

    /**
     * 接続処理.<br>
     * エンコーディング指定有り.<br>
     *
     * @param server
     * @param port
     * @param encoding
     * @throws Exception
     */
    public void connect(String server, int port, String encoding) throws Exception {
        try {
            this.socket = new Socket();
            InetSocketAddress inetAddr = new InetSocketAddress(server, port);
            this.socket.connect(inetAddr, ImdstDefine.clientConnectionOpenTimeout);
            this.socket.setSoTimeout(ImdstDefine.clientConnectionTimeout);

            this.pw = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), encoding)));
            this.br = new BufferedReader(new InputStreamReader(socket.getInputStream(), encoding));

            this.initClient();
            this.nowConnectServerInfo = server + ":" + new Integer(port).toString();
        } catch (Exception e) {
            try {
                if (this.br != null) {
                    this.br.close();
                    this.br = null;
                }

                if (this.pw != null) {
                    this.pw.close();
                    this.pw = null;
                }

                if (this.socket != null) {
                    this.socket.close();
                    this.socket = null;
                }
            } catch (Exception e2) {
                // 無視
                this.socket = null;
            }
            throw e;
        }
    }


    /**
     * マスタサーバとの接続を切断.<br>
     *
     * @throw Exception
     */
    public void close() throws Exception {
        try {
            this.transactionCode = "0";
            if (this.pw != null) {
                // 接続切断を通知
                this.pw.println(connectExitStr);
                this.pw.flush();

                this.pw.close();
                this.pw = null;
            }

            if (this.br != null) {
                this.br.close();
                this.br = null;
            }

            if (this.socket != null) {
                this.socket.close();
                this.socket = null;
            }
        } catch (Exception e) {
            throw e;
        }
    }


    /**
     * Clientを初期化する.<br>
     * 今のところは最大保存サイズの初期化のみ<br>
     *
     * @return boolean true:開始成功 false:開始失敗
     * @throws Exception
     */
    public boolean initClient() throws Exception {
        boolean ret = false;
        String serverRetStr = null;
        String[] serverRet = null;

        StringBuffer serverRequestBuf = null;

        try {
            if (this.socket == null) throw new Exception("No ServerConnect!!");


            // 文字列バッファ初期化
            serverRequestBuf = new StringBuffer();


            // 処理番号連結
            serverRequestBuf.append("0");
            // セパレータ連結
            serverRequestBuf.append(ImdstKeyValueClient.sepStr);


            // サーバ送信
            pw.println(serverRequestBuf.toString());
            pw.flush();

            // サーバから結果受け取り
            serverRetStr = br.readLine();

            serverRet = serverRetStr.split(ImdstKeyValueClient.sepStr);

            // 処理の妥当性確認
            if (serverRet[0].equals("0")) {
                if (serverRet[1].equals("true")) {

                    this.saveSize = new Integer(serverRet[2]).intValue();
                    this.maxValueSize = this.saveSize;
                    ret = true;
                } else {
                    ret = false;
                }
            } else {

                // 妥当性違反
                throw new Exception("Execute Violation of validity");
            }

        } catch (ConnectException ce) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.initClient();
                } catch (Exception e) {
                    throw ce;
                }
            } else {
                throw ce;
            }
        } catch (SocketException se) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.initClient();
                } catch (Exception e) {
                    throw se;
                }
            } else {
                throw se;
            }
        } catch (Throwable e) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.initClient();
                } catch (Exception ee) {
                    throw new Exception(e);
                }
            } else {
                throw new Exception(e);
            }
        }
        return ret;
    }


    /**
     * Transactionを開始する.<br>
     * データロック、ロックリリースを使用する場合は、<br>
     * 事前に呼び出す必要がある<br>
     *
     * @return boolean true:開始成功 false:開始失敗
     * @throws Exception
     */
    public boolean startTransaction() throws Exception {
        boolean ret = false;
        String serverRetStr = null;
        String[] serverRet = null;

        StringBuffer serverRequestBuf = null;

        try {
            if (this.socket == null) throw new Exception("No ServerConnect!!");


            // 文字列バッファ初期化
            serverRequestBuf = new StringBuffer();


            // 処理番号連結
            serverRequestBuf.append("37");
            // セパレータ連結
            serverRequestBuf.append(ImdstKeyValueClient.sepStr);


            // サーバ送信
            pw.println(serverRequestBuf.toString());
            pw.flush();

            // サーバから結果受け取り
            serverRetStr = br.readLine();

            serverRet = serverRetStr.split(ImdstKeyValueClient.sepStr);

            // 処理の妥当性確認
            if (serverRet[0].equals("37")) {
                if (serverRet[1].equals("true")) {
                    this.transactionCode = serverRet[2];
                    ret = true;
                } else {
                    ret = false;
                }
            } else {

                // 妥当性違反
                throw new Exception("Execute Violation of validity");
            }

        } catch (ConnectException ce) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.startTransaction();
                } catch (Exception e) {
                    throw ce;
                }
            } else {
                throw ce;
            }
        } catch (SocketException se) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.startTransaction();
                } catch (Exception e) {
                    throw se;
                }
            } else {
                throw se;
            }
        } catch (Throwable e) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.startTransaction();
                } catch (Exception ee) {
                    throw new Exception(e);
                }
            } else {
                throw new Exception(e);
            }
        }
        return ret;
    }


    /**
     * Transactionを終了する.<br>
     * データロック、ロックリリースを使用を完了後に、<br>
     * 呼び出すことで、現在使用中のTransactionを終了できる<br>
     *
     * 
     * @throws Exception
     */
    public void endTransaction() {
        this.transactionCode = "0";
    }


    /**
     * データのLockを依頼する.<br>
     * 本メソッドは、startTransactionメソッドを呼び出した場合のみ有効である
     * 
     * @param keyStr
     * @param lockingTime Lockを取得後、維持する時間(この時間を経過すると自動的にLockが解除される)(単位は秒)(0は無制限)
     * @param waitLockTime Lockを取得する場合に既に取得中の場合この時間はLock取得をリトライする(単位は秒)(0は1度取得を試みる)
     * @return String[] 要素1(Lock成否):"true" or "false"
     * @throws Exception
     */
    public String[] lockData(String keyStr, int lockingTime, int waitLockTime) throws Exception {
        String[] ret = new String[1]; 
        String serverRetStr = null;
        String[] serverRet = null;

        StringBuffer serverRequestBuf = null;

        try {
            if (this.socket == null) throw new Exception("No ServerConnect!!");

            if (this.transactionCode == null || this.transactionCode.equals("") || this.transactionCode.equals("0")) 
                throw new Exception("No Start Transaction!!");

            // エラーチェック
            // Keyに対する無指定チェック
            if (keyStr == null ||  keyStr.equals("")) {
                throw new Exception("The blank is not admitted on a key");
            }

            // 文字列バッファ初期化
            serverRequestBuf = new StringBuffer();


            // 処理番号連結
            serverRequestBuf.append("30");
            // セパレータ連結
            serverRequestBuf.append(ImdstKeyValueClient.sepStr);

            // Key連結(Keyはデータ送信時には必ず文字列が必要)
            serverRequestBuf.append(new String(this.dataEncoding(keyStr.getBytes())));

            // セパレータ連結
            serverRequestBuf.append(ImdstKeyValueClient.sepStr);
            // TransactionCode連結
            serverRequestBuf.append(this.transactionCode);
            // セパレータ連結
            serverRequestBuf.append(ImdstKeyValueClient.sepStr);
            // lockingTime連結
            serverRequestBuf.append(new Integer(lockingTime).toString());
            // セパレータ連結
            serverRequestBuf.append(ImdstKeyValueClient.sepStr);
            // waitLockTime連結
            serverRequestBuf.append(new Integer(waitLockTime).toString());


            // サーバ送信
            pw.println(serverRequestBuf.toString());
            pw.flush();

            // サーバから結果受け取り
            serverRetStr = br.readLine();

            serverRet = serverRetStr.split(ImdstKeyValueClient.sepStr);

            // 処理の妥当性確認
            if (serverRet[0].equals("30")) {
                if (serverRet[1].equals("true")) {

                    // Lock成功
                    ret[0] = serverRet[1];
                    //ret[1] = serverRet[2];
                } else if(serverRet[1].equals("false")) {

                    // Lock失敗
                    ret[0] = serverRet[1];
                    //ret[1] = null;
                } else if(serverRet[1].equals("error")) {

                    // エラー発生
                    ret[0] = serverRet[1];
                    //ret[1] = serverRet[2];
                }
            } else {

                // 妥当性違反
                throw new Exception("Execute Violation of validity");
            }
        } catch (ConnectException ce) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.lockData(keyStr, lockingTime, waitLockTime);
                } catch (Exception e) {
                    throw ce;
                }
            } else {
                throw ce;
            }
        } catch (SocketException se) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.lockData(keyStr, lockingTime, waitLockTime);
                } catch (Exception e) {
                    throw se;
                }
            } else {
                throw se;
            }
        } catch (Throwable e) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.lockData(keyStr, lockingTime, waitLockTime);
                } catch (Exception ee) {
                    throw new Exception(e);
                }
            } else {
                throw new Exception(e);
            }
        }
        return ret;
    }


    /**
     * データのLock解除を依頼する.<br>
     * 本メソッドは、startTransactionメソッドを呼び出した場合のみ有効である
     *
     * @param keyStr
     * @return String[] 要素1(Lock解除成否):"true" or "false"
     * @throws Exception
     */
    public String[] releaseLockData(String keyStr) throws Exception {
        String[] ret = new String[1]; 
        String serverRetStr = null;
        String[] serverRet = null;

        StringBuffer serverRequestBuf = null;

        try {
            if (this.socket == null) throw new Exception("No ServerConnect!!");

            if (this.transactionCode == null || this.transactionCode.equals("") || this.transactionCode.equals("0")) 
                throw new Exception("No Start Transaction!!");

            // エラーチェック
            // Keyに対する無指定チェック
            if (keyStr == null ||  keyStr.equals("")) {
                throw new Exception("The blank is not admitted on a key");
            }

            // 文字列バッファ初期化
            serverRequestBuf = new StringBuffer();


            // 処理番号連結
            serverRequestBuf.append("31");
            // セパレータ連結
            serverRequestBuf.append(ImdstKeyValueClient.sepStr);

            // Key連結(Keyはデータ送信時には必ず文字列が必要)
            serverRequestBuf.append(new String(this.dataEncoding(keyStr.getBytes())));
            // セパレータ連結
            serverRequestBuf.append(ImdstKeyValueClient.sepStr);

            // TransactionCode連結
            serverRequestBuf.append(this.transactionCode);

            // サーバ送信
            pw.println(serverRequestBuf.toString());
            pw.flush();

            // サーバから結果受け取り
            serverRetStr = br.readLine();

            serverRet = serverRetStr.split(ImdstKeyValueClient.sepStr);

            // 処理の妥当性確認
            if (serverRet[0].equals("31")) {
                if (serverRet[1].equals("true")) {

                    // Lock成功
                    ret[0] = serverRet[1];
                    //ret[1] = serverRet[2];
                } else if(serverRet[1].equals("false")) {

                    // データなし
                    ret[0] = serverRet[1];
                    //ret[1] = null;
                } else if(serverRet[1].equals("error")) {

                    // エラー発生
                    ret[0] = serverRet[1];
                    //ret[1] = serverRet[2];
                }
            } else {

                // 妥当性違反
                throw new Exception("Execute Violation of validity");
            }
        } catch (ConnectException ce) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.releaseLockData(keyStr);
                } catch (Exception e) {
                    throw ce;
                }
            } else {
                throw ce;
            }
        } catch (SocketException se) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.releaseLockData(keyStr);
                } catch (Exception e) {
                    throw se;
                }
            } else {
                throw se;
            }
        } catch (Throwable e) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.releaseLockData(keyStr);
                } catch (Exception ee) {
                    throw new Exception(e);
                }
            } else {
                throw new Exception(e);
            }
        }
        return ret;
    }


    // トランザクションを開始している場合、自身のトランザクションを一意に表す
    // コードを返す.
    // このコードをsetNowTransactionCodeに渡すと、別クライアントのTransactionを引き継げる
    // !! 他クライアントの処理を横取ることが出来るため、使用を推奨しない !! 
    public String getNowTransactionCode() {
        return this.transactionCode;
    }


    // 他のクライアントが実施しているトランザクションコードを設定することで、
    // トランザクション処理を引き継ぐことが出来る。
    // !!! 他クライアントの処理を横取ることが出来るため、使用を推奨しない !!!
    public void setNowTransactionCode(String transactionCode) {
        this.transactionCode = transactionCode;
    }


    /**
     * MasterNodeの生死を確認する.<br>
     *
     * @return boolean true:開始成功 false:開始失敗
     * @throws Exception
     */
    public boolean arrivalMasterNode() throws Exception {
        boolean ret = false;
        String serverRetStr = null;
        String[] serverRet = null;

        StringBuffer serverRequestBuf = null;

        try {
            if (this.socket == null) throw new Exception("No ServerConnect!!");


            // 文字列バッファ初期化
            serverRequestBuf = new StringBuffer();


            // 処理番号連結
            serverRequestBuf.append("12");
            // セパレータ連結
            serverRequestBuf.append(ImdstKeyValueClient.sepStr);


            // サーバ送信
            pw.println(serverRequestBuf.toString());
            pw.flush();

            // サーバから結果受け取り
            serverRetStr = br.readLine();
            serverRet = serverRetStr.split(ImdstKeyValueClient.sepStr);

            // 処理の妥当性確認
            if (serverRet[0].equals("12")) {
                if (serverRet[1].equals("true")) {
                    ret = true;
                } else {
                    ret = false;
                }
            } else {

                    ret = false;
            }

        } catch (ConnectException ce) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.arrivalMasterNode();
                } catch (Exception e) {
                    throw ce;
                }
            } else {
                throw ce;
            }
        } catch (SocketException se) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.arrivalMasterNode();
                } catch (Exception e) {
                    throw se;
                }
            } else {
                throw se;
            }
        } catch (Throwable e) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.arrivalMasterNode();
                } catch (Exception ee) {
                    throw new Exception(e);
                }
            } else {
                throw new Exception(e);
            }
        }
        return ret;
    }


    /**
     * マスタサーバへデータを送信する.<br>
     * Tagなし.<br>
     *
     * @param keyStr
     * @param value
     * @return boolean
     * @throws Exception
     */
    public boolean setValue(String keyStr, String value) throws Exception {
        return this.setValue(keyStr, null, value);
    }

    /**
     * マスタサーバへデータを送信する.<br>
     * Tag有り.<br>
     *
     * @param keyStr
     * @param tagStrs
     * @param value
     * @return boolean
     * @throws Exception
     */
    public boolean setValue(String keyStr, String[] tagStrs, String value) throws Exception {
        boolean ret = false; 
        String serverRetStr = null;
        String[] serverRet = null;

        StringBuffer serverRequestBuf = null;
        try {
            // Byte Lenghtチェック
            if (keyStr.getBytes().length > maxValueSize) throw new Exception("Save Key Max Size " + maxValueSize + " Byte");
            if (tagStrs != null) {
                for (int i = 0; i < tagStrs.length; i++) {
                    if (tagStrs[i].getBytes().length > maxValueSize) throw new Exception("Tag Max Size " + maxValueSize + " Byte");
                }
            }
            if (value.getBytes().length > maxValueSize) throw new Exception("Save Value Max Size " + maxValueSize + " Byte");

            if (this.socket == null) throw new Exception("No ServerConnect!!");

            // エラーチェック
            // Keyに対する無指定チェック
            if (keyStr == null ||  keyStr.equals("")) {
                throw new Exception("The blank is not admitted on a key");
            }

            // valueに対する無指定チェック(Valueはnullやブランクの場合は代行文字列に置き換える)
            if (value == null ||  value.equals("")) {
                value = ImdstKeyValueClient.blankStr;
            } else {

                // ValueをBase64でエンコード
                value = new String(this.dataEncoding(value.getBytes()));


            }

            // 文字列バッファ初期化
            serverRequestBuf = new StringBuffer();


            // 処理番号連結
            serverRequestBuf.append("1");
            // セパレータ連結
            serverRequestBuf.append(ImdstKeyValueClient.sepStr);


            // Key連結(Keyはデータ送信時には必ず文字列が必要)
            serverRequestBuf.append(new String(this.dataEncoding(keyStr.getBytes())));
            // セパレータ連結
            serverRequestBuf.append(ImdstKeyValueClient.sepStr);


            // Tag連結
            // Tag指定の有無を調べる
            if (tagStrs == null || tagStrs.length < 1) {

                // ブランク規定文字列を連結
                serverRequestBuf.append(ImdstKeyValueClient.blankStr);
            } else {

                // Tag数分連結
                serverRequestBuf.append(new String(this.dataEncoding(tagStrs[0].getBytes())));
                for (int i = 1; i < tagStrs.length; i++) {
                    serverRequestBuf.append(tagKeySep);
                    serverRequestBuf.append(new String(this.dataEncoding(tagStrs[i].getBytes())));
                }
            }

            // セパレータ連結
            serverRequestBuf.append(ImdstKeyValueClient.sepStr);

            // TransactionCode連結
            serverRequestBuf.append(this.transactionCode);

            // セパレータ連結
            serverRequestBuf.append(ImdstKeyValueClient.sepStr);

            // Value連結
            serverRequestBuf.append(value);

            // サーバ送信
            pw.println(serverRequestBuf.toString());
            pw.flush();

            // サーバから結果受け取り
            serverRetStr = br.readLine();
            serverRet = serverRetStr.split(ImdstKeyValueClient.sepStr);


            // 処理の妥当性確認
            if (serverRet.length == 3 && serverRet[0].equals("1")) {
                if (serverRet[1].equals("true")) {

                    // 処理成功
                    ret = true;
                } else{

                    // 処理失敗(メッセージ格納)
                    throw new Exception(serverRet[2]);
                }
            } else {

                // 妥当性違反
                throw new Exception("Execute Violation of validity [" + serverRetStr + "]");
            }
        } catch (ConnectException ce) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.setValue(keyStr, tagStrs, value);
                } catch (Exception e) {
                    throw ce;
                }
            } else {
                throw ce;
            }
        } catch (SocketException se) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.setValue(keyStr, tagStrs, value);
                } catch (Exception e) {
                    throw se;
                }
            } else {
                throw se;
            }
        } catch (Throwable e) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.setValue(keyStr, tagStrs, value);
                } catch (Exception ee) {
                    throw new Exception(e);
                }
            } else {
                throw new Exception(e);
            }
        }
        return ret;
    }

    /**
     * マスタサーバへ新規データを送信する.<br>
     * Tagなし.<br>
     * 既にデータが同一のKeyで登録されている場合は失敗する.<br>
     * その場合は、falseが返る<br>
     * 成功の場合は配列の長さは1である。失敗時は2である<br>
     *
     * @param keyStr
     * @param value
     * @return String[] 要素1(データ有無):"true" or "false",要素2(失敗時はメッセージ):"メッセージ"
     * @throws Exception
     */
    public String[] setNewValue(String keyStr, String value) throws Exception {
        return this.setNewValue(keyStr, null, value);
    }


    /**
     * マスタサーバへ新規データを送信する.<br>
     * Tag有り.<br>
     * 既にデータが同一のKeyで登録されている場合は失敗する.<br>
     * その場合は、falseが返る<br>
     * 成功の場合は配列の長さは1である。失敗時は2である<br>
     * 
     * @param keyStr
     * @param tagStrs
     * @param value
     * @return String[] 要素1(データ有無):"true" or "false",要素2(失敗時はメッセージ):"メッセージ"
     * @throws Exception
     */
    public String[] setNewValue(String keyStr, String[] tagStrs, String value) throws Exception {
        String[] ret = null; 
        String serverRetStr = null;
        String[] serverRet = null;

        StringBuffer serverRequestBuf = null;
        try {
            // Byte Lenghtチェック
            if (keyStr.getBytes().length > maxValueSize) throw new Exception("Save Key Max Size " + maxValueSize + " Byte");
            if (tagStrs != null) {
                for (int i = 0; i < tagStrs.length; i++) {
                    if (tagStrs[i].getBytes().length > maxValueSize) throw new Exception("Tag Max Size " + maxValueSize + " Byte");
                }
            }
            if (value.getBytes().length > maxValueSize) throw new Exception("Save Value Max Size " + maxValueSize + " Byte");

            if (this.socket == null) throw new Exception("No ServerConnect!!");

            // エラーチェック
            // Keyに対する無指定チェック
            if (keyStr == null ||  keyStr.equals("")) {
                throw new Exception("The blank is not admitted on a key");
            }

            // valueに対する無指定チェック(Valueはnullやブランクの場合は代行文字列に置き換える)
            if (value == null ||  value.equals("")) {
                value = ImdstKeyValueClient.blankStr;
            } else {

                // ValueをBase64でエンコード
                value = new String(this.dataEncoding(value.getBytes()));


            }

            // 文字列バッファ初期化
            serverRequestBuf = new StringBuffer();


            // 処理番号連結
            serverRequestBuf.append("6");
            // セパレータ連結
            serverRequestBuf.append(ImdstKeyValueClient.sepStr);


            // Key連結(Keyはデータ送信時には必ず文字列が必要)
            serverRequestBuf.append(new String(this.dataEncoding(keyStr.getBytes())));
            // セパレータ連結
            serverRequestBuf.append(ImdstKeyValueClient.sepStr);


            // Tag連結
            // Tag指定の有無を調べる
            if (tagStrs == null || tagStrs.length < 1) {

                // ブランク規定文字列を連結
                serverRequestBuf.append(ImdstKeyValueClient.blankStr);
            } else {

                // Tag数分連結
                serverRequestBuf.append(new String(this.dataEncoding(tagStrs[0].getBytes())));
                for (int i = 1; i < tagStrs.length; i++) {
                    serverRequestBuf.append(tagKeySep);
                    serverRequestBuf.append(new String(this.dataEncoding(tagStrs[i].getBytes())));
                }
            }

            // セパレータ連結
            serverRequestBuf.append(ImdstKeyValueClient.sepStr);

            // TransactionCode連結
            serverRequestBuf.append(this.transactionCode);

            // セパレータ連結
            serverRequestBuf.append(ImdstKeyValueClient.sepStr);

            // Value連結
            serverRequestBuf.append(value);

            // サーバ送信
            pw.println(serverRequestBuf.toString());
            pw.flush();

            // サーバから結果受け取り
            serverRetStr = br.readLine();
            serverRet = serverRetStr.split(ImdstKeyValueClient.sepStr);

            // 処理の妥当性確認
            if (serverRet != null && serverRet[0].equals("6")) {
                if (serverRet[1].equals("true")) {

                    // 処理成功
                    ret = new String[1];
                    ret[0] = "true";
                } else{

                    // 処理失敗(メッセージ格納)
                    ret = new String[2];
                    ret[0] = "false";
                    ret[1] = serverRet[2];
                }
            } else {

                // 妥当性違反
                throw new Exception("Execute Violation of validity [" + serverRetStr + "]");
            }
        } catch (ConnectException ce) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.setNewValue(keyStr, tagStrs, value);
                } catch (Exception e) {
                    throw ce;
                }
            } else {
                throw ce;
            }
        } catch (SocketException se) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.setNewValue(keyStr, tagStrs, value);
                } catch (Exception e) {
                    throw se;
                }
            } else {
                throw se;
            }
        } catch (Throwable e) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.setNewValue(keyStr, tagStrs, value);
                } catch (Exception ee) {
                    throw new Exception(e);
                }
            } else {
                throw new Exception(e);
            }
        }
        return ret;
    }


    /**
     * マスタサーバへデータを送信する(バイナリデータ).<br>
     * Tagなし.<br>
     *
     * @param keyStr
     * @param values
     * @return boolean
     * @throws Exception
     */
    public boolean setByteValue(String keyStr, byte[] values) throws Exception {
        return this.setByteValue(keyStr, null, values);
    }


    /**
     * マスタサーバへデータを送信する(バイナリデータ).<br>
     * Tag有り.<br>
     * 処理の流れとしては、まずvalueを一定の容量で区切り、その単位で、<br>
     * Key値にプレフィックスを付けた値を作成し、かつ、特定のセパレータで連結して、<br>
     * 渡されたKeyを使用して連結文字を保存する<br>
     * 
     *
     * @param keyStr
     * @param tagStrs
     * @param values
     * @return boolean
     * @throws Exception
     */
    public boolean setByteValue(String keyStr, String[] tagStrs, byte[] values) throws Exception {

        boolean ret = false;

        int bufSize = 0;
        int nowCounter = 0;

        byte[] workData = null;
        int counter = 0;
        int tmpKeyIndex = 0;
        String tmpKey = null;
        StringBuffer saveKeys = new StringBuffer();
        String sep = "";

        String[] tmpKeys = null;
        int keyCount = values.length / this.saveSize;
        int much = values.length % this.saveSize;

        String firstKey = null;
        String endKey = null;
        try {

            // Byte Lenghtチェック
            if (keyStr.getBytes().length > maxValueSize) throw new Exception("Save Key Max Size " + maxValueSize + " Byte");
            if (tagStrs != null) {
                for (int i = 0; i < tagStrs.length; i++) {
                    if (tagStrs[i].getBytes().length > maxValueSize) throw new Exception("Tag Max Size " + maxValueSize + " Byte");
                }
            }

            if (much > 0) keyCount = keyCount + 1;

            // バイトデータを分割してノードに転送する
            // 転送サイズは動的に指定可能である
            for (int i = 0; i < keyCount; i++) {

                if (keyCount == (i + 1)) {

                    bufSize = values.length - nowCounter;
                } else {

                    bufSize = this.saveSize;
                }

                // 保存バッファコピー領域作成
                workData = new byte[bufSize];

                for (int workCounter = 0; workCounter < bufSize; workCounter++) {

                    workData[workCounter] = values[nowCounter];
                    nowCounter++;
                }

                // 分割したデータのキーを作成
                tmpKey = keyStr.hashCode() + "_" + i;

                // ノードにバイナリデータ保存
                if(!this.sendByteData(tmpKey, workData)) throw new Exception("Byte Data Save Node Error");
            }

            firstKey = keyStr.hashCode() + "_" + 0;
            endKey = tmpKey;

            if (firstKey.equals(endKey)) {
                saveKeys.append(firstKey);
            } else {
                saveKeys.append(firstKey);
                saveKeys.append(this.byteDataKeysSep);
                saveKeys.append(tmpKey);
            }

            ret = this.setValue(keyStr, tagStrs, saveKeys.toString());

        } catch (Exception e) {
            throw e;
        }
        return ret;
    }


    /**
     * マスタサーバへデータを送信する(バイナリデータ).<br>
     *
     * @param keyStr
     * @param values
     * @return boolean
     * @throws Exception
     */
    private boolean sendByteData(String keyStr, byte[] values) throws Exception {
        boolean ret = false; 
        String serverRetStr = null;
        String[] serverRet = null;
        String value = null;
        StringBuffer serverRequestBuf = new StringBuffer();

        String saveStr = null;

        try {

            // valuesがnullであることはない
            // Valueを圧縮し、Base64でエンコード
            value = new String(this.dataEncoding(this.execCompress(values)));

            // 処理番号連結
            serverRequestBuf.append("1");
            // セパレータ連結
            serverRequestBuf.append(ImdstKeyValueClient.sepStr);

            // Key連結(Keyはデータ送信時には必ず文字列が必要)
            serverRequestBuf.append(new String(this.dataEncoding(keyStr.getBytes())));
            // セパレータ連結
            serverRequestBuf.append(ImdstKeyValueClient.sepStr);

            // Tagは必ず存在しない
            // ブランク規定文字列を連結
            serverRequestBuf.append(ImdstKeyValueClient.blankStr);

            // セパレータ連結
            serverRequestBuf.append(ImdstKeyValueClient.sepStr);

            // TransactionCode連結
            serverRequestBuf.append(this.transactionCode);

            // セパレータ連結
            serverRequestBuf.append(ImdstKeyValueClient.sepStr);

            // Value連結
            serverRequestBuf.append(value);

            // サーバ送信
            pw.println(serverRequestBuf.toString());
            pw.flush();

            // サーバから結果受け取り
            serverRetStr = br.readLine();

            serverRet = serverRetStr.split(ImdstKeyValueClient.sepStr);

            // 処理の妥当性確認
            if (serverRet.length == 3 && serverRet[0].equals("1")) {
                if (serverRet[1].equals("true")) {

                    // 処理成功
                    ret = true;
                } else{

                    // 処理失敗(メッセージ格納)
                    throw new Exception(serverRet[2]);
                }
            } else {

                // 妥当性違反
                throw new Exception("Execute Violation of validity [" + serverRetStr + "]");
            }
        } catch (ConnectException ce) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.sendByteData(keyStr, values);
                } catch (Exception e) {
                    throw ce;
                }
            } else {
                throw ce;
            }
        } catch (SocketException se) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.sendByteData(keyStr, values);
                } catch (Exception e) {
                    throw se;
                }
            } else {
                throw se;
            }
        } catch (Throwable e) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.sendByteData(keyStr, values);
                } catch (Exception ee) {
                    throw new Exception(e);
                }
            } else {
                throw new Exception(e);
            }
        }
        return ret;
    }


    /**
     * マスタサーバからKeyでデータを取得する.<br>
     * 文字列エンコーディング指定なし.<br>
     * デフォルトエンコーディングにて復元.<br>
     *
     * @param keyStr
     * @return String[] 要素1(データ有無):"true" or "false",要素2(データ):"データ文字列"
     * @throws Exception
     */
    public String[] getValue(String keyStr) throws Exception {
        return this.getValue(keyStr, null);
    }

    /**
     * マスタサーバからKeyでデータを取得する.<br>
     * 文字列エンコーディング指定あり.<br>
     *
     * @param keyStr
     * @param encoding
     * @return String[] 要素1(データ有無):"true" or "false",要素2(データ):"データ文字列"
     * @throws Exception
     */
    public String[] getValue(String keyStr, String encoding) throws Exception {
        String[] ret = new String[2]; 
        String serverRetStr = null;
        String[] serverRet = null;

        StringBuffer serverRequestBuf = null;

        try {
            if (this.socket == null) throw new Exception("No ServerConnect!!");

            // エラーチェック
            // Keyに対する無指定チェック
            if (keyStr == null ||  keyStr.equals("")) {
                throw new Exception("The blank is not admitted on a key");
            }

            // 文字列バッファ初期化
            serverRequestBuf = new StringBuffer();


            // 処理番号連結
            serverRequestBuf.append("2");
            // セパレータ連結
            serverRequestBuf.append(ImdstKeyValueClient.sepStr);


            // Key連結(Keyはデータ送信時には必ず文字列が必要)
            serverRequestBuf.append(new String(this.dataEncoding(keyStr.getBytes())));


            // サーバ送信
            pw.println(serverRequestBuf.toString());
            pw.flush();

            // サーバから結果受け取り
            serverRetStr = br.readLine();

            serverRet = serverRetStr.split(ImdstKeyValueClient.sepStr);

            // 処理の妥当性確認
            if (serverRet[0].equals("2")) {
                if (serverRet[1].equals("true")) {

                    // データ有り
                    ret[0] = serverRet[1];

                    // Valueがブランク文字か調べる
                    if (serverRet[2].equals(ImdstKeyValueClient.blankStr)) {
                        ret[1] = "";
                    } else {

                        // Value文字列をBase64でデコード
                        if (encoding == null) {
                            ret[1] = new String(this.dataDecoding(serverRet[2].getBytes()));
                        } else {
                            ret[1] = new String(this.dataDecoding(serverRet[2].getBytes()), encoding);
                        }
                    }
                } else if(serverRet[1].equals("false")) {

                    // データなし
                    ret[0] = serverRet[1];
                    ret[1] = null;
                } else if(serverRet[1].equals("error")) {

                    // エラー発生
                    ret[0] = serverRet[1];
                    ret[1] = serverRet[2];
                }
            } else {

                // 妥当性違反
                throw new Exception("Execute Violation of validity");
            }
        } catch (ConnectException ce) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.getValue(keyStr, encoding);
                } catch (Exception e) {
                    throw ce;
                }
            } else {
                throw ce;
            }
        } catch (SocketException se) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.getValue(keyStr, encoding);
                } catch (Exception e) {
                    throw se;
                }
            } else {
                throw se;
            }
        } catch (Throwable e) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.getValue(keyStr, encoding);
                } catch (Exception ee) {
                    throw new Exception(e);
                }
            } else {
                throw new Exception(e);
            }
        }
        return ret;
    }


    /**
     * マスタサーバからKeyでデータを取得する.<br>
     * Key値をBase64でエンコードしない.<br>
     *
     * @param keyStr
     * @param encoding
     * @return String[] 要素1(データ有無):"true" or "false",要素2(データ):"データ文字列"
     * @throws Exception
     */
    public String[] getValueNoEncode(String keyStr) throws Exception {
        String[] ret = new String[2]; 
        String serverRetStr = null;
        String[] serverRet = null;

        StringBuffer serverRequestBuf = null;

        try {
            if (this.socket == null) throw new Exception("No ServerConnect!!");

            // エラーチェック
            // Keyに対する無指定チェック
            if (keyStr == null ||  keyStr.equals("")) {
                throw new Exception("The blank is not admitted on a key");
            }

            // 文字列バッファ初期化
            serverRequestBuf = new StringBuffer();


            // 処理番号連結
            serverRequestBuf.append("2");
            // セパレータ連結
            serverRequestBuf.append(ImdstKeyValueClient.sepStr);


            // Key連結(Keyはデータ送信時には必ず文字列が必要)
            serverRequestBuf.append(keyStr);


            // サーバ送信
            pw.println(serverRequestBuf.toString());
            pw.flush();

            // サーバから結果受け取り
            serverRetStr = br.readLine();

            serverRet = serverRetStr.split(ImdstKeyValueClient.sepStr);

            // 処理の妥当性確認
            if (serverRet[0].equals("2")) {
                if (serverRet[1].equals("true")) {

                    // データ有り
                    ret[0] = serverRet[1];

                    // Valueがブランク文字か調べる
                    if (serverRet[2].equals(ImdstKeyValueClient.blankStr)) {
                        ret[1] = "";
                    } else {

                        ret[1] = serverRet[2];
                    }
                } else if(serverRet[1].equals("false")) {

                    // データなし
                    ret[0] = serverRet[1];
                    ret[1] = null;
                } else if(serverRet[1].equals("error")) {

                    // エラー発生
                    ret[0] = serverRet[1];
                    ret[1] = serverRet[2];
                }
            } else {

                // 妥当性違反
                throw new Exception("Execute Violation of validity");
            }
        } catch (ConnectException ce) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.getValueNoEncode(keyStr);
                } catch (Exception e) {
                    throw ce;
                }
            } else {
                throw ce;
            }
        } catch (SocketException se) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.getValueNoEncode(keyStr);
                } catch (Exception e) {
                    throw se;
                }
            } else {
                throw se;
            }
        } catch (Throwable e) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.getValueNoEncode(keyStr);
                } catch (Exception ee) {
                    throw new Exception(e);
                }
            } else {
                throw new Exception(e);
            }
        }
        return ret;
    }


    /**
     * マスタサーバからKeyでデータを取得する.<br>
     * Scriptを同時に実行する.<br>
     * 文字列エンコーディング指定あり.<br>
     *
     * @param keyStr キー値
     * @param scriptStr スクリプト文
     * @return String[] 要素1(データ、エラー有無):"true" or "false" or "error", 要素2(データorエラー内容):"文字列"
     * @throws Exception
     */
    public String[] getValueScript(String keyStr, String scriptStr) throws Exception {
        return getValueScript(keyStr, scriptStr, null);
        
    }


    /**
     * マスタサーバからKeyでデータを取得する.<br>
     * Scriptを同時に実行する.<br>
     * 文字列エンコーディング指定あり.<br>
     *
     * @param keyStr キー値
     * @param scriptStr スクリプト文
     * @param encoding
     * @return String[] 要素1(データ、エラー有無):"true" or "false" or "error", 要素2(データorエラー内容):"文字列"
     * @throws Exception
     */
    public String[] getValueScript(String keyStr, String scriptStr, String encoding) throws Exception {
        String[] ret = new String[2]; 
        String serverRetStr = null;
        String[] serverRet = null;

        StringBuffer serverRequestBuf = null;

        try {
            if (this.socket == null) throw new Exception("No ServerConnect!!");

            // エラーチェック
            // Keyに対する無指定チェック
            if (keyStr == null ||  keyStr.equals("")) {
                throw new Exception("The blank is not admitted on a key");
            } 

            // Scriptチェック
            if (scriptStr == null ||  scriptStr.equals("")) {
                scriptStr = ImdstKeyValueClient.blankStr;
                throw new Exception("The blank is not admitted on a Script");
            } 

            // 文字列バッファ初期化
            serverRequestBuf = new StringBuffer();


            // 処理番号連結
            serverRequestBuf.append("8");
            // セパレータ連結
            serverRequestBuf.append(ImdstKeyValueClient.sepStr);


            // Key連結(Keyはデータ送信時には必ず文字列が必要)
            serverRequestBuf.append(new String(this.dataEncoding(keyStr.getBytes())));
            // セパレータ連結
            serverRequestBuf.append(ImdstKeyValueClient.sepStr);
            // Script連結
            serverRequestBuf.append(new String(this.dataEncoding(scriptStr.getBytes())));

            // サーバ送信
            pw.println(serverRequestBuf.toString());
            pw.flush();

            // サーバから結果受け取り
            serverRetStr = br.readLine();

            serverRet = serverRetStr.split(ImdstKeyValueClient.sepStr);

            // 処理の妥当性確認
            if (serverRet[0].equals("8")) {
                if (serverRet[1].equals("true")) {

                    // データ有り
                    ret[0] = serverRet[1];

                    // Valueがブランク文字か調べる
                    if (serverRet[2].equals(ImdstKeyValueClient.blankStr)) {
                        ret[1] = "";
                    } else {

                        // Value文字列をBase64でデコード
                        if (encoding == null) {
                            ret[1] = new String(this.dataDecoding(serverRet[2].getBytes()));
                        } else {
                            ret[1] = new String(this.dataDecoding(serverRet[2].getBytes()), encoding);
                        }
                    }
                } else if(serverRet[1].equals("false")) {

                    // データなし
                    ret[0] = serverRet[1];
                    ret[1] = null;
                } else if(serverRet[1].equals("error")) {

                    // エラー発生
                    ret[0] = serverRet[1];
                    ret[1] = serverRet[2];
                }
            } else {

                // 妥当性違反
                throw new Exception("Execute Violation of validity");
            }
        } catch (ConnectException ce) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.getValueScript(keyStr, scriptStr, encoding);
                } catch (Exception e) {
                    throw ce;
                }
            } else {
                throw ce;
            }
        } catch (SocketException se) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.getValueScript(keyStr, scriptStr, encoding);
                } catch (Exception e) {
                    throw se;
                }
            } else {
                throw se;
            }
        } catch (Throwable e) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.getValueScript(keyStr, scriptStr, encoding);
                } catch (Exception ee) {
                    throw new Exception(e);
                }
            } else {
                throw new Exception(e);
            }
        }
        return ret;
    }


    /**
     * マスタサーバからKeyでデータを取得する.<br>
     * Scriptを同時に実行する.<br>
     * ScriptにValue更新指示を記述してる場合はこちらを実行する.<br>
     * 文字列エンコーディング指定あり.<br>
     *
     * @param keyStr キー値
     * @param scriptStr スクリプト文
     * @return String[] 要素1(データ、エラー有無):"true" or "false" or "error", 要素2(データorエラー内容):"文字列"
     * @throws Exception
     */
    public String[] getValueScriptForUpdate(String keyStr, String scriptStr) throws Exception {
        return getValueScriptForUpdate(keyStr, scriptStr, null);
        
    }


    /**
     * マスタサーバからKeyでデータを取得する.<br>
     * Scriptを同時に実行する.<br>
     * ScriptにValue更新指示を記述してる場合はこちらを実行する.<br>
     * 文字列エンコーディング指定あり.<br>
     *
     * @param keyStr キー値
     * @param scriptStr スクリプト文
     * @param encoding
     * @return String[] 要素1(データ、エラー有無):"true" or "false" or "error", 要素2(データorエラー内容):"文字列"
     * @throws Exception
     */
    public String[] getValueScriptForUpdate(String keyStr, String scriptStr, String encoding) throws Exception {
        String[] ret = new String[2]; 
        String serverRetStr = null;
        String[] serverRet = null;

        StringBuffer serverRequestBuf = null;

        try {
            if (this.socket == null) throw new Exception("No ServerConnect!!");

            // エラーチェック
            // Keyに対する無指定チェック
            if (keyStr == null ||  keyStr.equals("")) {
                throw new Exception("The blank is not admitted on a key");
            } 

            // Scriptチェック
            if (scriptStr == null ||  scriptStr.equals("")) {
                scriptStr = ImdstKeyValueClient.blankStr;
                throw new Exception("The blank is not admitted on a Script");
            } 

            // 文字列バッファ初期化
            serverRequestBuf = new StringBuffer();


            // 処理番号連結
            serverRequestBuf.append("9");
            // セパレータ連結
            serverRequestBuf.append(ImdstKeyValueClient.sepStr);


            // Key連結(Keyはデータ送信時には必ず文字列が必要)
            serverRequestBuf.append(new String(this.dataEncoding(keyStr.getBytes())));
            // セパレータ連結
            serverRequestBuf.append(ImdstKeyValueClient.sepStr);
            // Script連結
            serverRequestBuf.append(new String(this.dataEncoding(scriptStr.getBytes())));

            // サーバ送信
            pw.println(serverRequestBuf.toString());
            pw.flush();

            // サーバから結果受け取り
            serverRetStr = br.readLine();

            serverRet = serverRetStr.split(ImdstKeyValueClient.sepStr);

            // 処理の妥当性確認
            if (serverRet[0].equals("9")) {
                if (serverRet[1].equals("true")) {

                    // データ有り
                    ret[0] = serverRet[1];

                    // Valueがブランク文字か調べる
                    if (serverRet[2].equals(ImdstKeyValueClient.blankStr)) {
                        ret[1] = "";
                    } else {

                        // Value文字列をBase64でデコード
                        if (encoding == null) {
                            ret[1] = new String(this.dataDecoding(serverRet[2].getBytes()));
                        } else {
                            ret[1] = new String(this.dataDecoding(serverRet[2].getBytes()), encoding);
                        }
                    }
                } else if(serverRet[1].equals("false")) {

                    // データなし
                    ret[0] = serverRet[1];
                    ret[1] = null;
                } else if(serverRet[1].equals("error")) {

                    // エラー発生
                    ret[0] = serverRet[1];
                    ret[1] = serverRet[2];
                }
            } else {

                // 妥当性違反
                throw new Exception("Execute Violation of validity");
            }
        } catch (ConnectException ce) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.getValueScriptForUpdate(keyStr, scriptStr, encoding);
                } catch (Exception e) {
                    throw ce;
                }
            } else {
                throw ce;
            }
        } catch (SocketException se) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.getValueScriptForUpdate(keyStr, scriptStr, encoding);
                } catch (Exception e) {
                    throw se;
                }
            } else {
                throw se;
            }
        } catch (Throwable e) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.getValueScriptForUpdate(keyStr, scriptStr, encoding);
                } catch (Exception ee) {
                    throw new Exception(e);
                }
            } else {
                throw new Exception(e);
            }
        }
        return ret;
    }


    /**
     * マスタサーバからKeyでデータを削除する.<br><br>
     *
     * @param keyStr
     * @return String[] 要素1(データ有無):"true" or "false",要素2(データ):"データ文字列"
     * @throws Exception
     */
    public String[] removeValue(String keyStr) throws Exception {
        return this.removeValue(keyStr, null);
    }

    /**
     * マスタサーバからKeyでデータを削除する.<br>
     * 取得値のエンコーディング指定あり.<br>
     * @param keyStr
     * @return String[] 削除したデータ 内容) 要素1(データ削除有無):"true" or "false",要素2(削除データ):"データ文字列"
     * @throws Exception
     */
    public String[] removeValue(String keyStr, String encoding) throws Exception {
        String[] ret = new String[2]; 
        String serverRetStr = null;
        String[] serverRet = null;

        StringBuffer serverRequestBuf = null;

        try {
            if (this.socket == null) throw new Exception("No ServerConnect!!");

            // エラーチェック
            // Keyに対する無指定チェック
            if (keyStr == null ||  keyStr.equals("")) {
                throw new Exception("The blank is not admitted on a key");
            }

            // 文字列バッファ初期化
            serverRequestBuf = new StringBuffer();


            // 処理番号連結
            serverRequestBuf.append("5");
            // セパレータ連結
            serverRequestBuf.append(ImdstKeyValueClient.sepStr);
            // Key連結(Keyはデータ送信時には必ず文字列が必要)
            serverRequestBuf.append(new String(this.dataEncoding(keyStr.getBytes())));
            // セパレータ連結
            serverRequestBuf.append(ImdstKeyValueClient.sepStr);
            // TransactionCode連結
            serverRequestBuf.append(this.transactionCode);


            // サーバ送信
            pw.println(serverRequestBuf.toString());
            pw.flush();

            // サーバから結果受け取り
            serverRetStr = br.readLine();

            serverRet = serverRetStr.split(ImdstKeyValueClient.sepStr);

            // 処理の妥当性確認
            if (serverRet[0].equals("5")) {
                if (serverRet[1].equals("true")) {

                    // データ有り
                    ret[0] = serverRet[1];

                    // Valueがブランク文字か調べる
                    if (serverRet[2].equals(ImdstKeyValueClient.blankStr)) {
                        ret[1] = "";
                    } else {

                        // Value文字列をBase64でデコード
                        if (encoding == null) {
                            ret[1] = new String(this.dataDecoding(serverRet[2].getBytes()));
                        } else {
                            ret[1] = new String(this.dataDecoding(serverRet[2].getBytes()), encoding);
                        }
                    }
                } else if(serverRet[1].equals("false")) {

                    // データなし
                    ret[0] = serverRet[1];
                    ret[1] = null;
                } else if(serverRet[1].equals("error")) {

                    // エラー発生
                    ret[0] = serverRet[1];
                    ret[1] = serverRet[2];
                }
            } else {

                // 妥当性違反
                throw new Exception("Execute Violation of validity");
            }
        } catch (ConnectException ce) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.removeValue(keyStr, encoding);
                } catch (Exception e) {
                    throw ce;
                }
            } else {
                throw ce;
            }
        } catch (SocketException se) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.removeValue(keyStr, encoding);
                } catch (Exception e) {
                    throw se;
                }
            } else {
                throw se;
            }
        } catch (Throwable e) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.removeValue(keyStr, encoding);
                } catch (Exception ee) {
                    throw new Exception(e);
                }
            } else {
                throw new Exception(e);
            }
        }
        return ret;
    }


    /**
     * マスタサーバからKeyでデータを取得する(バイナリ).<br>
     *
     * @param keyStr
     * @return Object[] 要素1(String)(データ有無):"true" or "false",要素2(byte[])(データ):{バイト配列}
     * @throws Exception
     */
    public Object[] getByteValue(String keyStr) throws Exception {
        Object[] ret = new Object[2];
        Object[] byteTmpRet = null;

        String[] workKeyRet = null;
        String workKeyStr = null;
        String[] workKeys = null;

        byte[] workValue = null;
        byte[] tmpValue = new byte[0];
        byte[] retValue = new byte[0];

        boolean execFlg = true;
        try {
            workKeyRet = this.getValue(keyStr);
            
            if (workKeyRet[0].equals("true")) {

                workKeyStr = (String)workKeyRet[1];

                workKeyRet = workKeyStr.split(byteDataKeysSep);

                if (workKeyRet.length > 1) {
                    String[] keyWork = workKeyRet[1].split("_");

                    String keyStrPre = keyWork[0];
                    //右辺のIndex数値+1が配列サイズ
                    int maxKeyIndexSize = Integer.parseInt(keyWork[1]) + 1;

                    workKeyRet = new String[maxKeyIndexSize];
                    for (int i = 0; i < workKeyRet.length; i++) {
                        workKeyRet[i] = keyStrPre + "_" + i;
                    }
                }

                for (int idx = 0; idx < workKeyRet.length; idx++) {

                    byteTmpRet = this.getByteData(workKeyRet[idx]);

                    if (byteTmpRet[0].equals("true")) {

                        workValue = (byte[])byteTmpRet[1];

                        if (execFlg) {

                            tmpValue = new byte[retValue.length + workValue.length];

                            for (int i = 0; i < retValue.length; i++) {
                                tmpValue[i] = retValue[i];
                            }

                            for (int i = 0; i < workValue.length; i++) {
                                tmpValue[retValue.length + i] = workValue[i];
                            }
                            execFlg = false;
                        } else {

                            retValue = new byte[tmpValue.length + workValue.length];

                            for (int i = 0; i < tmpValue.length; i++) {
                                retValue[i] = tmpValue[i];
                            }

                            for (int i = 0; i < workValue.length; i++) {
                                retValue[tmpValue.length + i] = workValue[i];
                            }
                            execFlg = true;
                        }
                    } else {

                        // エラー発生
                        ret[0] = byteTmpRet[0];
                        ret[1] = byteTmpRet[1];
                        break;
                    }
                }

                ret[0] = "true";
                if (retValue.length >= tmpValue.length) {
                    ret[1] = retValue;
                } else{
                    ret[1] = tmpValue;
                }
            } else {
                ret[0] = workKeyRet[0];
                ret[1] = workKeyRet[1];
            }
        } catch(Exception e) {
            throw e;
        }
        return ret;
    }


    /**
     * マスタサーバからKeyでデータを取得する(バイナリ).<br>
     *
     * @param keyStr
     * @return Object[] 要素1(String)(データ有無):"true" or "false",要素2(byte[])(データ):{バイト配列}
     * @throws Exception
     */
    public Object[] getByteValueVer2(String keyStr) throws Exception {
        ArrayList byteDataList = null;
        Object[] ret = new Object[2];
        Object[] byteTmpRet = null;

        String[] workKeyRet = null;
        String workKeyStr = null;
        String[] workKeys = null;

        byte[] workValue = null;
        byte[] tmpValue = new byte[0];
        byte[] retValue = new byte[0];

        boolean execFlg = true;
        try {
            workKeyRet = this.getValue(keyStr);
            
            if (workKeyRet[0].equals("true")) {

                workKeyStr = (String)workKeyRet[1];

                workKeyRet = workKeyStr.split(byteDataKeysSep);

                if (workKeyRet.length > 1) {
                    String[] keyWork = workKeyRet[1].split("_");

                    String keyStrPre = keyWork[0];
                    //右辺のIndex数値+1が配列サイズ
                    int maxKeyIndexSize = Integer.parseInt(keyWork[1]) + 1;

                    workKeyRet = new String[maxKeyIndexSize];
                    for (int i = 0; i < workKeyRet.length; i++) {
                        workKeyRet[i] = keyStrPre + "_" + i;
                    }
                }

                byteDataList = new ArrayList(workKeyRet.length * new Double(maxValueSize * 1.38).intValue());
                for (int idx = 0; idx < workKeyRet.length; idx++) {


                    byteTmpRet = this.getByteData(workKeyRet[idx]);

                    if (byteTmpRet[0].equals("true")) {

                        workValue = (byte[])byteTmpRet[1];

                        if (execFlg) {
                            for (int i = 0; i < workValue.length; i++) {
                                byteDataList.add(workValue[i]);
                            }
                            execFlg = false;
                        } else {

                            for (int i = 0; i < workValue.length; i++) {
                                byteDataList.add(workValue[i]);
                            }
                            execFlg = true;
                        }
                    } else {

                        // エラー発生
                        ret[0] = byteTmpRet[0];
                        ret[1] = byteTmpRet[1];
                        break;
                    }

                }

                ret[0] = "true";
                byte[] retBytes = new byte[byteDataList.size()];
                int size = byteDataList.size();
                int idx = 1;
                for (; idx < size; idx++) {
                    retBytes[size - idx] = ((Byte)byteDataList.remove((size - idx))).byteValue();
                }
                retBytes[0] = ((Byte)byteDataList.remove((0))).byteValue();

                ret[1] = retBytes;
            } else {
                ret[0] = workKeyRet[0];
                ret[1] = workKeyRet[1];
            }
        } catch(Exception e) {
            throw e;
        }
        return ret;
    }


    /**
     * マスタサーバからKeyでデータを取得する(バイナリ).<br>
     *
     * @param keyStr
     * @return Object[] 要素1(String)(データ有無):"true" or "false",要素2(byte[])(データ):{バイト配列}
     * @throws Exception
     */
    private Object[] getByteData(String keyStr) throws Exception {
        Object[] ret = new Object[2];
        byte[] byteRet = null;
        String serverRetStr = null;
        String[] serverRet = null;

        StringBuffer serverRequestBuf = null;

        try {
            if (this.socket == null) throw new Exception("No ServerConnect!!");

            // エラーチェック
            // Keyに対する無指定チェック
            if (keyStr == null ||  keyStr.equals("")) {
                throw new Exception("The blank is not admitted on a key");
            }

            // 文字列バッファ初期化
            serverRequestBuf = new StringBuffer();


            // 処理番号連結
            serverRequestBuf.append("2");
            // セパレータ連結
            serverRequestBuf.append(ImdstKeyValueClient.sepStr);


            // Key連結(Keyはデータ送信時には必ず文字列が必要)
            serverRequestBuf.append(new String(this.dataEncoding(keyStr.getBytes())));


            // サーバ送信
            pw.println(serverRequestBuf.toString());
            pw.flush();

            // サーバから結果受け取り
            serverRetStr = br.readLine();

            serverRet = serverRetStr.split(ImdstKeyValueClient.sepStr);

            // 処理の妥当性確認
            if (serverRet[0].equals("2")) {
                if (serverRet[1].equals("true")) {

                    // データ有り
                    ret[0] = serverRet[1];

                    // Valueがブランク文字か調べる
                    if (serverRet[2].equals(ImdstKeyValueClient.blankStr)) {
                        byteRet = new byte[0];
                        ret[1] = byteRet;
                    } else {

                        // Value文字列をBase64でデコードし、圧縮解除
                        ret[1] = this.execDecompres(this.dataDecoding(serverRet[2].getBytes()));
                        //ret[1] = this.dataDecoding(this.execDecompres(this.dataDecoding(serverRet[2].getBytes())));
                    }
                } else if(serverRet[1].equals("false")) {

                    // データなし
                    ret[0] = serverRet[1];
                    ret[1] = null;
                } else if(serverRet[1].equals("error")) {

                    // エラー発生
                    ret[0] = serverRet[1];
                    ret[1] = serverRet[2];
                }
            } else {

                // 妥当性違反
                throw new Exception("Execute Violation of validity");
            }
        } catch (ConnectException ce) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.getByteData(keyStr);
                } catch (Exception e) {
                    throw ce;
                }
            } else {
                throw ce;
            }
        } catch (SocketException se) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.getByteData(keyStr);
                } catch (Exception e) {
                    throw se;
                }
            } else {
                throw se;
            }
        } catch (Throwable e) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.getByteData(keyStr);
                } catch (Exception ee) {
                    throw new Exception(e);
                }
            } else {
                throw new Exception(e);
            }
        }
        return ret;
    }


    /**
     * マスタサーバからTagでKey値群を取得する.<br>
     *
     * @param tagStr
     * @return Object[] 要素1(データ有無):"true" or "false",要素2(データ):"データ文字列"
     * @throws Exception
     */
    public Object[] getTagKeys(String tagStr) throws Exception {
        Object[] ret = new Object[2]; 
        String serverRetStr = null;
        String[] serverRet = null;

        StringBuffer serverRequestBuf = null;

        try {
            if (this.socket == null) throw new Exception("No ServerConnect!!");

            // エラーチェック
            // Keyに対する無指定チェック
            if (tagStr == null ||  tagStr.equals("")) {
                throw new Exception("The blank is not admitted on a tag");
            }

            // 文字列バッファ初期化
            serverRequestBuf = new StringBuffer();


            // 処理番号連結
            serverRequestBuf.append("3");
            // セパレータ連結
            serverRequestBuf.append(ImdstKeyValueClient.sepStr);


            // tag連結(Keyはデータ送信時には必ず文字列が必要)
            serverRequestBuf.append(new String(this.dataEncoding(tagStr.getBytes())));


            // サーバ送信
            pw.println(serverRequestBuf.toString());
            pw.flush();

            // サーバから結果受け取り
            serverRetStr = br.readLine();

            serverRet = serverRetStr.split(ImdstKeyValueClient.sepStr);

            // 処理の妥当性確
            if (serverRet[0].equals("4")) {
                if (serverRet[1].equals("true")) {

                    // データ有り
                    ret[0] = serverRet[1];

                    // Valueがブランク文字か調べる
                    if (serverRet[2].equals(ImdstKeyValueClient.blankStr)) {
                        String[] tags = {""};
                        ret[1] = tags;
                    } else {
                        String[] tags = null;
                        String[] cnvTags = null;

                        tags = serverRet[2].split(tagKeySep);
                        String[] decTags = new String[tags.length];
                        for (int i = 0; i < tags.length; i++) {
                            decTags[i] = new String(this.dataDecoding(tags[i].getBytes()));
                        }
                        ret[1] = decTags;
                    }
                } else if(serverRet[1].equals("false")) {

                    // データなし
                    ret[0] = serverRet[1];
                    ret[1] = null;
                } else if(serverRet[1].equals("error")) {

                    // エラー発生
                    ret[0] = serverRet[1];
                    ret[1] = serverRet[2];
                }
            } else {

                // 妥当性違反
                throw new Exception("Execute Violation of validity [" + serverRet[0] + "]");
            }
        } catch (ConnectException ce) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.getTagKeys(tagStr);
                } catch (Exception e) {
                    throw ce;
                }
            } else {
                throw ce;
            }
        } catch (SocketException se) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.getTagKeys(tagStr);
                } catch (Exception e) {
                    throw se;
                }
            } else {
                throw se;
            }
        } catch (Throwable e) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.getTagKeys(tagStr);
                } catch (Exception ee) {
                    throw new Exception(e);
                }
            } else {
                throw new Exception(e);
            }
        }
        return ret;
    }


    /**
     * DataNodeのステータスを取得する.<br>
     * DataNodのステータスは常にメインマスターノードが管理しているので、メインマスターノードに<br>
     * 接続している場合のみ取得可能.<br>
     *
     */
    public String getDataNodeStatus(String nodeInfo) throws Exception {
        String ret = null;
        String serverRetStr = null;
        String[] serverRet = null;

        StringBuffer serverRequestBuf = null;

        try {
            if (this.socket == null) throw new Exception("No ServerConnect!!");


            // 文字列バッファ初期化
            serverRequestBuf = new StringBuffer();


            // 処理番号連結
            serverRequestBuf.append("10");
            // セパレータ連結
            serverRequestBuf.append(ImdstKeyValueClient.sepStr);
            // ノード名連結
            serverRequestBuf.append(nodeInfo);

            // サーバ送信
            pw.println(serverRequestBuf.toString());
            pw.flush();

            // サーバから結果受け取り
            serverRetStr = br.readLine();

            serverRet = serverRetStr.split(ImdstKeyValueClient.sepStr);

            // 処理の妥当性確認
            if (serverRet[0].equals("10")) {
                if (serverRet[1].equals("true")) {
                    if (serverRet.length >= 3) {
                        ret = serverRet[2];
                    } else {
                        ret = "";
                    }
                } else {
                    ret = "";
                }
            } else {

                // 妥当性違反
                throw new Exception("Execute Violation of validity");
            }

        } catch (ConnectException ce) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.nextConnect();
                    ret = this.getDataNodeStatus(nodeInfo);
                } catch (Exception e) {
                    throw ce;
                }
            } else {
                throw ce;
            }
        } catch (SocketException se) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.nextConnect();
                    ret = this.getDataNodeStatus(nodeInfo);
                } catch (Exception e) {
                    throw se;
                }
            } else {
                throw se;
            }
        } catch (Exception e) {
            throw e;
        }
        return ret;
    }


    // Base64でエンコード
    private byte[] dataEncoding(byte[] datas) {
        return BASE64EncoderStream.encode(datas);
    }

    // Base64でデコード
    private byte[] dataDecoding(byte[] datas) {
        return BASE64DecoderStream.decode(datas);
    }

    // 圧縮メソッド
    private byte[] execCompress(byte[] bytes) throws Exception {
        if (!this.compressMode) return bytes;
        try {
            Deflater compresser = new Deflater(); 

            compresser.setInput(bytes); 
            compresser.finish();

            // 圧縮単位
            int bufSize = 2048;

            byte[] output = new byte[bufSize]; 
            byte[] workByte1 = new byte[0];
            byte[] workByte2 = new byte[0];

            int flg = 0;
            int use = 1;

            while(true) {
                int compressedDataLength = compresser.deflate(output);

                if (compressedDataLength == bufSize) {
                    if (flg == 0) {
                        workByte1 = new byte[workByte2.length + bufSize];

                        for (int i = 0; i < workByte2.length; i++) {
                            workByte1[i] = workByte2[i];
                        }

                        for (int i = 0; i < bufSize; i++) {
                            workByte1[workByte2.length + i] = output[i];
                        }

                        flg = 1;
                    } else {
                        workByte2 = new byte[workByte1.length + bufSize];

                        for (int i = 0; i < workByte1.length; i++) {
                            workByte2[i] = workByte1[i];
                        }

                        for (int i = 0; i < bufSize; i++) {
                            workByte2[workByte1.length + i] = output[i];
                        }
                        flg = 0;
                    }
                } else {
                    if (workByte1.length == workByte2.length) {

                        workByte1 = new byte[compressedDataLength];
                        for (int i = 0; i < compressedDataLength; i++) {
                            workByte1[i] = output[i];
                        }
                    } else if (workByte1.length > workByte2.length) {

                        workByte2 = new byte[workByte1.length + compressedDataLength];
                        for (int i = 0; i < workByte1.length; i++) {
                            workByte2[i] = workByte1[i];
                        }

                        for (int i = 0; i < compressedDataLength; i++) {
                            workByte2[workByte1.length + i] = output[i];
                        }
                    } else if (workByte1.length < workByte2.length) {

                        workByte1 = new byte[workByte2.length + compressedDataLength];

                        for (int i = 0; i < workByte2.length; i++) {
                            workByte1[i] = workByte2[i];
                        }

                        for (int i = 0; i < compressedDataLength; i++) {
                            workByte1[workByte2.length + i] = output[i];
                        }
                    }
                    break;
                }
            }



            if (workByte1.length > workByte2.length) {

                return workByte1;
            } else if (workByte1.length < workByte2.length) {

                return workByte2;
            }
        } catch (Exception e) {
            throw e;
        }
        return null;
    }


    // 圧縮解除メソッド
    private byte[] execDecompres(byte[] bytes)  throws Exception {
        if (!this.compressMode) return bytes;
        try {
            // 圧縮解除単位
            int bufSize = 2048;

            Inflater decompresser = new Inflater(); 
            decompresser.setInput(bytes, 0, bytes.length); 

            byte[] result = new byte[bufSize]; 
            byte[] workByte1 = new byte[0];
            byte[] workByte2 = new byte[0];
            int flg = 0;

            while(true) {
                int resultLength = decompresser.inflate(result); 

                if (resultLength == bufSize) {
                    if (flg == 0) {
                        workByte1 = new byte[workByte2.length + bufSize];

                        for (int i = 0; i < workByte2.length; i++) {
                            workByte1[i] = workByte2[i];
                        }

                        for (int i = 0; i < bufSize; i++) {
                            workByte1[workByte2.length + i] = result[i];
                        }

                        flg = 1;
                    } else {
                        workByte2 = new byte[workByte1.length + bufSize];

                        for (int i = 0; i < workByte1.length; i++) {
                            workByte2[i] = workByte1[i];
                        }

                        for (int i = 0; i < bufSize; i++) {
                            workByte2[workByte1.length + i] = result[i];
                        }
                        flg = 0;
                    }
                } else {

                    if (workByte1.length == workByte2.length) {

                        workByte1 = new byte[resultLength];
                        for (int i = 0; i < resultLength; i++) {
                            workByte1[i] = result[i];
                        }
                    } else if (workByte1.length > workByte2.length) {

                        workByte2 = new byte[workByte1.length + resultLength];
                        for (int i = 0; i < workByte1.length; i++) {
                            workByte2[i] = workByte1[i];
                        }

                        for (int i = 0; i < resultLength; i++) {
                            workByte2[workByte1.length + i] = result[i];
                        }
                    } else if (workByte1.length < workByte2.length) {

                        workByte1 = new byte[workByte2.length + resultLength];

                        for (int i = 0; i < workByte2.length; i++) {
                            workByte1[i] = workByte2[i];
                        }

                        for (int i = 0; i < resultLength; i++) {
                            workByte1[workByte2.length + i] = result[i];
                        }
                    }
                    break;
                }
            }

            decompresser.end();

            if (workByte1.length > workByte2.length) {

                return workByte1;
            } else if (workByte1.length < workByte2.length) {

                return workByte2;
            }
        } catch (Exception e) {
            throw e;
        }
        return null;
    }
}
