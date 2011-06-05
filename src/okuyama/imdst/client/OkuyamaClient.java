package okuyama.imdst.client;

import java.util.*;
import java.io.*;
import java.net.*;
import java.util.zip.*;

import com.sun.mail.util.BASE64DecoderStream;
import com.sun.mail.util.BASE64EncoderStream;

import okuyama.imdst.util.ImdstDefine;


/**
 * MasterNodeと通信を行うクライアント<br>
 * ユーザ開発アプリケーションが利用する想定.<br>
 *<br>
 * 使い方)<br>
 *<br>
 * // Create Okuyama Client Instance<br>
 * OkuyamaClient okuyamaClient = new OkuyamaClient();<br>
 *<br>
 * try {<br>
 *&nbsp;&nbsp; // --------------<br>
 *&nbsp;&nbsp; // Connecting MasterNode<br>
 *&nbsp;&nbsp; okuyamaClient.connect(args[1], port);<br>
 *<br>
 *&nbsp;&nbsp; // Multi MasterNode<br>
 *&nbsp;&nbsp; // String[] infos = {"MasterNode01:PortNo","MasterNode02:PortNo","MasterNode03:PortNo"};<br>
 *&nbsp;&nbsp; // okuyamaClient.setConnectionInfos(infos);<br>
 *&nbsp;&nbsp; // okuyamaClient.autoConnect();<br>
 *<br>
 *<br>
 *&nbsp;&nbsp;&nbsp;// --------------<br>
 *&nbsp;&nbsp;&nbsp;// Set Value<br>
 *&nbsp;&nbsp;&nbsp;if (okuyamaClient.setValue("datasavekey_001", "savedatavalue_001")) {<br>
 *<br>
 *&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;System.out.println("setValue - Success");<br>
 *&nbsp;&nbsp;&nbsp;} else {<br>
 *<br>
 *&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;System.out.println("setValue - Error");<br>
 *&nbsp;&nbsp;&nbsp;}<br>
 *<br>
 *<br>
 *&nbsp;&nbsp;&nbsp;// --------------<br>
 *&nbsp;&nbsp;&nbsp;// Get Value<br>
 *&nbsp;&nbsp;&nbsp;String[] getRet = okuyamaClient.getValue("datasavekey_001");<br>
 *&nbsp;&nbsp;&nbsp;if (getRet[0].equals("true")) {<br>
 *<br>
 *&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;// Data exists<br>
 *&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;// Value is displayed<br>
 *&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;System.out.println(ret[1]);<br>
 *&nbsp;&nbsp;&nbsp;} else if (getRet[0].equals("false")) {<br>
 *&nbsp;&nbsp;&nbsp;<br>
 *&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;// Data not found<br>
 *&nbsp;&nbsp;&nbsp;} else if (getRet[0].equals("error")) {<br>
 *<br>
 *&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;// Server error<br>
 *&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;// ErrorMessage is displayed<br>
 *&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;System.out.println(ret[1]);<br>
 *&nbsp;&nbsp;&nbsp;}<br>
 *<br>
 *<br>
 *&nbsp;&nbsp;&nbsp;// --------------<br>
 *&nbsp;&nbsp;&nbsp;// Set Value & Tag<br>
 *&nbsp;&nbsp;&nbsp;String[] tags = {"tag1", "tag2"};<br>
 *&nbsp;&nbsp;&nbsp;if (!okuyamaClient.setValue("datasavekey_001", tags, "savedatavalue_001")) {<br>
 *<br>
 *&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;System.out.println("setValue - Error");<br>
 *&nbsp;&nbsp;&nbsp;} else {<br>
 *<br>
 *&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;System.out.println("setValue - Success");<br>
 *&nbsp;&nbsp;&nbsp;}<br>
 *<br>
 *<br>
 *&nbsp;&nbsp;&nbsp;// --------------<br>
 *&nbsp;&nbsp;&nbsp;// Tag Get<br>
 *&nbsp;&nbsp;&nbsp;Object[] ret = okuyamaClient.getTagKeys("tag1");<br>
 *&nbsp;&nbsp;&nbsp;if (ret[0].equals("true")) {<br>
 *<br>
 *&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;// Data exists<br>
 *&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;String[] keys = (String[])ret[1];<br>
 *&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;for (int idx = 0; idx < keys.length; idx++) {<br>
 *<br>
 *&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;// Key is displayed<br>
 *&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;System.out.println(keys[idx]);<br>
 *&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;}<br>
 *&nbsp;&nbsp;&nbsp;} else if (ret[0].equals("false")) {<br>
 *<br>
 *&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;// Data not found<br>
 *&nbsp;&nbsp;&nbsp;} else if (ret[0].equals("error")) {<br>
 *<br>
 *&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;// Server error<br>
 *&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;// ErrorMessage is displayed<br>
 *&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;System.out.println(ret[1]);<br>
 *&nbsp;&nbsp;&nbsp;}<br>
 *<br>
 *<br>
 *&nbsp;&nbsp;&nbsp;// --------------<br>
 *&nbsp;&nbsp;&nbsp;// Remove Data<br>
 *&nbsp;&nbsp;&nbsp;String[] ret = okuyamaClient.removeValue("datasavekey_001");<br>
 *&nbsp;&nbsp;&nbsp;if (ret[0].equals("true")) {<br>
 *<br>
 *&nbsp;&nbsp;&nbsp;&nbsp; // Remove success<br>
 *&nbsp;&nbsp;&nbsp;&nbsp; // Removed data is displayed<br>
 *&nbsp;&nbsp;&nbsp;&nbsp; System.out.println(ret[1]);<br>
 *&nbsp;&nbsp; } else if (ret[0].equals("false")) {<br>
 *&nbsp;&nbsp;<br>
 *&nbsp;&nbsp;&nbsp;&nbsp; // Data not found<br>
 *&nbsp;&nbsp; } else if (ret[0].equals("error")) {<br>
 *&nbsp;&nbsp; <br>
 *&nbsp;&nbsp;&nbsp;&nbsp; // Server error<br>
 *&nbsp;&nbsp;&nbsp;&nbsp; // ErrorMessage is displayed<br>
 *&nbsp;&nbsp;&nbsp;&nbsp; System.out.println(ret[1]);<br>
 *&nbsp;&nbsp; <br>
 *&nbsp;&nbsp; }<br>
 *<br>
 *<br>
 *&nbsp;&nbsp;&nbsp;// --------------<br>
 *&nbsp;&nbsp; // Add New Value<br>
 *&nbsp;&nbsp; String[] retParam = okuyamaClient.setNewValue("NewKey", "value999");<br>
 *&nbsp;&nbsp; if(retParam[0].equals("false")) {<br>
 *&nbsp;&nbsp;<br>
 *&nbsp;&nbsp;&nbsp;&nbsp; // Register Error<br>
 *&nbsp;&nbsp;&nbsp;&nbsp; System.out.println(retParam[1]);<br>
 *&nbsp;&nbsp; } else {<br>
 *&nbsp;&nbsp;<br>
 *&nbsp;&nbsp;&nbsp;&nbsp; System.out.println("Register Success");<br>
 *&nbsp;&nbsp; }<br>
 *<br>
 * } catch (OkuyamaClientException oce) {<br>
 *&nbsp;&nbsp; oce.printStackTrace();<br>
 * } finally {<br>
 *&nbsp;&nbsp; try {<br>
 *&nbsp;&nbsp;&nbsp;&nbsp; // --------------<br>
 *&nbsp;&nbsp;&nbsp;&nbsp; // Close<br>
 *&nbsp;&nbsp;&nbsp;&nbsp; okuyamaClient.close();<br>
 *&nbsp;&nbsp; } catch (OkuyamaClientException oce2) {<br>
 *&nbsp;&nbsp;&nbsp;&nbsp; oce2.printStackTrace();<br>
 *&nbsp;&nbsp;&nbsp;&nbsp; okuyamaClient = null;<br>
 *&nbsp;&nbsp; }<br>
 * }<br>
 *<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class OkuyamaClient {

    // 接続先情報
    protected ArrayList masterNodesList = null;
    // ソケット
    protected Socket socket = null;

    // 現在接続中のMasterServer
    protected String nowConnectServerInfo = "";

    // サーバへの出力用
    protected PrintWriter pw = null;

    // サーバからの受信用
    protected BufferedReader br = null;

    protected String transactionCode = "0";

    public static final String SEARCH_VALUE_TYPE_AND = "1";

    public static final String SEARCH_VALUE_TYPE_OR = "2";

    // データセパレータ文字列
    protected static final String sepStr = ImdstDefine.keyHelperClientParamSep;

    // 接続時のデフォルトのエンコーディング
    protected static final String connectDefaultEncoding = ImdstDefine.keyHelperClientParamEncoding;

    // ブランク文字列の代行文字列
    protected static final String blankStr = ImdstDefine.imdstBlankStrData;

    // 接続要求切断文字列
    protected static final String connectExitStr = ImdstDefine.imdstConnectExitRequest;

    // Tagで取得出来るキー値のセパレータ文字列
    protected static final String tagKeySep = ImdstDefine.imdstTagKeyAppendSep;

    protected static final String byteDataKeysSep = ":#:";

    // バイナリデータ分割保存サイズ
    protected int saveSize = ImdstDefine.saveDataMaxSize - 1;

    // 保存できる最大長
    protected int maxValueSize = ImdstDefine.saveDataMaxSize - 1;

    // 保存できる最大長
    protected int maxKeySize = new Double(ImdstDefine.saveKeyMaxSize * 0.7).intValue();

    // byteデータ送信時に圧縮を行うかを決定
    protected boolean compressMode = false;


    // setメソッド用リクエストBuffer
    protected StringBuilder setValueServerReqBuf = new StringBuilder(ImdstDefine.stringBufferMiddleSize);

    // getメソッド用リクエストBuffer
    protected StringBuilder getValueServerReqBuf = new StringBuilder(ImdstDefine.stringBufferSmallSize);


    private boolean sendSearchFlg = false;


    /**
     * コンストラクタ
     *
     */
    public OkuyamaClient() {
        // エンコーダ、デコーダの初期化に時間を使うようなので初期化
        this.dataEncoding("".getBytes());
        this.dataDecoding("".getBytes());
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
     * @throws OkuyamaClientException
     */
    public void autoConnect() throws OkuyamaClientException {
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
                this.pw = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), OkuyamaClient.connectDefaultEncoding)));
                this.br = new BufferedReader(new InputStreamReader(socket.getInputStream(), OkuyamaClient.connectDefaultEncoding));
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
                if(tmpMasterNodeList.size() < 1) throw new OkuyamaClientException(e);
            }
        }
    }


    /**
     * 接続処理.<br>
     * エンコーディング指定なし.<br>
     *
     * @param server サーバ名
     * @param port ポート番号
     * @throws OkuyamaClientException
     */
    public void connect(String server, int port) throws OkuyamaClientException {
        this.connect(server, port, OkuyamaClient.connectDefaultEncoding, ImdstDefine.clientConnectionOpenTimeout, ImdstDefine.clientConnectionTimeout);
    }


    /**
     * 接続処理.<br>
     * エンコーディング指定なし.<br>
     *
     * @param server サーバ名
     * @param port ポート番号
     * @throws OkuyamaClientException
     */
    public void connect(String server, int port, String encoding) throws OkuyamaClientException {
        this.connect(server, port, encoding, ImdstDefine.clientConnectionOpenTimeout, ImdstDefine.clientConnectionTimeout);
    }


    /**
     * 接続処理.<br>
     * エンコーディング指定有り.<br>
     *
     * @param server サーバ名
     * @param port ポート番号
     * @param encoding サーバとのストリームエンコーディング指定(デフォルトUTF-8)
     * @throws OkuyamaClientException
     */
    public void connect(String server, int port, String encoding, int openTimeout, int connectionTimeout) throws OkuyamaClientException {
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
            throw new OkuyamaClientException(e);
        }
    }


    /**
     * MasterNodeとの接続を切断.<br>
     *
     * @throws OkuyamaClientException
     */
    public void close() throws OkuyamaClientException {
        try {

            this.transactionCode = "0";
            if (this.pw != null) {
                try {

                    // 接続切断を通知
                    this.pw.println(connectExitStr);
                    this.pw.flush();
                } catch (Exception e2) {
                }
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
            throw new OkuyamaClientException(e);
        }
    }


    /**
     * Clientを初期化する.<br>
     * 今のところはValueの最大保存サイズの初期化のみ.<br>
     * ※本メソッドは内部で自動的に呼び出されるので特に呼び出す必要はない.<br>
     * ※あまりに長時間コネクションプールを行う場合に呼び出せば<br>
     *   その都度サイズを初期化できる.<br>
     * 
     * @return boolean true:開始成功 false:開始失敗
     * @throws OkuyamaClientException
     */
    public boolean initClient() throws OkuyamaClientException {
        boolean ret = false;
        String serverRetStr = null;
        String[] serverRet = null;

        StringBuilder serverRequestBuf = null;

        try {
            if (this.socket == null) throw new OkuyamaClientException("No ServerConnect!!");

            // 文字列バッファ初期化
            serverRequestBuf = new StringBuilder();

            // 処理番号連結
            serverRequestBuf.append("0");


            // サーバ送信
            pw.println(serverRequestBuf.toString());

            pw.flush();

            // サーバから結果受け取り
            serverRetStr = br.readLine();

            serverRet = serverRetStr.split(OkuyamaClient.sepStr);

            // 処理の妥当性確認
            if (serverRet[0].equals("0")) {
                if (serverRet[1].equals("true")) {

                    this.saveSize = (new Integer(serverRet[2]).intValue()) - 1;
                    this.maxValueSize = this.saveSize;
                    ret = true;
                } else {
                    ret = false;
                }
            } else {

                // 妥当性違反
                throw new OkuyamaClientException("Execute Violation of validity");
            }

        } catch (OkuyamaClientException ice) {

            throw ice;
        } catch (ConnectException ce) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.initClient();
                } catch (Exception e) {
                    throw new OkuyamaClientException(ce);
                }
            } else {
                throw new OkuyamaClientException(ce);
            }
        } catch (SocketException se) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.initClient();
                } catch (Exception e) {
                    throw new OkuyamaClientException(se);
                }
            } else {
                throw new OkuyamaClientException(se);
            }
        } catch (Throwable e) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.initClient();
                } catch (Exception ee) {
                    throw new OkuyamaClientException(e);
                }
            } else {
                throw new OkuyamaClientException(e);
            }
        }
        return ret;
    }


    /**
     * Transactionを開始する.<br>
     * データロック、ロックリリースを使用する場合は、<br>
     * 事前に呼び出す必要がある.<br>
     *
     * @return boolean true:開始成功 false:開始失敗
     * @throws OkuyamaClientException
     */
    public boolean startTransaction() throws OkuyamaClientException {
        boolean ret = false;
        String serverRetStr = null;
        String[] serverRet = null;

        StringBuilder serverRequestBuf = null;

        try {
            if (this.socket == null) throw new OkuyamaClientException("No ServerConnect!!");


            // 文字列バッファ初期化
            serverRequestBuf = new StringBuilder();


            // 処理番号連結
            serverRequestBuf.append("37");
            // セパレータ連結
            serverRequestBuf.append(OkuyamaClient.sepStr);


            // サーバ送信
            pw.println(serverRequestBuf.toString());
            pw.flush();

            // サーバから結果受け取り
            serverRetStr = br.readLine();

            serverRet = serverRetStr.split(OkuyamaClient.sepStr);

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
                throw new OkuyamaClientException("Execute Violation of validity");
            }
        } catch (OkuyamaClientException ice) {

            throw ice;
        } catch (ConnectException ce) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.startTransaction();
                } catch (Exception e) {
                    throw new OkuyamaClientException(ce);
                }
            } else {
                throw new OkuyamaClientException(ce);
            }
        } catch (SocketException se) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.startTransaction();
                } catch (Exception e) {
                    throw new OkuyamaClientException(se);
                }
            } else {
                throw new OkuyamaClientException(se);
            }
        } catch (Throwable e) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.startTransaction();
                } catch (Exception ee) {
                    throw new OkuyamaClientException(e);
                }
            } else {
                throw new OkuyamaClientException(e);
            }
        }
        return ret;
    }


    /**
     * Transactionを終了する.<br>
     * データロック、ロックリリースの使用を完了後に、<br>
     * 呼び出すことで、現在使用中のTransactionを終了できる<br>
     *
     */
    public void endTransaction() {
        this.transactionCode = "0";
    }


    /**
     * データのLockを依頼する.<br>
     * 本メソッドは、startTransactionメソッドを呼び出した場合のみ有効である<br>
     * 
     * @param keyStr ロック対象のKey値
     * @param lockingTime Lockを取得後、維持する時間(この時間を経過すると自動的にLockが解除される)(単位は秒)(0は無制限)
     * @param waitLockTime Lockを取得する場合に既に取得中の場合この時間はLock取得をリトライする(単位は秒)(0は1度取得を試みる)
     * @return String[] 要素1(Lock成否):"true" or "false"
     * @throws OkuyamaClientException
     */
    public String[] lockData(String keyStr, int lockingTime, int waitLockTime) throws OkuyamaClientException {
        String[] ret = new String[1]; 
        String serverRetStr = null;
        String[] serverRet = null;

        StringBuilder serverRequestBuf = null;

        try {
            if (this.socket == null) throw new OkuyamaClientException("No ServerConnect!!");

            if (this.transactionCode == null || this.transactionCode.equals("") || this.transactionCode.equals("0")) 
                throw new OkuyamaClientException("No Start Transaction!!");

            // エラーチェック
            // Keyに対する無指定チェック
            if (keyStr == null ||  keyStr.equals("")) {
                throw new OkuyamaClientException("The blank is not admitted on a key");
            }

            // 文字列バッファ初期化
            serverRequestBuf = new StringBuilder();


            // 処理番号連結
            serverRequestBuf.append("30");
            // セパレータ連結
            serverRequestBuf.append(OkuyamaClient.sepStr);

            // Key連結(Keyはデータ送信時には必ず文字列が必要)
            serverRequestBuf.append(new String(this.dataEncoding(keyStr.getBytes())));

            // セパレータ連結
            serverRequestBuf.append(OkuyamaClient.sepStr);
            // TransactionCode連結
            serverRequestBuf.append(this.transactionCode);
            // セパレータ連結
            serverRequestBuf.append(OkuyamaClient.sepStr);
            // lockingTime連結
            serverRequestBuf.append(new Integer(lockingTime).toString());
            // セパレータ連結
            serverRequestBuf.append(OkuyamaClient.sepStr);
            // waitLockTime連結
            serverRequestBuf.append(new Integer(waitLockTime).toString());


            // サーバ送信
            pw.println(serverRequestBuf.toString());
            pw.flush();

            // サーバから結果受け取り
            serverRetStr = br.readLine();

            serverRet = serverRetStr.split(OkuyamaClient.sepStr);

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
                throw new OkuyamaClientException("Execute Violation of validity");
            }
        } catch (OkuyamaClientException ice) {
            throw ice;
        } catch (ConnectException ce) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.lockData(keyStr, lockingTime, waitLockTime);
                } catch (Exception e) {
                    throw new OkuyamaClientException(ce);
                }
            } else {
                throw new OkuyamaClientException(ce);
            }
        } catch (SocketException se) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.lockData(keyStr, lockingTime, waitLockTime);
                } catch (Exception e) {
                    throw new OkuyamaClientException(se);
                }
            } else {
                throw new OkuyamaClientException(se);
            }
        } catch (Throwable e) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.lockData(keyStr, lockingTime, waitLockTime);
                } catch (Exception ee) {
                    throw new OkuyamaClientException(e);
                }
            } else {
                throw new OkuyamaClientException(e);
            }
        }
        return ret;
    }


    /**
     * データのLock解除を依頼する.<br>
     * 本メソッドは、startTransactionメソッドを呼び出した場合のみ有効である.<br>
     *
     * @param keyStr ロック対象Key値
     * @return String[] 要素1(Lock解除成否):"true" or "false"
     * @throws OkuyamaClientException
     */
    public String[] releaseLockData(String keyStr) throws OkuyamaClientException {
        String[] ret = new String[1]; 
        String serverRetStr = null;
        String[] serverRet = null;

        StringBuilder serverRequestBuf = null;

        try {
            if (this.socket == null) throw new OkuyamaClientException("No ServerConnect!!");

            if (this.transactionCode == null || this.transactionCode.equals("") || this.transactionCode.equals("0")) 
                throw new OkuyamaClientException("No Start Transaction!!");

            // エラーチェック
            // Keyに対する無指定チェック
            if (keyStr == null ||  keyStr.equals("")) {
                throw new OkuyamaClientException("The blank is not admitted on a key");
            }

            // 文字列バッファ初期化
            serverRequestBuf = new StringBuilder();


            // 処理番号連結
            serverRequestBuf.append("31");
            // セパレータ連結
            serverRequestBuf.append(OkuyamaClient.sepStr);

            // Key連結(Keyはデータ送信時には必ず文字列が必要)
            serverRequestBuf.append(new String(this.dataEncoding(keyStr.getBytes())));
            // セパレータ連結
            serverRequestBuf.append(OkuyamaClient.sepStr);

            // TransactionCode連結
            serverRequestBuf.append(this.transactionCode);

            // サーバ送信
            pw.println(serverRequestBuf.toString());
            pw.flush();

            // サーバから結果受け取り
            serverRetStr = br.readLine();

            serverRet = serverRetStr.split(OkuyamaClient.sepStr);

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
                throw new OkuyamaClientException("Execute Violation of validity");
            }
        } catch (OkuyamaClientException ice) {
            throw ice;
        } catch (ConnectException ce) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.releaseLockData(keyStr);
                } catch (Exception e) {
                    throw new OkuyamaClientException(ce);
                }
            } else {
                throw new OkuyamaClientException(ce);
            }
        } catch (SocketException se) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.releaseLockData(keyStr);
                } catch (Exception e) {
                    throw new OkuyamaClientException(se);
                }
            } else {
                throw new OkuyamaClientException(se);
            }
        } catch (Throwable e) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.releaseLockData(keyStr);
                } catch (Exception ee) {
                    throw new OkuyamaClientException(e);
                }
            } else {
                throw new OkuyamaClientException(e);
            }
        }
        return ret;
    }


    /**
     * トランザクションを開始している場合、自身のトランザクションを一意に表す
     * コードを返す.
     * このコードをsetNowTransactionCodeに渡すと、別クライアントのTransactionを引き継げる
     * !! 他クライアントの処理を横取ることが出来るため、使用を推奨しない !! 
     * 
     * @return String
     */
    protected String getNowTransactionCode() {
        return this.transactionCode;
    }


    /**
     * 他のクライアントが実施しているトランザクションコードを設定することで、
     * トランザクション処理を引き継ぐことが出来る。
     * !!! 他クライアントの処理を横取ることが出来るため、使用を推奨しない !!!
     * 
     * @return String
     */
    protected void setNowTransactionCode(String transactionCode) {
        this.transactionCode = transactionCode;
    }


    /**
     * MasterNodeへデータを登録要求する.<br>
     * Tagなし.<br>
     *
     * @param keyStr Key値
     * @param value value値
     * @return boolean 登録成否
     * @throws OkuyamaClientException
     */
    public boolean setValue(String keyStr, String value) throws OkuyamaClientException {
        return this.setValue(keyStr, null, value, null, null);
    }

    /**
     * MasterNodeへデータを登録要求する.<br>
     * 有効期限あり
     * Tagなし.<br>
     *
     * @param keyStr Key値
     * @param value value値
     * @param expireTime データの有効期限時間(単位/秒)
     * @return boolean 登録成否
     * @throws OkuyamaClientException
     */
    public boolean setValue(String keyStr, String value, Integer expireTime) throws OkuyamaClientException {
        return this.setValue(keyStr, null, value, null, expireTime);
    }


    /**
     * MasterNodeへデータを登録要求する.<br>
     * 有効期限なし<br>
     * Tagなし.<br>
     *
     * @param keyStr Key値
     * @param value value値
     * @param encode Valueを取得後クライアントに返す際の文字コード
     * @return boolean 登録成否
     * @throws OkuyamaClientException
     */
    public boolean setValue(String keyStr, String value, String encode) throws OkuyamaClientException {
        return this.setValue(keyStr, null, value, encode, null);
    }

    /**
     * MasterNodeへデータを登録要求する.<br>
     * 有効期限あり<br>
     * Tagなし.<br>
     *
     * @param keyStr Key値
     * @param value value値
     * @param encode Valueを取得後クライアントに返す際の文字コード
     * @param expireTime データの有効期限時間(単位/秒)
     * @return boolean 登録成否
     * @throws OkuyamaClientException
     */
    public boolean setValue(String keyStr, String value, String encode, Integer expireTime) throws OkuyamaClientException {
        return this.setValue(keyStr, null, value, encode, expireTime);
    }


    /**
     * MasterNodeへデータを登録要求する.<br>
     * 有効期限なし<br>
     * Tag有り.<br>
     *
     * @param keyStr Key値
     * @param tagStrs Tag値の配列 例){"tag1","tag2","tag3"}
     * @param value value値
     * @return boolean 登録成否
     * @throws OkuyamaClientException
     */
    public boolean setValue(String keyStr, String[] tagStrs, String value) throws OkuyamaClientException {
        return this.setValue(keyStr, tagStrs, value, null, null);
    }

    /**
     * MasterNodeへデータを登録要求する.<br>
     * 有効期限あり<br>
     * Tag有り.<br>
     *
     * @param keyStr Key値
     * @param tagStrs Tag値の配列 例){"tag1","tag2","tag3"}
     * @param value value値
     * @param expireTime データの有効期限時間(単位/秒)
     * @return boolean 登録成否
     * @throws OkuyamaClientException
     */
    public boolean setValue(String keyStr, String[] tagStrs, String value, Integer expireTime) throws OkuyamaClientException {
        return this.setValue(keyStr, tagStrs, value, null, expireTime);
    }


    /**
     * MasterNodeへデータを登録要求する.<br>
     * 有効期限なし<br>
     * Tag有り.<br>
     *
     * @param keyStr Key値
     * @param tagStrs Tag値の配列 例){"tag1","tag2","tag3"}
     * @param value value値
     * @return boolean 登録成否
     * @throws OkuyamaClientException
     */
    public boolean setValue(String keyStr, String[] tagStrs, String value, String encode) throws OkuyamaClientException {
        return this.setValue(keyStr, tagStrs, value, encode, null);
    }

    /**
     * MasterNodeへデータを登録要求する.<br>
     * 有効期限あり<br>
     * Tag有り.<br>
     *
     * @param keyStr Key値
     * @param tagStrs Tag値の配列 例){"tag1","tag2","tag3"}
     * @param value value値
     * @param encode Valueを取得後クライアントに返す際の文字コード
     * @param expireTime データの有効期限時間(単位/秒)
     * @return boolean 登録成否
     * @throws OkuyamaClientException
     */
    public boolean setValue(String keyStr, String[] tagStrs, String value, String encode, Integer expireTime) throws OkuyamaClientException {
        boolean ret = false; 
        String serverRetStr = null;
        String[] serverRet = null;
        String encodeValue = null;
        // 文字列バッファ初期化
        setValueServerReqBuf.delete(0, Integer.MAX_VALUE);
        try {
            // Byte Lenghtチェック
            if (tagStrs != null) {
                for (int i = 0; i < tagStrs.length; i++) {
                    if (encode == null) {
                        if (tagStrs[i].getBytes().length > maxValueSize) throw new OkuyamaClientException("Tag Max Size " + maxValueSize + " Byte");
                    } else {
                        if (tagStrs[i].getBytes(encode).length > maxValueSize) throw new OkuyamaClientException("Tag Max Size " + maxValueSize + " Byte");
                    }
                }
            }

            if (value != null)
                if(encode == null) {
                    if (value.getBytes().length > maxValueSize) 
                        throw new OkuyamaClientException("Save Value Max Size " + maxValueSize + " Byte");
                } else {
                    if (value.getBytes(encode).length > maxValueSize) 
                        throw new OkuyamaClientException("Save Value Max Size " + maxValueSize + " Byte");
                }
            if (this.socket == null) throw new OkuyamaClientException("No ServerConnect!!");

            // エラーチェック

            // Keyに対する無指定チェック
            if (keyStr == null ||  keyStr.trim().equals(""))
                throw new OkuyamaClientException("The blank is not admitted on a key");

            if (encode == null) {
                if (keyStr.getBytes().length > maxKeySize) throw new OkuyamaClientException("Save Key Max Size " + maxKeySize + " Byte");
            } else {
                if (keyStr.getBytes(encode).length > maxKeySize) throw new OkuyamaClientException("Save Key Max Size " + maxKeySize + " Byte");
            }

            // valueに対する無指定チェック(Valueはnullやブランクの場合は代行文字列に置き換える)
            if (value == null ||  value.equals("")) {
                encodeValue = OkuyamaClient.blankStr;
            } else {

                // ValueをBase64でエンコード

                if (encode == null) {
                    encodeValue = new String(this.dataEncoding(value.getBytes()));
                } else {
                    encodeValue = new String(this.dataEncoding(value.getBytes(encode)));
                }
            }


            // 処理番号連結
            setValueServerReqBuf.append("1");
            // セパレータ連結
            setValueServerReqBuf.append(OkuyamaClient.sepStr);


            // Key連結(Keyはデータ送信時には必ず文字列が必要)
            setValueServerReqBuf.append(new String(this.dataEncoding(keyStr.getBytes())));
            // セパレータ連結
            setValueServerReqBuf.append(OkuyamaClient.sepStr);


            // Tag連結
            // Tag指定の有無を調べる
            if (tagStrs == null || tagStrs.length < 1) {

                // ブランク規定文字列を連結
                setValueServerReqBuf.append(OkuyamaClient.blankStr);
            } else {

                // Tag数分連結
                if (encode == null) {
                    setValueServerReqBuf.append(new String(this.dataEncoding(tagStrs[0].getBytes())));
                } else {
                    setValueServerReqBuf.append(new String(this.dataEncoding(tagStrs[0].getBytes(encode))));
                }
                for (int i = 1; i < tagStrs.length; i++) {
                    setValueServerReqBuf.append(tagKeySep);
                    if (encode == null) {
                        setValueServerReqBuf.append(new String(this.dataEncoding(tagStrs[i].getBytes())));
                    } else {
                        setValueServerReqBuf.append(new String(this.dataEncoding(tagStrs[i].getBytes(encode))));
                    }
                }
            }

            // セパレータ連結
            setValueServerReqBuf.append(OkuyamaClient.sepStr);

            // TransactionCode連結
            setValueServerReqBuf.append(this.transactionCode);

            // セパレータ連結
            setValueServerReqBuf.append(OkuyamaClient.sepStr);

            // Value連結
            setValueServerReqBuf.append(encodeValue);

            // 有効期限あり
            if (expireTime != null) {
                // セパレータ連結
                setValueServerReqBuf.append(OkuyamaClient.sepStr);
                setValueServerReqBuf.append(expireTime);
                // セパレータ連結　最後に区切りを入れて送信データ終わりを知らせる
                setValueServerReqBuf.append(OkuyamaClient.sepStr);
            }
            // サーバ送信
            pw.println(setValueServerReqBuf.toString());
            pw.flush();

            // サーバから結果受け取り
            serverRetStr = br.readLine();
            serverRet = serverRetStr.split(OkuyamaClient.sepStr);

            // 処理の妥当性確認
            if (serverRet.length == 3 && serverRet[0].equals("1")) {
                if (serverRet[1].equals("true")) {

                    // 処理成功
                    ret = true;
                } else if (serverRet[1].equals("error")){

                    // 処理失敗(メッセージ格納)
                    throw new OkuyamaClientException(serverRet[2]);
                }
            } else {

                // 妥当性違反
                throw new OkuyamaClientException("Execute Violation of validity [" + serverRetStr + "]");
            }

        } catch (OkuyamaClientException ice) {
            throw ice;
        } catch (ConnectException ce) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.setValue(keyStr, tagStrs, value, encode, expireTime);
                } catch (Exception e) {
                    throw new OkuyamaClientException(ce);
                }
            } else {
                throw new OkuyamaClientException(ce);
            }
        } catch (SocketException se) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.setValue(keyStr, tagStrs, value, encode, expireTime);
                } catch (Exception e) {
                    throw new OkuyamaClientException(se);
                }
            } else {
                throw new OkuyamaClientException(se);
            }
        } catch (Throwable e) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.setValue(keyStr, tagStrs, value, encode, expireTime);
                } catch (Exception ee) {
                    throw new OkuyamaClientException(e);
                }
            } else {
                throw new OkuyamaClientException(e);
            }
        }
        return ret;
    }


    /**
     * MasterNodeへデータを登録要求する.<br>
     * 登録と同時にValueの検索Indexを作成する<br>
     * 検索Indexを作成するので通常のSetに比べて時間がかかる.<br>
     * 全文Indexが作成されるので、値は検索可能な文字を指定すること。例えばBASE64エンコードの値などの場合は<br>
     * 検索時も同様にエンコードした値で検索する必要がある.<br>
     * ※okuyamaは検索Index作成前に、同様のKey値で値が登録されている場合は、そのKey値で登録されているValue値の<br>
     * 検索インデックスを削除してから登録が行われる.<br>
     * Tagなし.<br>
     * Prefixなし.<br>
     *
     * @param keyStr Key値
     * @param value value値
     * @return boolean 登録成否
     * @throws OkuyamaClientException
     */
    public boolean setValueAndCreateIndex(String keyStr, String value) throws OkuyamaClientException {
        return this.setValueAndCreateIndex(keyStr, null, value, null , 3);
    }

    /**
     * MasterNodeへデータを登録要求する.<br>
     * 登録と同時にValueの検索Indexを作成する<br>
     * 検索Indexを作成するので通常のSetに比べて時間がかかる.<br>
     * 全文Indexが作成されるので、値は検索可能な文字を指定すること。例えばBASE64エンコードの値などの場合は<br>
     * 検索時も同様にエンコードした値で検索する必要がある.<br>
     * ※okuyamaは検索Index作成前に、同様のKey値で値が登録されている場合は、そのKey値で登録されているValue値の<br>
     * 検索インデックスを削除してから登録が行われる.<br>
     * Tagなし.<br>
     *
     * @param keyStr Key値
     * @param value value値
     * @param indexPrefix 作成する検索IndexをグルーピングするPrefix文字列.この値と同様の値を指定してsearchValueメソッドを呼び出すと、グループに限定して全文検索が可能となる. 最大は128文字
     * @return boolean 登録成否
     * @throws OkuyamaClientException
     */
    public boolean setValueAndCreateIndex(String keyStr, String value, String indexPrefix) throws OkuyamaClientException {
        return this.setValueAndCreateIndex(keyStr, null, value, indexPrefix, 3);
    }

    /**
     * MasterNodeへデータを登録要求する.<br>
     * 登録と同時にValueの検索Indexを作成する<br>
     * 検索Indexを作成するので通常のSetに比べて時間がかかる.<br>
     * 全文Indexが作成されるので、値は検索可能な文字を指定すること。例えばBASE64エンコードの値などの場合は<br>
     * 検索時も同様にエンコードした値で検索する必要がある.<br>
     * ※okuyamaは検索Index作成前に、同様のKey値で値が登録されている場合は、そのKey値で登録されているValue値の<br>
     * 検索インデックスを削除してから登録が行われる.<br>
     * Tagなし.<br>
     *
     * @param keyStr Key値
     * @param value value値
     * @param indexPrefix 作成する検索IndexをグルーピングするPrefix文字列.この値と同様の値を指定してsearchValueメソッドを呼び出すと、グループに限定して全文検索が可能となる. 最大は128文字
     * @param createIndexLen 作成するN-GramIndxのNの部分は指定(ヒストグラム以上のIndexを作成可能)
     * @return boolean 登録成否
     * @throws OkuyamaClientException
     */
    public boolean setValueAndCreateIndex(String keyStr, String value, String indexPrefix, int createIndexLen) throws OkuyamaClientException {
        return this.setValueAndCreateIndex(keyStr, null, value, indexPrefix, createIndexLen);
    }


    /**
     * MasterNodeへデータを登録要求する.<br>
     * 登録と同時にValueの検索Indexを作成する<br>
     * 検索Indexを作成するので通常のSetに比べて時間がかかる.<br>
     * 全文Indexが作成されるので、値は検索可能な文字を指定すること。例えばBASE64エンコードの値などの場合は<br>
     * 検索時も同様にエンコードした値で検索する必要がある.<br>
     * ※okuyamaは検索Index作成前に、同様のKey値で値が登録されている場合は、そのKey値で登録されているValue値の<br>
     * 検索インデックスを削除してから登録が行われる.<br>
     * Tagなし.<br>
     *
     * @param keyStr Key値
     * @param value value値
     * @param indexPrefix 作成する検索IndexをグルーピングするPrefix文字列.この値と同様の値を指定してsearchValueメソッドを呼び出すと、グループに限定して全文検索が可能となる. 最大は128文字
     * @param createIndexLen 作成するN-GramIndxのNの部分は指定(ヒストグラム以上のIndexを作成可能)
     * @param createIndexLenMin 作成するN-GramIndxの最小のNを指定する。例えば3を指定すると2文字までのIndexは作成しない
     * @return boolean 登録成否
     * @throws OkuyamaClientException
     */
    public boolean setValueAndCreateIndex(String keyStr, String value, String indexPrefix, int createIndexLen, int createIndexLenMin) throws OkuyamaClientException {
        return this.setValueAndCreateIndex(keyStr, null, value, indexPrefix, createIndexLen, createIndexLenMin);
    }

    /**
     * MasterNodeへデータを登録要求する.<br>
     * 登録と同時にValueの検索Indexを作成する<br>
     * 検索Indexを作成するので通常のSetに比べて時間がかかる.<br>
     * 全文Indexが作成されるので、値は検索可能な文字を指定すること。例えばBASE64エンコードの値などの場合は<br>
     * 検索時も同様にエンコードした値で検索する必要がある.<br>
     * ※okuyamaは検索Index作成前に、同様のKey値で値が登録されている場合は、そのKey値で登録されているValue値の<br>
     * 検索インデックスを削除してから登録が行われる.<br>
     * Tag有り.<br>
     *
     * @param keyStr Key値
     * @param tagStrs Tag値の配列 例){"tag1","tag2","tag3"}
     * @param value value値
     * @param indexPrefix 作成する検索IndexをグルーピングするPrefix文字列.この値と同様の値を指定してsearchValueメソッドを呼び出すと、グループに限定して全文検索が可能となる. 最大は128文字
     * @param createIndexLen 作成するN-GramIndxのNの部分は指定(ヒストグラム以上のIndexを作成可能)
     * @return boolean 登録成否
     * @throws OkuyamaClientException
     */
    public boolean setValueAndCreateIndex(String keyStr, String[] tagStrs, String value, String indexPrefix) throws OkuyamaClientException {
        return this.setValueAndCreateIndex(keyStr, tagStrs, value,  indexPrefix, 3, 1);

    }

    /**
     * MasterNodeへデータを登録要求する.<br>
     * 登録と同時にValueの検索Indexを作成する<br>
     * 検索Indexを作成するので通常のSetに比べて時間がかかる.<br>
     * 全文Indexが作成されるので、値は検索可能な文字を指定すること。例えばBASE64エンコードの値などの場合は<br>
     * 検索時も同様にエンコードした値で検索する必要がある.<br>
     * ※okuyamaは検索Index作成前に、同様のKey値で値が登録されている場合は、そのKey値で登録されているValue値の<br>
     * 検索インデックスを削除してから登録が行われる.<br>
     * Tag有り.<br>
     *
     * @param keyStr Key値
     * @param tagStrs Tag値の配列 例){"tag1","tag2","tag3"}
     * @param value value値
     * @param indexPrefix 作成する検索IndexをグルーピングするPrefix文字列.この値と同様の値を指定してsearchValueメソッドを呼び出すと、グループに限定して全文検索が可能となる. 最大は128文字
     * @param createIndexLen 作成するN-GramIndxのNの部分は指定(ヒストグラム以上のIndexを作成可能)
     * @return boolean 登録成否
     * @throws OkuyamaClientException
     */
    public boolean setValueAndCreateIndex(String keyStr, String[] tagStrs, String value, String indexPrefix, int createIndexLen) throws OkuyamaClientException {
        return this.setValueAndCreateIndex(keyStr, tagStrs, value,  indexPrefix, createIndexLen, 1);
    }

    /**
     * MasterNodeへデータを登録要求する.<br>
     * 登録と同時にValueの検索Indexを作成する<br>
     * 検索Indexを作成するので通常のSetに比べて時間がかかる.<br>
     * 全文Indexが作成されるので、値は検索可能な文字を指定すること。例えばBASE64エンコードの値などの場合は<br>
     * 検索時も同様にエンコードした値で検索する必要がある.<br>
     * ※okuyamaは検索Index作成前に、同様のKey値で値が登録されている場合は、そのKey値で登録されているValue値の<br>
     * 検索インデックスを削除してから登録が行われる.<br>
     * Tag有り.<br>
     *
     * @param keyStr Key値
     * @param tagStrs Tag値の配列 例){"tag1","tag2","tag3"}
     * @param value value値
     * @param indexPrefix 作成する検索IndexをグルーピングするPrefix文字列.この値と同様の値を指定してsearchValueメソッドを呼び出すと、グループに限定して全文検索が可能となる. 最大は128文字
     * @param createIndexLen 作成するN-GramIndxのNの部分は指定(ヒストグラム以上のIndexを作成可能)
     * @param createIndexLenMin 作成するN-GramIndxの最小のNを指定する。例えば3を指定すると2文字までのIndexは作成しない
     * @return boolean 登録成否
     * @throws OkuyamaClientException
     */
    public boolean setValueAndCreateIndex(String keyStr, String[] tagStrs, String value, String indexPrefix, int createIndexLen, int createIndexLenMin) throws OkuyamaClientException {
        boolean ret = false; 
        String serverRetStr = null;
        String[] serverRet = null;
        String encodeValue = null;

        // 文字列バッファ初期化
        setValueServerReqBuf.delete(0, Integer.MAX_VALUE);

        try {
            // Byte Lenghtチェック
            if (tagStrs != null) {
                for (int i = 0; i < tagStrs.length; i++) {
                    if (tagStrs[i].getBytes(ImdstDefine.characterDecodeSetBySearch).length > maxValueSize) throw new OkuyamaClientException("Tag Max Size " + maxValueSize + " Byte");
                }
            }

            if (value != null)
                if (value.getBytes(ImdstDefine.characterDecodeSetBySearch).length > maxValueSize) 
                    throw new OkuyamaClientException("Save Value Max Size " + maxValueSize + " Byte");

            if (this.socket == null) throw new OkuyamaClientException("No ServerConnect!!");

            // エラーチェック
            // Keyに対する無指定チェック
            if (keyStr == null ||  keyStr.trim().equals(""))
                throw new OkuyamaClientException("The blank is not admitted on a key");

            if (keyStr.getBytes(ImdstDefine.characterDecodeSetBySearch).length > maxKeySize) throw new OkuyamaClientException("Save Key Max Size " + maxKeySize + " Byte");

            // valueに対する無指定チェック(Valueはnullやブランクの場合は代行文字列に置き換える)
            if (value == null ||  value.equals("")) {
                encodeValue = OkuyamaClient.blankStr;
            } else {

                // ValueをBase64でエンコード
                encodeValue = new String(this.dataEncoding(value.getBytes(ImdstDefine.characterDecodeSetBySearch)), ImdstDefine.characterDecodeSetBySearch);
            }


            // 処理番号連結
            setValueServerReqBuf.append("42");
            // セパレータ連結
            setValueServerReqBuf.append(OkuyamaClient.sepStr);


            // Key連結(Keyはデータ送信時には必ず文字列が必要)
            setValueServerReqBuf.append(new String(this.dataEncoding(keyStr.getBytes(ImdstDefine.characterDecodeSetBySearch))));
            // セパレータ連結
            setValueServerReqBuf.append(OkuyamaClient.sepStr);


            // Tag連結
            // Tag指定の有無を調べる
            if (tagStrs == null || tagStrs.length < 1) {

                // ブランク規定文字列を連結
                setValueServerReqBuf.append(OkuyamaClient.blankStr);
            } else {

                // Tag数分連結
                setValueServerReqBuf.append(new String(this.dataEncoding(tagStrs[0].getBytes(ImdstDefine.characterDecodeSetBySearch))));
                for (int i = 1; i < tagStrs.length; i++) {
                    setValueServerReqBuf.append(tagKeySep);
                    setValueServerReqBuf.append(new String(this.dataEncoding(tagStrs[i].getBytes(ImdstDefine.characterDecodeSetBySearch))));
                }
            }

            // セパレータ連結
            setValueServerReqBuf.append(OkuyamaClient.sepStr);

            // TransactionCode連結
            setValueServerReqBuf.append(this.transactionCode);

            // セパレータ連結
            setValueServerReqBuf.append(OkuyamaClient.sepStr);

            // Value連結
            setValueServerReqBuf.append(encodeValue);

            // セパレータ連結
            setValueServerReqBuf.append(OkuyamaClient.sepStr);

            // Indexプレフィックス指定の有無を調べてIndexプレフィックス連結
            if (indexPrefix == null || indexPrefix.length() < 1) {
                // ブランク規定文字列を連結
                setValueServerReqBuf.append(OkuyamaClient.blankStr);
            } else {

                // Indexプレフィックス連結
                setValueServerReqBuf.append(new String(this.dataEncoding(indexPrefix.getBytes(ImdstDefine.characterDecodeSetBySearch))));
            }

            // セパレータ連結
            setValueServerReqBuf.append(OkuyamaClient.sepStr);

            // createIndexLen連結
            setValueServerReqBuf.append(createIndexLen);

            // セパレータ連結
            setValueServerReqBuf.append(OkuyamaClient.sepStr);

            // createIndexLenMin連結
            setValueServerReqBuf.append(createIndexLenMin);


            // サーバ送信
            pw.println(setValueServerReqBuf.toString());
            pw.flush();

            // サーバから結果受け取り
            serverRetStr = br.readLine();

            serverRet = serverRetStr.split(OkuyamaClient.sepStr);

            // 処理の妥当性確認
            if (serverRet.length == 3 && serverRet[0].equals("42")) {
                if (serverRet[1].equals("true")) {

                    // 処理成功
                    ret = true;
                } else if (serverRet[1].equals("error")){

                    // 処理失敗(メッセージ格納)
                    throw new OkuyamaClientException(serverRet[2]);
                }
            } else {

                // 妥当性違反
                throw new OkuyamaClientException("Execute Violation of validity [" + serverRetStr + "]");
            }

        } catch (OkuyamaClientException ice) {
            throw ice;
        } catch (ConnectException ce) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.setValueAndCreateIndex(keyStr, tagStrs, value, indexPrefix, createIndexLen);
                } catch (Exception e) {
                    throw new OkuyamaClientException(ce);
                }
            } else {
                throw new OkuyamaClientException(ce);
            }
        } catch (SocketException se) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.setValueAndCreateIndex(keyStr, tagStrs, value, indexPrefix, createIndexLen);
                } catch (Exception e) {
                    throw new OkuyamaClientException(se);
                }
            } else {
                throw new OkuyamaClientException(se);
            }
        } catch (Throwable e) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.setValueAndCreateIndex(keyStr, tagStrs, value, indexPrefix, createIndexLen);
                } catch (Exception ee) {
                    throw new OkuyamaClientException(e);
                }
            } else {
                throw new OkuyamaClientException(e);
            }
        }
        return ret;
    }


    /**
     * 検索用の辞書Indexを設定する.<br>
     * 設定した辞書文字列がIndex作成要求を出したValueに存在した場合は作成される<br>
     * 通常作成はN-Gramなので、ここでN-GramでのN文字以上の辞書文字列を設定すれば<br>
     * 検索時に高速に検索される.<br>
     * また検索時に辞書に存在する文字列を指定すれば高速に検索される.<br>
     * 設定する文字列のエンコードはUTF-8固定<br>
     *
     * @param dictionaryStrList
     * @return boolean true:登録成功 false:登録失敗
     * @throws OkuyamaClientException
     */
    public boolean setDictionaryCharacters(String[] characterList) throws OkuyamaClientException {
        boolean ret = false;
        String serverRetStr = null;
        String[] serverRet = null;

        StringBuilder serverRequestBuf = null;

        try {
            if (this.socket == null) throw new OkuyamaClientException("No ServerConnect!!");

            if (characterList == null || characterList.length < 1) {
                throw new OkuyamaClientException("No Characters");
            }


            StringBuilder dictionaryBuf = new StringBuilder(50);
            String dictionarySep = "";

            for (int i = 0; i < characterList.length; i++) {
                dictionaryBuf.append(dictionarySep);
                dictionaryBuf.append(new String(this.dataEncoding(characterList[i].getBytes("UTF-8"))));
                dictionarySep = "|";
            }

            // 文字列バッファ初期化
            serverRequestBuf = new StringBuilder();


            // 処理番号連結
            serverRequestBuf.append("50");
            // セパレータ連結
            serverRequestBuf.append(OkuyamaClient.sepStr);
            // 辞書文字列結合
            serverRequestBuf.append(dictionaryBuf.toString());

            // サーバ送信
            pw.println(serverRequestBuf.toString());
            pw.flush();

            // サーバから結果受け取り
            serverRetStr = br.readLine();

            serverRet = serverRetStr.split(OkuyamaClient.sepStr);

            // 処理の妥当性確認
            if (serverRet[0].equals("50")) {
                if (serverRet[1].equals("true")) {

                    ret = true;
                } else {
                    ret = false;
                }
            } else {

                // 妥当性違反
                throw new OkuyamaClientException("Execute Violation of validity");
            }
        } catch (OkuyamaClientException ice) {

            throw ice;
        } catch (ConnectException ce) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.setDictionaryCharacters(characterList);
                } catch (Exception e) {
                    throw new OkuyamaClientException(ce);
                }
            } else {
                throw new OkuyamaClientException(ce);
            }
        } catch (SocketException se) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.setDictionaryCharacters(characterList);
                } catch (Exception e) {
                    throw new OkuyamaClientException(se);
                }
            } else {
                throw new OkuyamaClientException(se);
            }
        } catch (Throwable e) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.setDictionaryCharacters(characterList);
                } catch (Exception ee) {
                    throw new OkuyamaClientException(e);
                }
            } else {
                throw new OkuyamaClientException(e);
            }
        }
        return ret;
    }



    /**
     * 検索用の辞書Indexを全てクリアする.<br>
     *
     * @param dictionaryStrList
     * @return boolean true:登録成功 false:登録失敗
     * @throws OkuyamaClientException
     */
    public boolean clearDictionaryCharacters() throws OkuyamaClientException {
        boolean ret = false;
        String serverRetStr = null;
        String[] serverRet = null;

        StringBuilder serverRequestBuf = null;

        try {
            if (this.socket == null) throw new OkuyamaClientException("No ServerConnect!!");


            // 文字列バッファ初期化
            serverRequestBuf = new StringBuilder();


            // 処理番号連結
            serverRequestBuf.append("50");
            // セパレータ連結
            serverRequestBuf.append(OkuyamaClient.sepStr);
            // 辞書文字列結合
            serverRequestBuf.append(OkuyamaClient.blankStr);

            // サーバ送信
            pw.println(serverRequestBuf.toString());
            pw.flush();

            // サーバから結果受け取り
            serverRetStr = br.readLine();

            serverRet = serverRetStr.split(OkuyamaClient.sepStr);

            // 処理の妥当性確認
            if (serverRet[0].equals("50")) {
                if (serverRet[1].equals("true")) {

                    ret = true;
                } else {
                    ret = false;
                }
            } else {

                // 妥当性違反
                throw new OkuyamaClientException("Execute Violation of validity");
            }
        } catch (OkuyamaClientException ice) {

            throw ice;
        } catch (ConnectException ce) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.clearDictionaryCharacters();
                } catch (Exception e) {
                    throw new OkuyamaClientException(ce);
                }
            } else {
                throw new OkuyamaClientException(ce);
            }
        } catch (SocketException se) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.clearDictionaryCharacters();
                } catch (Exception e) {
                    throw new OkuyamaClientException(se);
                }
            } else {
                throw new OkuyamaClientException(se);
            }
        } catch (Throwable e) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.clearDictionaryCharacters();
                } catch (Exception ee) {
                    throw new OkuyamaClientException(e);
                }
            } else {
                throw new OkuyamaClientException(e);
            }
        }
        return ret;
    }


    /**
     * MasterNodeへ新規データを登録要求する.<br>
     * Tagなし.<br>
     * 既にデータが同一のKeyで登録されている場合は失敗する.<br>
     * その場合は、falseが返る<br>
     * 成功の場合は配列の長さは1である。失敗時は2である<br>
     *
     * @param keyStr Key値
     * @param value Value値
     * @return String[] 要素1(データ有無):"true" or "false",要素2(失敗時はメッセージ):"メッセージ"
     * @throws OkuyamaClientException
     */
    public String[] setNewValue(String keyStr, String value) throws OkuyamaClientException {
        return this.setNewValue(keyStr, null, value);
    }


    /**
     * MasterNodeへ新規データを登録要求する.<br>
     * Tag有り.<br>
     * 既にデータが同一のKeyで登録されている場合は失敗する.<br>
     * その場合は、falseが返る<br>
     * 成功の場合は配列の長さは1である。失敗時は2である<br>
     * 
     * @param keyStr Key値
     * @param tagStrs Tag値 例){"tag1","tag2","tag3"}
     * @param value Value値
     * @return String[] 要素1(データ有無):"true" or "false",要素2(失敗時はメッセージ):"メッセージ"
     * @throws OkuyamaClientException
     */
    public String[] setNewValue(String keyStr, String[] tagStrs, String value) throws OkuyamaClientException {
        String[] ret = null; 
        String serverRetStr = null;
        String[] serverRet = null;
        String encodeValue = null;


        // 文字列バッファ初期化
        setValueServerReqBuf.delete(0, Integer.MAX_VALUE);

        try {
            if (this.socket == null) throw new OkuyamaClientException("No ServerConnect!!");

            // Byte Lenghtチェック
            if (tagStrs != null) {
                for (int i = 0; i < tagStrs.length; i++) {
                    if (tagStrs[i].getBytes().length > maxValueSize) throw new OkuyamaClientException("Tag Max Size " + maxValueSize + " Byte");
                }
            }

            if (value != null) 
                if (value.getBytes().length > maxValueSize) 
                    throw new OkuyamaClientException("Save Value Max Size " + maxValueSize + " Byte");

            // エラーチェック
            // Keyに対する無指定チェック
            if (keyStr == null ||  keyStr.trim().equals(""))
                throw new OkuyamaClientException("The blank is not admitted on a key");

            if (keyStr.getBytes().length > maxKeySize) throw new OkuyamaClientException("Save Key Max Size " + maxKeySize + " Byte");

            // valueに対する無指定チェック(Valueはnullやブランクの場合は代行文字列に置き換える)
            if (value == null ||  value.equals("")) {
                encodeValue = OkuyamaClient.blankStr;
            } else {

                // ValueをBase64でエンコード
                encodeValue = new String(this.dataEncoding(value.getBytes()));
            }


            // 処理番号連結
            setValueServerReqBuf.append("6");
            // セパレータ連結
            setValueServerReqBuf.append(OkuyamaClient.sepStr);


            // Key連結(Keyはデータ送信時には必ず文字列が必要)
            setValueServerReqBuf.append(new String(this.dataEncoding(keyStr.getBytes())));
            // セパレータ連結
            setValueServerReqBuf.append(OkuyamaClient.sepStr);


            // Tag連結
            // Tag指定の有無を調べる
            if (tagStrs == null || tagStrs.length < 1) {

                // ブランク規定文字列を連結
                setValueServerReqBuf.append(OkuyamaClient.blankStr);
            } else {

                // Tag数分連結
                setValueServerReqBuf.append(new String(this.dataEncoding(tagStrs[0].getBytes())));
                for (int i = 1; i < tagStrs.length; i++) {
                    setValueServerReqBuf.append(tagKeySep);
                    setValueServerReqBuf.append(new String(this.dataEncoding(tagStrs[i].getBytes())));
                }
            }

            // セパレータ連結
            setValueServerReqBuf.append(OkuyamaClient.sepStr);

            // TransactionCode連結
            setValueServerReqBuf.append(this.transactionCode);

            // セパレータ連結
            setValueServerReqBuf.append(OkuyamaClient.sepStr);

            // Value連結
            setValueServerReqBuf.append(encodeValue);

            // サーバ送信
            pw.println(setValueServerReqBuf.toString());
            pw.flush();

            // サーバから結果受け取り
            serverRetStr = br.readLine();
            serverRet = serverRetStr.split(OkuyamaClient.sepStr);

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
                throw new OkuyamaClientException("Execute Violation of validity [" + serverRetStr + "]");
            }
        } catch (OkuyamaClientException ice) {
            throw ice;
        } catch (ConnectException ce) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.setNewValue(keyStr, tagStrs, value);
                } catch (Exception e) {
                    throw new OkuyamaClientException(ce);
                }
            } else {
                throw new OkuyamaClientException(ce);
            }
        } catch (SocketException se) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.setNewValue(keyStr, tagStrs, value);
                } catch (Exception e) {
                    throw new OkuyamaClientException(se);
                }
            } else {
                throw new OkuyamaClientException(se);
            }
        } catch (Throwable e) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.setNewValue(keyStr, tagStrs, value);
                } catch (Exception ee) {
                    throw new OkuyamaClientException(e);
                }
            } else {
                throw new OkuyamaClientException(e);
            }
        }
        return ret;
    }


    /**
     * MasterNodeへバージョンチェック付き値登録要求をする.<br>
     * Tagなし.<br>
     * バージョン値を使用して更新前チェックを行う.<br>
     * 失敗した場合は、falseが返る<br>
     * 成功の場合は配列の長さは1である。失敗時は2である<br>
     * memcachedのcasに相当.<br>
     *
     * @param keyStr Key値
     * @param value Value値
     * @param versionNo getValueVersionCheckメソッドで取得したバージョンNo
     * @return String[] 要素1(データ有無):"true" or "false",要素2(失敗時はメッセージ):"メッセージ"
     * @throws OkuyamaClientException
     */
    public String[] setValueVersionCheck(String keyStr, String value, String versionNo) throws OkuyamaClientException {
        return this.setValueVersionCheck(keyStr, null, value, versionNo);
    }


    /**
     * MasterNodeへバージョンチェック付き値登録要求をする.<br>
     * Tag有り.<br>
     * バージョン値を使用して更新前チェックを行う.<br>
     * 失敗した場合は、falseが返る<br>
     * 成功の場合は配列の長さは1である。失敗時は2である<br>
     * memcachedのcasに相当.<br>
     * 
     * @param keyStr Key値
     * @param tagStrs Tag値
     * @param value Value値
     * @param versionNo getValueVersionCheckメソッドで取得したバージョンNo
     * @return String[] 要素1(データ有無):"true" or "false",要素2(失敗時はメッセージ):"メッセージ"
     * @throws OkuyamaClientException
     */
    public String[] setValueVersionCheck(String keyStr, String[] tagStrs, String value, String versionNo) throws OkuyamaClientException {
        String[] ret = null; 
        String serverRetStr = null;
        String[] serverRet = null;
        String encodeValue = null;
        // 文字列バッファ初期化
        setValueServerReqBuf.delete(0, Integer.MAX_VALUE);

        try {
            // エラーチェック
            if (this.socket == null) throw new OkuyamaClientException("No ServerConnect!!");

            // Byte Lenghtチェック
            if (tagStrs != null) {
                for (int i = 0; i < tagStrs.length; i++) {
                    if (tagStrs[i].getBytes().length > maxValueSize) throw new OkuyamaClientException("Tag Max Size " + maxValueSize + " Byte");
                }
            }

            if (value != null)
                if (value.getBytes().length > maxValueSize) 
                    throw new OkuyamaClientException("Save Value Max Size " + maxValueSize + " Byte");

            // Keyに対する無指定チェック
            if (keyStr == null ||  keyStr.trim().equals(""))
                throw new OkuyamaClientException("The blank is not admitted on a key");

            if (keyStr.getBytes().length > maxKeySize) throw new OkuyamaClientException("Save Key Max Size " + maxKeySize + " Byte");


            // valueに対する無指定チェック(Valueはnullやブランクの場合は代行文字列に置き換える)
            if (value == null ||  value.equals("")) {

                encodeValue = OkuyamaClient.blankStr;
            } else {

                // ValueをBase64でエンコード
                encodeValue = new String(this.dataEncoding(value.getBytes()));
            }


            // 処理番号連結
            setValueServerReqBuf.append("16");
            // セパレータ連結
            setValueServerReqBuf.append(OkuyamaClient.sepStr);

            // Key連結(Keyはデータ送信時には必ず文字列が必要)
            setValueServerReqBuf.append(new String(this.dataEncoding(keyStr.getBytes())));
            // セパレータ連結
            setValueServerReqBuf.append(OkuyamaClient.sepStr);


            // Tag連結
            // Tag指定の有無を調べる
            if (tagStrs == null || tagStrs.length < 1) {

                // ブランク規定文字列を連結
                setValueServerReqBuf.append(OkuyamaClient.blankStr);
            } else {

                // Tag数分連結
                setValueServerReqBuf.append(new String(this.dataEncoding(tagStrs[0].getBytes())));
                for (int i = 1; i < tagStrs.length; i++) {
                    setValueServerReqBuf.append(tagKeySep);
                    setValueServerReqBuf.append(new String(this.dataEncoding(tagStrs[i].getBytes())));
                }
            }

            // セパレータ連結
            setValueServerReqBuf.append(OkuyamaClient.sepStr);

            // TransactionCode連結
            setValueServerReqBuf.append(this.transactionCode);

            // セパレータ連結
            setValueServerReqBuf.append(OkuyamaClient.sepStr);

            // Value連結
            setValueServerReqBuf.append(encodeValue);

            // セパレータ連結
            setValueServerReqBuf.append(OkuyamaClient.sepStr);

            // バージョン値連結
            setValueServerReqBuf.append(versionNo);

            // サーバ送信
            pw.println(setValueServerReqBuf.toString());
            pw.flush();


            // サーバから結果受け取り
            serverRetStr = br.readLine();

            serverRet = serverRetStr.split(OkuyamaClient.sepStr);

            // 処理の妥当性確認
            if (serverRet != null && serverRet[0].equals("16")) {
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
                throw new OkuyamaClientException("Execute Violation of validity [" + serverRetStr + "]");
            }
        } catch (OkuyamaClientException ice) {
            throw ice;
        } catch (ConnectException ce) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.setValueVersionCheck(keyStr, tagStrs, value, versionNo);
                } catch (Exception e) {
                    throw new OkuyamaClientException(ce);
                }
            } else {
                throw new OkuyamaClientException(ce);
            }
        } catch (SocketException se) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.setValueVersionCheck(keyStr, tagStrs, value, versionNo);
                } catch (Exception e) {
                    throw new OkuyamaClientException(se);
                }
            } else {
                throw new OkuyamaClientException(se);
            }
        } catch (Throwable e) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.setValueVersionCheck(keyStr, tagStrs, value, versionNo);
                } catch (Exception ee) {
                    throw new OkuyamaClientException(e);
                }
            } else {
                throw new OkuyamaClientException(e);
            }
        }
        return ret;
    }


    /**
     * MasterNodeへデータを登録要求する(バイナリデータ).<br>
     * Tagなし.<br>
     *
     * @param keyStr Key値
     * @param values Value値(byte配列)
     * @return boolean 登録成否
     * @throws OkuyamaClientException
     */
    public boolean setByteValue(String keyStr, byte[] values) throws OkuyamaClientException {
        return this.setByteValue(keyStr, null, values);
    }


    /**
     * MasterNodeへデータを登録要求する(バイナリデータ).<br>
     * Tag有り.<br>
     * 処理の流れとしては、まずvalueを一定の容量で区切り、その単位で、<br>
     * Key値にプレフィックスを付けた値を作成し、かつ、特定のセパレータで連結して、<br>
     * 渡されたKeyを使用して連結文字を保存する<br>
     * 
     *
     * @param keyStr Key値
     * @param tagStrs Tag値
     * @param values Value値(byte配列)
     * @return boolean 登録成否
     * @throws OkuyamaClientException
     */
    public boolean setByteValue(String keyStr, String[] tagStrs, byte[] values) throws OkuyamaClientException {

        boolean ret = false;

        int bufSize = 0;
        int nowCounter = 0;

        byte[] workData = null;
        int counter = 0;
        int tmpKeyIndex = 0;
        String tmpKey = null;
        StringBuilder saveKeys = new StringBuilder(ImdstDefine.stringBufferLarge_3Size);
        String sep = "";

        String[] tmpKeys = null;
        int keyCount = values.length / this.saveSize;
        int much = values.length % this.saveSize;

        String firstKey = null;
        String endKey = null;
        try {

            // Byte Lenghtチェック
            if (keyStr == null ||  keyStr.trim().equals(""))
                throw new OkuyamaClientException("The blank is not admitted on a key");

            if (keyStr.getBytes().length > maxKeySize) throw new OkuyamaClientException("Save Key Max Size " + maxKeySize + " Byte");

            if (values == null || values.length == 0)
                throw new OkuyamaClientException("The blank is not admitted on a value");

            if (tagStrs != null) {
                for (int i = 0; i < tagStrs.length; i++) {
                    if (tagStrs[i].getBytes().length > maxValueSize) throw new OkuyamaClientException("Tag Max Size " + maxValueSize + " Byte");
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
                if(!this.sendByteData(tmpKey, workData)) throw new OkuyamaClientException("Byte Data Save Node Error");
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
            throw new OkuyamaClientException(e);
        }
        return ret;
    }


    /**
     * MasterNodeへデータを送信する(バイナリデータ).<br>
     *
     * @param keyStr
     * @param values
     * @return boolean
     * @throws OkuyamaClientException
     */
    private boolean sendByteData(String keyStr, byte[] values) throws OkuyamaClientException {
        boolean ret = false; 
        String serverRetStr = null;
        String[] serverRet = null;
        String value = null;
        StringBuilder serverRequestBuf = new StringBuilder(ImdstDefine.stringBufferLarge_3Size);

        String saveStr = null;

        try {

            // valuesがnullであることはない
            // Valueを圧縮し、Base64でエンコード
            value = new String(this.dataEncoding(this.execCompress(values)));

            // 処理番号連結
            serverRequestBuf.append("1");
            // セパレータ連結
            serverRequestBuf.append(OkuyamaClient.sepStr);

            // Key連結(Keyはデータ送信時には必ず文字列が必要)
            serverRequestBuf.append(new String(this.dataEncoding(keyStr.getBytes())));
            // セパレータ連結
            serverRequestBuf.append(OkuyamaClient.sepStr);

            // Tagは必ず存在しない
            // ブランク規定文字列を連結
            serverRequestBuf.append(OkuyamaClient.blankStr);

            // セパレータ連結
            serverRequestBuf.append(OkuyamaClient.sepStr);

            // TransactionCode連結
            serverRequestBuf.append(this.transactionCode);

            // セパレータ連結
            serverRequestBuf.append(OkuyamaClient.sepStr);

            // Value連結
            serverRequestBuf.append(value);

            // サーバ送信
            pw.println(serverRequestBuf.toString());
            pw.flush();

            // サーバから結果受け取り
            serverRetStr = br.readLine();

            serverRet = serverRetStr.split(OkuyamaClient.sepStr);

            // 処理の妥当性確認
            if (serverRet.length == 3 && serverRet[0].equals("1")) {
                if (serverRet[1].equals("true")) {

                    // 処理成功
                    ret = true;
                } else{

                    // 処理失敗(メッセージ格納)
                    throw new OkuyamaClientException(serverRet[2]);
                }
            } else {

                // 妥当性違反
                throw new OkuyamaClientException("Execute Violation of validity [" + serverRetStr + "]");
            }
        } catch (OkuyamaClientException ice) {
            throw ice;
        } catch (ConnectException ce) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.sendByteData(keyStr, values);
                } catch (Exception e) {
                    throw new OkuyamaClientException(ce);
                }
            } else {
                throw new OkuyamaClientException(ce);
            }
        } catch (SocketException se) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.sendByteData(keyStr, values);
                } catch (Exception e) {
                    throw new OkuyamaClientException(se);
                }
            } else {
                throw new OkuyamaClientException(se);
            }
        } catch (Throwable e) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.sendByteData(keyStr, values);
                } catch (Exception ee) {
                    throw new OkuyamaClientException(e);
                }
            } else {
                throw new OkuyamaClientException(e);
            }
        }
        return ret;
    }



    /**
     * MasterNodeへデータを送信する(バイナリデータ).<br>
     * setByteValueメソッドとの違いはValueをSplitしないで登録する部分.<br>
     *
     * @param keyStr Key値
     * @param values Value値
     * @return boolean 登録成否
     * @throws OkuyamaClientException
     */
    public boolean sendByteValue(String keyStr, byte[] values) throws OkuyamaClientException {
        boolean ret = false; 
        String serverRetStr = null;
        String[] serverRet = null;
        String value = null;
        StringBuilder serverRequestBuf = new StringBuilder(ImdstDefine.stringBufferLarge_3Size);

        String saveStr = null;

        try {
            if (this.socket == null) throw new OkuyamaClientException("No ServerConnect!!");

            // Byte Lenghtチェック
            if (keyStr == null ||  keyStr.trim().equals(""))
                throw new OkuyamaClientException("The blank is not admitted on a key");

            if (keyStr.getBytes().length > maxKeySize) throw new OkuyamaClientException("Save Key Max Size " + maxKeySize + " Byte");

            if (values == null || values.length == 0)
                throw new OkuyamaClientException("The blank is not admitted on a value");

            if (values.length > maxValueSize) 
                throw new OkuyamaClientException("Save Value Max Size " + maxValueSize + " Byte");


            // valuesがnullであることはない
            // ValueをBase64でエンコード
            value = new String(this.dataEncoding(values));

            // 処理番号連結
            serverRequestBuf.append("1");
            // セパレータ連結
            serverRequestBuf.append(OkuyamaClient.sepStr);

            // Key連結(Keyはデータ送信時には必ず文字列が必要)
            serverRequestBuf.append(new String(this.dataEncoding(keyStr.getBytes())));
            // セパレータ連結
            serverRequestBuf.append(OkuyamaClient.sepStr);

            // Tagは必ず存在しない
            // ブランク規定文字列を連結
            serverRequestBuf.append(OkuyamaClient.blankStr);

            // セパレータ連結
            serverRequestBuf.append(OkuyamaClient.sepStr);

            // TransactionCode連結
            serverRequestBuf.append(this.transactionCode);

            // セパレータ連結
            serverRequestBuf.append(OkuyamaClient.sepStr);

            // Value連結
            serverRequestBuf.append(value);

            // サーバ送信
            pw.println(serverRequestBuf.toString());
            pw.flush();

            // サーバから結果受け取り
            serverRetStr = br.readLine();

            serverRet = serverRetStr.split(OkuyamaClient.sepStr);

            // 処理の妥当性確認
            if (serverRet.length == 3 && serverRet[0].equals("1")) {
                if (serverRet[1].equals("true")) {

                    // 処理成功
                    ret = true;
                } else{

                    // 処理失敗(メッセージ格納)
                    throw new OkuyamaClientException(serverRet[2]);
                }
            } else {

                // 妥当性違反
                throw new OkuyamaClientException("Execute Violation of validity [" + serverRetStr + "]");
            }
        } catch (OkuyamaClientException ice) {
            throw ice;
        } catch (ConnectException ce) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.sendByteValue(keyStr, values);
                } catch (Exception e) {
                    throw new OkuyamaClientException(ce);
                }
            } else {
                throw new OkuyamaClientException(ce);
            }
        } catch (SocketException se) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.sendByteValue(keyStr, values);
                } catch (Exception e) {
                    throw new OkuyamaClientException(se);
                }
            } else {
                throw new OkuyamaClientException(se);
            }
        } catch (Throwable e) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.sendByteValue(keyStr, values);
                } catch (Exception ee) {
                    throw new OkuyamaClientException(e);
                }
            } else {
                throw new OkuyamaClientException(e);
            }
        }
        return ret;
    }



    /**
     * MasterNodeからKeyでValueを取得する.<br>
     * 文字列エンコーディング指定なし.<br>
     * デフォルトエンコーディングにて復元.<br>
     *
     * @param keyStr Key値
     * @return String[] 要素1(データ有無):"true" or "false", 要素2(データ):"データ文字列"
     * @throws OkuyamaClientException
     */
    public String[] getValue(String keyStr) throws OkuyamaClientException {
        return this.getValue(keyStr, null);
    }


    /**
     * MasterNodeからKeyでValueを取得する.<br>
     * 文字列エンコーディング指定あり.<br>
     *
     * @param keyStr Key値
     * @param encoding エンコーディング指定
     * @return String[] 要素1(データ有無):"true" or "false",要素2(データ):"データ文字列"
     * @throws OkuyamaClientException
     */
    public String[] getValue(String keyStr, String encoding) throws OkuyamaClientException {
        String[] ret = new String[2]; 
        String serverRetStr = null;
        String[] serverRet = null;

        // 文字列バッファ初期化
        getValueServerReqBuf.delete(0, Integer.MAX_VALUE);

        try {
            if (this.socket == null) throw new OkuyamaClientException("No ServerConnect!!");

            // エラーチェック
            // Keyに対する無指定チェック
            if (keyStr == null ||  keyStr.trim().equals("")) {
                throw new OkuyamaClientException("The blank is not admitted on a key");
            }

            // Keyに対するLengthチェック
            if (keyStr.getBytes().length > maxKeySize) throw new OkuyamaClientException("Save Key Max Size " + maxKeySize + " Byte");


            // 処理番号連結
            getValueServerReqBuf.append("2");
            // セパレータ連結
            getValueServerReqBuf.append(OkuyamaClient.sepStr);


            // Key連結(Keyはデータ送信時には必ず文字列が必要)
            getValueServerReqBuf.append(new String(this.dataEncoding(keyStr.getBytes())));


            // サーバ送信
            pw.println(getValueServerReqBuf.toString());

            pw.flush();

            // サーバから結果受け取り
            serverRetStr = br.readLine();

            serverRet = serverRetStr.split(OkuyamaClient.sepStr);

            // 処理の妥当性確認
            if (serverRet[0].equals("2")) {
                if (serverRet[1].equals("true")) {

                    // データ有り
                    ret[0] = serverRet[1];

                    // Valueがブランク文字か調べる
                    if (serverRet[2].equals(OkuyamaClient.blankStr)) {
                        ret[1] = "";
                    } else {

                        // Value文字列をBase64でデコード
                        if (encoding == null) {
                            ret[1] = new String(this.dataDecoding(serverRet[2].getBytes()));
                        } else {
                            ret[1] = new String(this.dataDecoding(serverRet[2].getBytes(encoding)), encoding);
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
                throw new OkuyamaClientException("Execute Violation of validity");
            }
        } catch (OkuyamaClientException ice) {
            throw ice;
        } catch (ConnectException ce) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.getValue(keyStr, encoding);
                } catch (Exception e) {
                    throw new OkuyamaClientException(ce);
                }
            } else {
                throw new OkuyamaClientException(ce);
            }
        } catch (SocketException se) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.getValue(keyStr, encoding);
                } catch (Exception e) {
                    throw new OkuyamaClientException(se);
                }
            } else {
                throw new OkuyamaClientException(se);
            }
        } catch (Throwable e) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.getValue(keyStr, encoding);
                } catch (Exception ee) {
                    throw new OkuyamaClientException(e);
                }
            } else {
                throw new OkuyamaClientException(e);
            }
        }
        return ret;
    }


    /**
     * MasterNodeからKey値の配列を渡すことでValue値の集合を取得する.<br>
     * 文字列エンコーディング指定なし.<br>
     * デフォルトエンコーディングにて復元.<br>
     *
     * @param keyStrList Key値配列
     * @return Map 取得データのMap 取得キーに同一の値を複数指定した場合は束ねられる Mapのキー値は指定されたKeyとなりValueは取得した値となる
     * @throws OkuyamaClientException
     */
    public Map getMultiValue(String[] keyStrList) throws OkuyamaClientException {
        return this.getMultiValue(keyStrList, null);
    }


    /**
     * MasterNodeからKey値の配列を渡すことでValue値の集合を取得する.<br>
     * 文字列エンコーディング指定あり.<br>
     *
     * @param keyStrList Key値配列
     * @param encoding エンコーディング指定
     * @return Map 取得データのMap 取得キーに同一の値を複数指定した場合は束ねられる Mapのキー値は指定されたKeyとなりValueは取得した値となる
     * @throws OkuyamaClientException
     */
    public Map getMultiValue(String[] keyStrList, String encoding) throws OkuyamaClientException {
        Map ret = new HashMap(); 
        String serverRetStr = null;
        String[] serverRet = null;

        // 文字列バッファ初期化
        getValueServerReqBuf.delete(0, Integer.MAX_VALUE);

        try {
            if (this.socket == null) throw new OkuyamaClientException("No ServerConnect!!");

            // エラーチェック
            // Keyに対する無指定チェック
            if (keyStrList == null ||  keyStrList.length == 1) {
                throw new OkuyamaClientException("The blank is not admitted on a key");
            }


            // 処理番号連結
            getValueServerReqBuf.append("22");
            // セパレータ連結
            getValueServerReqBuf.append(OkuyamaClient.sepStr);
            
            // Key連結(Keyはデータ送信時には必ず文字列が必要)
            // Key値をBase64Encodeして","で連結する
            String keysSep = "";
            ArrayList sendKeyList = new ArrayList();
            for (int idx = 0; idx < keyStrList.length; idx++){
                // ブランクは無視
                if (keyStrList[idx] != null && !keyStrList[idx].equals("")) {
                    // Keyに対するLengthチェック
                    if (keyStrList[idx].getBytes().length > maxKeySize) throw new OkuyamaClientException("Save Key Max Size " + maxKeySize + " Byte Key=[");

                    getValueServerReqBuf.append(keysSep);
                    getValueServerReqBuf.append(new String(this.dataEncoding(keyStrList[idx].getBytes())));
                    sendKeyList.add(keyStrList[idx]);
                    keysSep = OkuyamaClient.sepStr;
                }
            }
            // サーバ送信

            pw.println(getValueServerReqBuf.toString());
            pw.flush();

            // サーバから結果受け取り
            int readIdx = 0;


            while (!(serverRetStr = br.readLine()).equals(ImdstDefine.getMultiEndOfDataStr)) {

                serverRet = serverRetStr.split(OkuyamaClient.sepStr);
                // 処理の妥当性確認
                if (serverRet[0].equals("22")) {
                    if (serverRet[1].equals("true")) {
    
                        // データ有り
                        String[] oneDataRet = new String[2];
                        oneDataRet[0] = (String)sendKeyList.get(readIdx);
                        
                        // Valueがブランク文字か調べる
                        if (serverRet[2].equals(OkuyamaClient.blankStr)) {
                            oneDataRet[1] = "";
                        } else {
    
                            // Value文字列をBase64でデコード
                            if (encoding == null) {
                                oneDataRet[1] = new String(this.dataDecoding(serverRet[2].getBytes()));
                            } else {
                                oneDataRet[1] = new String(this.dataDecoding(serverRet[2].getBytes(encoding)), encoding);
                            }
                        }
                        ret.put(oneDataRet[0], oneDataRet[1]);
                    } else if(serverRet[1].equals("false")) {
    
                        // データなし
                        // 処理なし
                    } else if(serverRet[1].equals("error")) {
    
                        // エラー発生
                        // 処理なし
                    }
                } else {
    
                    // 妥当性違反
                    throw new OkuyamaClientException("Execute Violation of validity");
                }
                readIdx++;
            }

            if(ret.size() == 0) ret = null;
        } catch (OkuyamaClientException ice) {
            throw ice;
        } catch (ConnectException ce) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.getMultiValue(keyStrList, encoding);
                } catch (Exception e) {
                    throw new OkuyamaClientException(ce);
                }
            } else {
                throw new OkuyamaClientException(ce);
            }
        } catch (SocketException se) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.getMultiValue(keyStrList, encoding);
                } catch (Exception e) {
                    throw new OkuyamaClientException(se);
                }
            } else {
                throw new OkuyamaClientException(se);
            }
        } catch (Throwable e) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.getMultiValue(keyStrList, encoding);
                } catch (Exception ee) {
                    throw new OkuyamaClientException(e);
                }
            } else {
                e.printStackTrace();
                throw new OkuyamaClientException(e);
            }
        }
        return ret;
    }


    /**
     * MasterNodeからKeyでValueを取得する.<br>
     * 文字列エンコーディング指定なし.<br>
     * デフォルトエンコーディングにて復元.<br>
     * 取得と同時に値の有効期限を取得時から最初に設定した時間分延長更新<br>
     * 有効期限を設定していない場合は更新されない.<br>
     * Sessionキャッシュなどでアクセスした時間から所定時間有効などの場合にこちらのメソッドで<br>
     * 値を取得していれば自動的に有効期限が更新される<br>
     *
     * @param keyStr Key値
     * @return String[] 要素1(データ有無):"true" or "false", 要素2(データ):"データ文字列"
     * @throws OkuyamaClientException
     */
    public String[] getValueAndUpdateExpireTime(String keyStr) throws OkuyamaClientException {
        return this.getValueAndUpdateExpireTime(keyStr, null);
    }


    /**
     * MasterNodeからKeyでValueを取得する.<br>
     * 文字列エンコーディング指定あり.<br>
     * 取得と同時に値の有効期限を取得時から最初に設定した時間分延長更新<br>
     * 有効期限を設定していない場合は更新されない.<br>
     * Sessionキャッシュなどでアクセスした時間から所定時間有効などの場合にこちらのメソッドで<br>
     * 値を取得していれば自動的に有効期限が更新される<br>
     *
     * @param keyStr Key値
     * @param encoding エンコーディング指定
     * @return String[] 要素1(データ有無):"true" or "false",要素2(データ):"データ文字列"
     * @throws OkuyamaClientException
     */
    public String[] getValueAndUpdateExpireTime(String keyStr, String encoding) throws OkuyamaClientException {
        String[] ret = new String[2]; 
        String serverRetStr = null;
        String[] serverRet = null;

        // 文字列バッファ初期化
        getValueServerReqBuf.delete(0, Integer.MAX_VALUE);

        try {
            if (this.socket == null) throw new OkuyamaClientException("No ServerConnect!!");

            // エラーチェック
            // Keyに対する無指定チェック
            if (keyStr == null ||  keyStr.trim().equals("")) {
                throw new OkuyamaClientException("The blank is not admitted on a key");
            }

            // Keyに対するLengthチェック
            if (keyStr.getBytes().length > maxKeySize) throw new OkuyamaClientException("Save Key Max Size " + maxKeySize + " Byte");


            // 処理番号連結
            getValueServerReqBuf.append("17");
            // セパレータ連結
            getValueServerReqBuf.append(OkuyamaClient.sepStr);


            // Key連結(Keyはデータ送信時には必ず文字列が必要)
            getValueServerReqBuf.append(new String(this.dataEncoding(keyStr.getBytes())));


            // サーバ送信
            pw.println(getValueServerReqBuf.toString());

            pw.flush();

            // サーバから結果受け取り
            serverRetStr = br.readLine();

            serverRet = serverRetStr.split(OkuyamaClient.sepStr);

            // 処理の妥当性確認
            if (serverRet[0].equals("17")) {
                if (serverRet[1].equals("true")) {

                    // データ有り
                    ret[0] = serverRet[1];

                    // Valueがブランク文字か調べる
                    if (serverRet[2].equals(OkuyamaClient.blankStr)) {
                        ret[1] = "";
                    } else {

                        // Value文字列をBase64でデコード
                        if (encoding == null) {
                            ret[1] = new String(this.dataDecoding(serverRet[2].getBytes()));
                        } else {
                            ret[1] = new String(this.dataDecoding(serverRet[2].getBytes(encoding)), encoding);
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
                throw new OkuyamaClientException("Execute Violation of validity");
            }
        } catch (OkuyamaClientException ice) {
            throw ice;
        } catch (ConnectException ce) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.getValueAndUpdateExpireTime(keyStr, encoding);
                } catch (Exception e) {
                    throw new OkuyamaClientException(ce);
                }
            } else {
                throw new OkuyamaClientException(ce);
            }
        } catch (SocketException se) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.getValueAndUpdateExpireTime(keyStr, encoding);
                } catch (Exception e) {
                    throw new OkuyamaClientException(se);
                }
            } else {
                throw new OkuyamaClientException(se);
            }
        } catch (Throwable e) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.getValueAndUpdateExpireTime(keyStr, encoding);
                } catch (Exception ee) {
                    throw new OkuyamaClientException(e);
                }
            } else {
                throw new OkuyamaClientException(e);
            }
        }
        return ret;
    }


    /**
     * MasterNodeからTag値を渡すことで紐付くValue値の集合を取得する.<br>
     * 文字列エンコーディング指定なし.<br>
     * デフォルトエンコーディングにて復元.<br>
     *
     * @param tagStr Tag値
     * @return Map 取得データのMap 取得キーに同一の値を複数指定した場合は束ねられる Mapのキー値は指定されたKeyとなりValueは取得した値となる
     * @throws OkuyamaClientException
     */
    public Map getTagValues(String tagStr) throws OkuyamaClientException {
        return this.getTagValues(tagStr, null);
    }


    /**
     * MasterNodeからTag値を渡すことで紐付くValue値の集合を取得する.<br>
     * 文字列エンコーディング指定あり.<br>
     *
     * @param tagStr Tag値
     * @param encoding エンコーディング指定
     * @return Map 取得データのMap 取得キーに同一の値を複数指定した場合は束ねられる Mapのキー値は指定されたKeyとなりValueは取得した値となる
     * @throws OkuyamaClientException
     */
    public Map getTagValues(String tagStr, String encoding) throws OkuyamaClientException {
        Map ret = new HashMap(); 
        String serverRetStr = null;
        String[] serverRet = null;

        // 文字列バッファ初期化
        getValueServerReqBuf.delete(0, Integer.MAX_VALUE);

        try {
            if (this.socket == null) throw new OkuyamaClientException("No ServerConnect!!");

            // エラーチェック
            // Tagに対する無指定チェック
            if (tagStr == null ||  tagStr.equals("")) {
                throw new OkuyamaClientException("The blank is not admitted on a tag");
            }

            // Tagに対するLengthチェック
            if (tagStr.getBytes().length > maxKeySize) throw new OkuyamaClientException("Save Tag Max Size " + maxKeySize + " Byte");


            // 処理番号連結
            getValueServerReqBuf.append("23");
            // セパレータ連結
            getValueServerReqBuf.append(OkuyamaClient.sepStr);

            // tag値連結(Keyはデータ送信時には必ず文字列が必要)
            getValueServerReqBuf.append(new String(this.dataEncoding(tagStr.getBytes())));

            // サーバ送信

            pw.println(getValueServerReqBuf.toString());
            pw.flush();

            // サーバから結果受け取り
            int readIdx = 0;
            while (!(serverRetStr = br.readLine()).equals(ImdstDefine.getMultiEndOfDataStr)) {

                serverRet = serverRetStr.split(OkuyamaClient.sepStr);
                // 処理の妥当性確認
                if (serverRet[0].equals("23")) {
                    if (serverRet[1].equals("true")) {
    
                        // データ有り
                        String[] oneDataRet = new String[2];

                        if (encoding == null) {
                            oneDataRet[0] = new String(this.dataDecoding(serverRet[2].getBytes()));
                        } else {
                            oneDataRet[0] = new String(this.dataDecoding(serverRet[2].getBytes()), encoding);
                        }

                        // Valueがブランク文字か調べる
                        if (serverRet[3].equals(OkuyamaClient.blankStr)) {
                            oneDataRet[1] = "";
                        } else {
    
                            // Value文字列をBase64でデコード
                            if (encoding == null) {
                                oneDataRet[1] = new String(this.dataDecoding(serverRet[3].getBytes()));
                            } else {
                                oneDataRet[1] = new String(this.dataDecoding(serverRet[3].getBytes()), encoding);
                            }
                        }
                        ret.put(oneDataRet[0], oneDataRet[1]);
                    } else if(serverRet[1].equals("false")) {
    
                        // データなし
                        // 処理なし
                    } else if(serverRet[1].equals("error")) {
    
                        // エラー発生
                        // 処理なし
                    }
                } else {
    
                    // 妥当性違反
                    throw new OkuyamaClientException("Execute Violation of validity");
                }
                readIdx++;
            }

            if(ret.size() == 0) ret = null;
        } catch (OkuyamaClientException ice) {
            throw ice;
        } catch (ConnectException ce) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.getTagValues(tagStr, encoding);
                } catch (Exception e) {
                    throw new OkuyamaClientException(ce);
                }
            } else {
                throw new OkuyamaClientException(ce);
            }
        } catch (SocketException se) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.getTagValues(tagStr, encoding);
                } catch (Exception e) {
                    throw new OkuyamaClientException(se);
                }
            } else {
                throw new OkuyamaClientException(se);
            }
        } catch (Throwable e) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.getTagValues(tagStr, encoding);
                } catch (Exception ee) {
                    throw new OkuyamaClientException(e);
                }
            } else {
                throw new OkuyamaClientException(e);
            }
        }
        return ret;
    }


    /**
     * MasterNodeからKeyでValueを取得する.<br>
     * 文字列エンコーディング指定なし.<br>
     * デフォルトエンコーディングにて復元.<br>
     * バージョン情報(memcachedでのcasユニーク値)を返す.<br>
     * memcachedのgetsに相当.<br>
     *
     * @param keyStr Key値
     * @return String[] 要素1(データ有無):"true" or "false",要素2(データ):"データ文字列",要素3(VersionNo):"0始まりの数値"
     * @throws OkuyamaClientException
     */
    public String[] getValueVersionCheck(String keyStr) throws OkuyamaClientException {
        return this.getValueVersionCheck(keyStr, null);
    }


    /**
     * MasterNodeからKeyでValueを取得する.<br>
     * 文字列エンコーディング指定あり.<br>
     * バージョン情報(memcachedでのcasユニーク値)を返す.<br>
     * memcachedのgetsに相当.<br>
     *
     * @param keyStr Key値
     * @param encoding
     * @return String[] 要素1(データ有無):"true" or "false",要素2(データ):"データ文字列",要素3(VersionNo):"0始まりの数値"
     * @throws OkuyamaClientException
     */
    public String[] getValueVersionCheck(String keyStr, String encoding) throws OkuyamaClientException {
        String[] ret = new String[3]; 
        String serverRetStr = null;
        String[] serverRet = null;

        // 文字列バッファ初期化
        getValueServerReqBuf.delete(0, Integer.MAX_VALUE);

        try {
            if (this.socket == null) throw new OkuyamaClientException("No ServerConnect!!");

            // エラーチェック
            // Keyに対する無指定チェック
            if (keyStr == null ||  keyStr.trim().equals("")) {
                throw new OkuyamaClientException("The blank is not admitted on a key");
            }

            // Keyに対するLengthチェック
            if (keyStr.getBytes().length > maxKeySize) throw new OkuyamaClientException("Save Key Max Size " + keyStr + " Byte");


            // 処理番号連結
            getValueServerReqBuf.append("15");
            // セパレータ連結
            getValueServerReqBuf.append(OkuyamaClient.sepStr);

            // Key連結(Keyはデータ送信時には必ず文字列が必要)
            getValueServerReqBuf.append(new String(this.dataEncoding(keyStr.getBytes())));


            // サーバ送信
            pw.println(getValueServerReqBuf.toString());


            pw.flush();

            // サーバから結果受け取り
            serverRetStr = br.readLine();

            serverRet = serverRetStr.split(OkuyamaClient.sepStr);

            // 処理の妥当性確認
            if (serverRet[0].equals("15")) {
                if (serverRet[1].equals("true")) {

                    // データ有り
                    ret[0] = serverRet[1];

                    // Valueがブランク文字か調べる
                    if (serverRet[2].equals(OkuyamaClient.blankStr)) {
                        ret[1] = "";
                    } else {

                        // Value文字列をBase64でデコード
                        if (encoding == null) {
                            ret[1] = new String(this.dataDecoding(serverRet[2].getBytes()));
                        } else {
                            ret[1] = new String(this.dataDecoding(serverRet[2].getBytes()), encoding);
                        }
                    }

                    if (serverRet.length > 2)
                        ret[2] = serverRet[3];

                } else if(serverRet[1].equals("false")) {

                    // データなし
                    ret[0] = serverRet[1];
                    ret[1] = null;
                    ret[2] = null;
                } else if(serverRet[1].equals("error")) {

                    // エラー発生
                    ret[0] = serverRet[1];
                    ret[1] = serverRet[2];

                    if (serverRet.length > 3)
                        ret[2] = serverRet[3];
                }
            } else {

                // 妥当性違反
                throw new OkuyamaClientException("Execute Violation of validity");
            }
        } catch (OkuyamaClientException ice) {
            throw ice;
        } catch (ConnectException ce) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.getValueVersionCheck(keyStr, encoding);
                } catch (Exception e) {
                    throw new OkuyamaClientException(ce);
                }
            } else {
                throw new OkuyamaClientException(ce);
            }
        } catch (SocketException se) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.getValueVersionCheck(keyStr, encoding);
                } catch (Exception e) {
                    throw new OkuyamaClientException(se);
                }
            } else {
                throw new OkuyamaClientException(se);
            }
        } catch (Throwable e) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.getValueVersionCheck(keyStr, encoding);
                } catch (Exception ee) {
                    throw new OkuyamaClientException(e);
                }
            } else {
                throw new OkuyamaClientException(e);
            }
        }
        return ret;
    }


    /**
     * MasterNodeへデータの加算を要求する.<br>
     *
     * @param keyStr Key値
     * @param value 加算値
     * @return Object[] 要素1(処理成否):Boolean true/false,要素2(演算後の結果):Long 数値
     * @throws OkuyamaClientException
     */
    public Object[] incrValue(String keyStr, long value) throws OkuyamaClientException {
        Object[] ret = new Object[2]; 
        String serverRetStr = null;
        String[] serverRet = null;
        String valueStr = null;

        // 文字列バッファ初期化

        StringBuilder incrValueServerReqBuf = new StringBuilder();

        try {
            // Byte Lenghtチェック
            if (this.socket == null) throw new OkuyamaClientException("No ServerConnect!!");

            // エラーチェック
            // Keyに対する無指定チェック
            if (keyStr == null ||  keyStr.trim().equals(""))
                throw new OkuyamaClientException("The blank is not admitted on a key");

            if (keyStr.getBytes().length > maxKeySize) throw new OkuyamaClientException("Save Key Max Size " + maxKeySize + " Byte");

            // ValueをBase64でエンコード
            valueStr = new String(this.dataEncoding(new Long(value).toString().getBytes()));


            // 処理番号連結
            incrValueServerReqBuf.append("13");
            // セパレータ連結
            incrValueServerReqBuf.append(OkuyamaClient.sepStr);


            // Key連結(Keyはデータ送信時には必ず文字列が必要)
            incrValueServerReqBuf.append(new String(this.dataEncoding(keyStr.getBytes())));
            // セパレータ連結
            incrValueServerReqBuf.append(OkuyamaClient.sepStr);

            // TransactionCode連結
            incrValueServerReqBuf.append(this.transactionCode);
            // セパレータ連結
            incrValueServerReqBuf.append(OkuyamaClient.sepStr);

            // Value連結
            incrValueServerReqBuf.append(valueStr);

            // サーバ送信

            pw.println(incrValueServerReqBuf.toString());
            pw.flush();

            // サーバから結果受け取り
            serverRetStr = br.readLine();
            serverRet = serverRetStr.split(OkuyamaClient.sepStr);

            // 処理の妥当性確認
            if (serverRet.length == 3 && serverRet[0].equals("13")) {
                if (serverRet[1].equals("true")) {

                    // 処理成功
                    ret[0] = new Boolean(true);
                    ret[1] = new Long(new String(BASE64DecoderStream.decode(serverRet[2].getBytes())));
                } else if(serverRet[1].equals("false")) {

                    // データなし
                    ret[0] = new Boolean(false);
                    ret[1] = null;
                } else if (serverRet[1].equals("error")){

                    // 処理失敗(メッセージ格納)
                    throw new OkuyamaClientException(serverRet[2]);
                }
            } else {

                // 妥当性違反
                throw new OkuyamaClientException("Execute Violation of validity [" + serverRetStr + "]");
            }

        } catch (OkuyamaClientException ice) {
            throw ice;
        } catch (ConnectException ce) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.incrValue(keyStr, value);
                } catch (Exception e) {
                    throw new OkuyamaClientException(ce);
                }
            } else {
                throw new OkuyamaClientException(ce);
            }
        } catch (SocketException se) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.incrValue(keyStr, value);
                } catch (Exception e) {
                    throw new OkuyamaClientException(se);
                }
            } else {
                throw new OkuyamaClientException(se);
            }
        } catch (Throwable e) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.incrValue(keyStr, value);
                } catch (Exception ee) {
                    throw new OkuyamaClientException(e);
                }
            } else {
                throw new OkuyamaClientException(e);
            }
        }
        return ret;
    }


    /**
     * MasterNodeへデータの減算を要求する.<br>
     *
     * @param keyStr Key値
     * @param value 減算値
     * @return Object[] 要素1(処理成否):Boolean true/false,要素2(演算後の結果):Long 数値
     * @throws OkuyamaClientException
     */
    public Object[] decrValue(String keyStr, long value) throws OkuyamaClientException {
        Object[] ret = new Object[2]; 
        String serverRetStr = null;
        String[] serverRet = null;
        String valueStr = null;

        // 文字列バッファ初期化

        StringBuilder decrValueServerReqBuf = new StringBuilder();

        try {
            // Byte Lenghtチェック
            if (this.socket == null) throw new OkuyamaClientException("No ServerConnect!!");

            // エラーチェック
            // Keyに対する無指定チェック
            if (keyStr == null ||  keyStr.trim().equals(""))
                throw new OkuyamaClientException("The blank is not admitted on a key");

            if (keyStr.getBytes().length > maxKeySize) throw new OkuyamaClientException("Save Key Max Size " + maxKeySize + " Byte");

            // ValueをBase64でエンコード
            valueStr = new String(this.dataEncoding(new Long(value).toString().getBytes()));


            // 処理番号連結
            decrValueServerReqBuf.append("14");
            // セパレータ連結
            decrValueServerReqBuf.append(OkuyamaClient.sepStr);


            // Key連結(Keyはデータ送信時には必ず文字列が必要)
            decrValueServerReqBuf.append(new String(this.dataEncoding(keyStr.getBytes())));
            // セパレータ連結
            decrValueServerReqBuf.append(OkuyamaClient.sepStr);

            // TransactionCode連結
            decrValueServerReqBuf.append(this.transactionCode);
            // セパレータ連結
            decrValueServerReqBuf.append(OkuyamaClient.sepStr);

            // Value連結
            decrValueServerReqBuf.append(valueStr);

            // サーバ送信
            pw.println(decrValueServerReqBuf.toString());
            pw.flush();

            // サーバから結果受け取り
            serverRetStr = br.readLine();
            serverRet = serverRetStr.split(OkuyamaClient.sepStr);


            // 処理の妥当性確認
            if (serverRet.length == 3 && serverRet[0].equals("14")) {
                if (serverRet[1].equals("true")) {

                    // 処理成功
                    ret[0] = new Boolean(true);
                    ret[1] = new Long(new String(BASE64DecoderStream.decode(serverRet[2].getBytes())));
                } else if(serverRet[1].equals("false")) {

                    // データなし
                    ret[0] = new Boolean(false);
                    ret[1] = null;
                } else if (serverRet[1].equals("error")){

                    // 処理失敗(メッセージ格納)
                    throw new OkuyamaClientException(serverRet[2]);
                }
            } else {

                // 妥当性違反
                throw new OkuyamaClientException("Execute Violation of validity [" + serverRetStr + "]");
            }

        } catch (OkuyamaClientException ice) {
            throw ice;
        } catch (ConnectException ce) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.decrValue(keyStr, value);
                } catch (Exception e) {
                    throw new OkuyamaClientException(ce);
                }
            } else {
                throw new OkuyamaClientException(ce);
            }
        } catch (SocketException se) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.decrValue(keyStr, value);
                } catch (Exception e) {
                    throw new OkuyamaClientException(se);
                }
            } else {
                throw new OkuyamaClientException(se);
            }
        } catch (Throwable e) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.decrValue(keyStr, value);
                } catch (Exception ee) {
                    throw new OkuyamaClientException(e);
                }
            } else {
                throw new OkuyamaClientException(e);
            }
        }
        return ret;
    }


    /**
     * MasterNodeからKeyでValueを取得する.<br>
     * Scriptを同時に実行する.<br>
     * 文字列エンコーディング指定なし.<br>
     *
     * @param keyStr キー値
     * @param scriptStr スクリプト文
     * @return String[] 要素1(データ、エラー有無):"true" or "false" or "error", 要素2(データorエラー内容):"文字列"
     * @throws OkuyamaClientException
     */
    public String[] getValueScript(String keyStr, String scriptStr) throws OkuyamaClientException {
        return getValueScript(keyStr, scriptStr, null);
        
    }


    /**
     * MasterNodeからKeyでデータを取得する.<br>
     * Scriptを同時に実行する.<br>
     * 文字エンコーディング指定あり.<br>
     *
     * @param keyStr キー値
     * @param scriptStr スクリプト文
     * @param encoding エンコード指定
     * @return String[] 要素1(データ、エラー有無):"true" or "false" or "error", 要素2(データorエラー内容):"文字列"
     * @throws OkuyamaClientException
     */
    public String[] getValueScript(String keyStr, String scriptStr, String encoding) throws OkuyamaClientException {
        String[] ret = new String[2]; 
        String serverRetStr = null;
        String[] serverRet = null;

        StringBuilder serverRequestBuf = null;

        try {
            if (this.socket == null) throw new OkuyamaClientException("No ServerConnect!!");

            // エラーチェック
            // Keyに対する無指定チェック
            if (keyStr == null ||  keyStr.trim().equals("")) 
                throw new OkuyamaClientException("The blank is not admitted on a key");

            // Keyに対するLengthチェック
            if (keyStr.getBytes().length > maxKeySize) throw new OkuyamaClientException("Save Key Max Size " + keyStr + " Byte");

            // Scriptチェック
            if (scriptStr == null ||  scriptStr.trim().equals("")) {
                scriptStr = OkuyamaClient.blankStr;
                throw new OkuyamaClientException("The blank is not admitted on a Script");
            } 

            // 文字列バッファ初期化
            serverRequestBuf = new StringBuilder(ImdstDefine.stringBufferMiddleSize);


            // 処理番号連結
            serverRequestBuf.append("8");
            // セパレータ連結
            serverRequestBuf.append(OkuyamaClient.sepStr);


            // Key連結(Keyはデータ送信時には必ず文字列が必要)
            serverRequestBuf.append(new String(this.dataEncoding(keyStr.getBytes())));
            // セパレータ連結
            serverRequestBuf.append(OkuyamaClient.sepStr);
            // Script連結
            serverRequestBuf.append(new String(this.dataEncoding(scriptStr.getBytes())));

            // サーバ送信
            pw.println(serverRequestBuf.toString());
            pw.flush();

            // サーバから結果受け取り
            serverRetStr = br.readLine();

            serverRet = serverRetStr.split(OkuyamaClient.sepStr);

            // 処理の妥当性確認
            if (serverRet[0].equals("8")) {
                if (serverRet[1].equals("true")) {

                    // データ有り
                    ret[0] = serverRet[1];

                    // Valueがブランク文字か調べる
                    if (serverRet[2].equals(OkuyamaClient.blankStr)) {
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
                throw new OkuyamaClientException("Execute Violation of validity");
            }
        } catch (OkuyamaClientException ice) {
            throw ice;
        } catch (ConnectException ce) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.getValueScript(keyStr, scriptStr, encoding);
                } catch (Exception e) {
                    throw new OkuyamaClientException(ce);
                }
            } else {
                throw new OkuyamaClientException(ce);
            }
        } catch (SocketException se) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.getValueScript(keyStr, scriptStr, encoding);
                } catch (Exception e) {
                    throw new OkuyamaClientException(se);
                }
            } else {
                throw new OkuyamaClientException(se);
            }
        } catch (Throwable e) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.getValueScript(keyStr, scriptStr, encoding);
                } catch (Exception ee) {
                    throw new OkuyamaClientException(e);
                }
            } else {
                throw new OkuyamaClientException(e);
            }
        }
        return ret;
    }


    /**
     * MasterNodeからKeyでValueを取得する.<br>
     * Scriptを同時に実行する.<br>
     * ScriptにValue更新指示を記述してる場合はこちらを実行する.<br>
     * 文字列エンコーディング指定なし.<br>
     *
     * @param keyStr キー値
     * @param scriptStr スクリプト文
     * @return String[] 要素1(データ、エラー有無):"true" or "false" or "error", 要素2(データorエラー内容):"文字列"
     * @throws OkuyamaClientException
     */
    public String[] getValueScriptForUpdate(String keyStr, String scriptStr) throws OkuyamaClientException {
        return getValueScriptForUpdate(keyStr, scriptStr, null);
        
    }


    /**
     * MasterNodeからKeyでデータを取得する.<br>
     * Scriptを同時に実行する.<br>
     * ScriptにValue更新指示を記述してる場合はこちらを実行する.<br>
     * 文字列エンコーディング指定あり.<br>
     *
     * @param keyStr キー値
     * @param scriptStr スクリプト文
     * @param encoding
     * @return String[] 要素1(データ、エラー有無):"true" or "false" or "error", 要素2(データorエラー内容):"文字列"
     * @throws OkuyamaClientException
     */
    public String[] getValueScriptForUpdate(String keyStr, String scriptStr, String encoding) throws OkuyamaClientException {
        String[] ret = new String[2]; 
        String serverRetStr = null;
        String[] serverRet = null;

        StringBuilder serverRequestBuf = null;

        try {
            if (this.socket == null) throw new OkuyamaClientException("No ServerConnect!!");

            // エラーチェック
            // Keyに対する無指定チェック
            if (keyStr == null ||  keyStr.trim().equals("")) 
                throw new OkuyamaClientException("The blank is not admitted on a key");

            // Keyに対するLengthチェック
            if (keyStr.getBytes().length > maxKeySize) throw new OkuyamaClientException("Save Key Max Size " + keyStr + " Byte");

            // Scriptチェック
            if (scriptStr == null ||  scriptStr.equals("")) {
                scriptStr = OkuyamaClient.blankStr;
                throw new OkuyamaClientException("The blank is not admitted on a Script");
            } 

            // 文字列バッファ初期化
            serverRequestBuf = new StringBuilder(ImdstDefine.stringBufferMiddleSize);


            // 処理番号連結
            serverRequestBuf.append("9");
            // セパレータ連結
            serverRequestBuf.append(OkuyamaClient.sepStr);


            // Key連結(Keyはデータ送信時には必ず文字列が必要)
            serverRequestBuf.append(new String(this.dataEncoding(keyStr.getBytes())));
            // セパレータ連結
            serverRequestBuf.append(OkuyamaClient.sepStr);
            // Script連結
            serverRequestBuf.append(new String(this.dataEncoding(scriptStr.getBytes())));

            // サーバ送信
            pw.println(serverRequestBuf.toString());
            pw.flush();

            // サーバから結果受け取り
            serverRetStr = br.readLine();

            serverRet = serverRetStr.split(OkuyamaClient.sepStr);

            // 処理の妥当性確認
            if (serverRet[0].equals("9")) {
                if (serverRet[1].equals("true")) {

                    // データ有り
                    ret[0] = serverRet[1];

                    // Valueがブランク文字か調べる
                    if (serverRet[2].equals(OkuyamaClient.blankStr)) {
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
                throw new OkuyamaClientException("Execute Violation of validity");
            }
        } catch (OkuyamaClientException ice) {
            throw ice;
        } catch (ConnectException ce) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.getValueScriptForUpdate(keyStr, scriptStr, encoding);
                } catch (Exception e) {
                    throw new OkuyamaClientException(ce);
                }
            } else {
                throw new OkuyamaClientException(ce);
            }
        } catch (SocketException se) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.getValueScriptForUpdate(keyStr, scriptStr, encoding);
                } catch (Exception e) {
                    throw new OkuyamaClientException(se);
                }
            } else {
                throw new OkuyamaClientException(se);
            }
        } catch (Throwable e) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.getValueScriptForUpdate(keyStr, scriptStr, encoding);
                } catch (Exception ee) {
                    throw new OkuyamaClientException(e);
                }
            } else {
                throw new OkuyamaClientException(e);
            }
        }
        return ret;
    }


    /**
     * MasterNodeからKeyでValueを削除する.<br><br>
     *
     * @param keyStr Key値
     * @return String[] 要素1(データ有無):"true" or "false",要素2(データ):"データ文字列"
     * @throws OkuyamaClientException
     */
    public String[] removeValue(String keyStr) throws OkuyamaClientException {
        return this.removeValue(keyStr, null);
    }

    /**
     * MasterNodeからKeyでデータを削除する.<br>
     * 取得値のエンコーディング指定あり.<br>
     *
     * @param keyStr Key値
     * @return String[] 削除したデータ 内容) 要素1(データ削除有無):"true" or "false",要素2(削除データ):"データ文字列"
     * @throws OkuyamaClientException
     */
    public String[] removeValue(String keyStr, String encoding) throws OkuyamaClientException {
        String[] ret = new String[2]; 
        String serverRetStr = null;
        String[] serverRet = null;

        StringBuilder serverRequestBuf = null;

        try {
            if (this.socket == null) throw new OkuyamaClientException("No ServerConnect!!");

            // エラーチェック
            // Keyに対する無指定チェック
            if (keyStr == null ||  keyStr.trim().equals(""))
                throw new OkuyamaClientException("The blank is not admitted on a key");

            // Keyに対するLengthチェック
            if (keyStr.getBytes().length > maxKeySize) throw new OkuyamaClientException("Save Key Max Size " + keyStr + " Byte");

            // 文字列バッファ初期化
            serverRequestBuf = new StringBuilder(ImdstDefine.stringBufferSmallSize);

            // 処理番号連結
            serverRequestBuf.append("5");
            // セパレータ連結
            serverRequestBuf.append(OkuyamaClient.sepStr);
            // Key連結(Keyはデータ送信時には必ず文字列が必要)
            serverRequestBuf.append(new String(this.dataEncoding(keyStr.getBytes())));
            // セパレータ連結
            serverRequestBuf.append(OkuyamaClient.sepStr);
            // TransactionCode連結
            serverRequestBuf.append(this.transactionCode);


            // サーバ送信
            pw.println(serverRequestBuf.toString());


            pw.flush();

            // サーバから結果受け取り
            serverRetStr = br.readLine();

            serverRet = serverRetStr.split(OkuyamaClient.sepStr);

            // 処理の妥当性確認
            if (serverRet[0].equals("5")) {
                if (serverRet[1].equals("true")) {

                    // データ有り
                    ret[0] = serverRet[1];

                    // Valueがブランク文字か調べる
                    if (serverRet[2].equals(OkuyamaClient.blankStr)) {
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
                throw new OkuyamaClientException("Execute Violation of validity");
            }
        } catch (OkuyamaClientException ice) {
            throw ice;
        } catch (ConnectException ce) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.removeValue(keyStr, encoding);
                } catch (Exception e) {
                    throw new OkuyamaClientException(ce);
                }
            } else {
                throw new OkuyamaClientException(ce);
            }
        } catch (SocketException se) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.removeValue(keyStr, encoding);
                } catch (Exception e) {
                    throw new OkuyamaClientException(se);
                }
            } else {
                throw new OkuyamaClientException(se);
            }
        } catch (Throwable e) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.removeValue(keyStr, encoding);
                } catch (Exception ee) {
                    throw new OkuyamaClientException(e);
                }
            } else {
                throw new OkuyamaClientException(e);
            }
        }
        return ret;
    }


    /**
     * MasterNodeへKey値とTag値を指定してTagの紐付きを削除する.<br>
     *
     * @param keyStr Key値
     * @param tagStr tag値
     * @return boolean 削除成否
     * @throws OkuyamaClientException
     */
    public boolean removeTagFromKey(String keyStr, String tagStr) throws OkuyamaClientException {
        boolean ret = false; 
        String serverRetStr = null;
        String[] serverRet = null;

        // 文字列バッファ初期化
        StringBuilder serverRequestBuf = null;

        try {
            // Byte Lenghtチェック
            if (tagStr != null) {
                if (tagStr.getBytes().length > maxValueSize) throw new OkuyamaClientException("Tag Max Size " + maxValueSize + " Byte");
            }

            if (this.socket == null) throw new OkuyamaClientException("No ServerConnect!!");

            // エラーチェック
            // Keyに対する無指定チェック
            if (keyStr == null ||  keyStr.trim().equals(""))
                throw new OkuyamaClientException("The blank is not admitted on a key");

            if (keyStr.getBytes().length > maxKeySize) throw new OkuyamaClientException("Save Key Max Size " + maxKeySize + " Byte");

            serverRequestBuf = new StringBuilder();
            // 処理番号連結
            serverRequestBuf.append("40");
            // セパレータ連結
            serverRequestBuf.append(OkuyamaClient.sepStr);


            // Tag連結
            serverRequestBuf.append(new String(this.dataEncoding(tagStr.getBytes())));
            // セパレータ連結
            serverRequestBuf.append(OkuyamaClient.sepStr);

            // Key連結
            serverRequestBuf.append(new String(this.dataEncoding(keyStr.getBytes())));
            // セパレータ連結
            serverRequestBuf.append(OkuyamaClient.sepStr);

            // TransactionCode連結
            serverRequestBuf.append(this.transactionCode);

            // サーバ送信
            pw.println(serverRequestBuf.toString());
            pw.flush();

            // サーバから結果受け取り
            serverRetStr = br.readLine();
            serverRet = serverRetStr.split(OkuyamaClient.sepStr);

            // 処理の妥当性確認
            if (serverRet.length == 2 && serverRet[0].equals("40")) {
                if (serverRet[1].equals("true")) {

                    // 処理成功
                    ret = true;
                } else {

                    // データなし
                    ret = false;
                }
            } else {

                // 妥当性違反
                throw new OkuyamaClientException("Execute Violation of validity [" + serverRetStr + "]");
            }

        } catch (OkuyamaClientException ice) {
            throw ice;
        } catch (ConnectException ce) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.removeTagFromKey(keyStr, tagStr);
                } catch (Exception e) {
                    throw new OkuyamaClientException(ce);
                }
            } else {
                throw new OkuyamaClientException(ce);
            }
        } catch (SocketException se) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.removeTagFromKey(keyStr, tagStr);
                } catch (Exception e) {
                    throw new OkuyamaClientException(se);
                }
            } else {
                throw new OkuyamaClientException(se);
            }
        } catch (Throwable e) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.removeTagFromKey(keyStr, tagStr);
                } catch (Exception ee) {
                    throw new OkuyamaClientException(e);
                }
            } else {
                throw new OkuyamaClientException(e);
            }
        }
        return ret;
    }


    /**
     * 全文検索用のIndexを削除する。
     * Prefixなし<br>
     * 検索Index長さ指定なし<br>
     *
     * @param keyStr Key値
     * @return boolean 削除成否
     * @throws OkuyamaClientException
     */
    public boolean removeSearchIndex(String keyStr) throws OkuyamaClientException {
        return this.removeSearchIndex(keyStr, null, 3);
    }

    /**
     * 全文検索用のIndexを削除する。
     * Prefixあり<br>
     * 検索Index長さ指定なし<br>
     *
     * @param keyStr Key値
     * @param indexPrefix 作成時に設定したIndexのPrefix値
     * @return boolean 削除成否
     * @throws OkuyamaClientException
     */
    public boolean removeSearchIndex(String keyStr, String indexPrefix) throws OkuyamaClientException {
        return this.removeSearchIndex(keyStr, indexPrefix, 3);
    }

    /**
     * 全文検索用のIndexを削除する。
     * Prefixなし<br>
     * 検索Index長さ指定あり<br>
     *
     * @param keyStr Key値
     * @param indexLength 作成時に指定した作成Indexの長さ指定
     * @return boolean 削除成否
     * @throws OkuyamaClientException
     */
    public boolean removeSearchIndex(String keyStr, int indexLength) throws OkuyamaClientException {
        return this.removeSearchIndex(keyStr, null, indexLength);
    }


    /**
     * 全文検索用のIndexを削除する。
     * Prefixあり<br>
     * 検索Index長さ指定あり<br>
     *
     * @param keyStr Key値
     * @param indexPrefix 作成時に設定したIndexのPrefix値
     * @param indexLength 作成時に指定した作成Indexの長さ指定
     * @return boolean 削除成否
     * @throws OkuyamaClientException
     */
    public boolean removeSearchIndex(String keyStr, String indexPrefix, int indexLength) throws OkuyamaClientException {
        boolean ret = false; 
        String serverRetStr = null;
        String[] serverRet = null;
        String encodeValue = null;

        // 文字列バッファ初期化
        setValueServerReqBuf.delete(0, Integer.MAX_VALUE);

        try {

            if (this.socket == null) throw new OkuyamaClientException("No ServerConnect!!");

            // エラーチェック
            // Keyに対する無指定チェック
            if (keyStr == null ||  keyStr.trim().equals(""))
                throw new OkuyamaClientException("The blank is not admitted on a key");

            if (keyStr.getBytes(ImdstDefine.characterDecodeSetBySearch).length > maxKeySize) throw new OkuyamaClientException("Save Key Max Size " + maxKeySize + " Byte");


            // 処理番号連結
            setValueServerReqBuf.append("44");
            // セパレータ連結
            setValueServerReqBuf.append(OkuyamaClient.sepStr);


            // Key連結(Keyはデータ送信時には必ず文字列が必要)
            setValueServerReqBuf.append(new String(this.dataEncoding(keyStr.getBytes(ImdstDefine.characterDecodeSetBySearch))));
            // セパレータ連結
            setValueServerReqBuf.append(OkuyamaClient.sepStr);


            // TransactionCode連結
            setValueServerReqBuf.append(this.transactionCode);

            // セパレータ連結
            setValueServerReqBuf.append(OkuyamaClient.sepStr);


            // Indexプレフィックス指定の有無を調べてIndexプレフィックス連結
            if (indexPrefix == null || indexPrefix.length() < 1) {
                // ブランク規定文字列を連結
                setValueServerReqBuf.append(OkuyamaClient.blankStr);
            } else {

                // Indexプレフィックス連結
                setValueServerReqBuf.append(new String(this.dataEncoding(indexPrefix.getBytes(ImdstDefine.characterDecodeSetBySearch))));
            }

            // セパレータ連結
            setValueServerReqBuf.append(OkuyamaClient.sepStr);

            // createIndexLen連結
            setValueServerReqBuf.append(indexLength);


            // サーバ送信
            pw.println(setValueServerReqBuf.toString());
            pw.flush();

            // サーバから結果受け取り
            serverRetStr = br.readLine();
            serverRet = serverRetStr.split(OkuyamaClient.sepStr);

            // 処理の妥当性確認
            if (serverRet[0].equals("44")) {
                if (serverRet[1].equals("true")) {

                    // 処理成功
                    ret = true;
                } else if (serverRet[1].equals("false")){

                    // データなし
                    ret = false;
                } else if (serverRet[1].equals("error")){

                    // 処理失敗(メッセージ格納)
                    throw new OkuyamaClientException(serverRet[2]);
                }
            } else {

                // 妥当性違反
                throw new OkuyamaClientException("Execute Violation of validity [" + serverRetStr + "]");
            }

        } catch (OkuyamaClientException ice) {
            throw ice;
        } catch (ConnectException ce) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.removeSearchIndex(keyStr, indexPrefix, indexLength);
                } catch (Exception e) {
                    throw new OkuyamaClientException(ce);
                }
            } else {
                throw new OkuyamaClientException(ce);
            }
        } catch (SocketException se) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.removeSearchIndex(keyStr, indexPrefix, indexLength);
                } catch (Exception e) {
                    throw new OkuyamaClientException(se);
                }
            } else {
                throw new OkuyamaClientException(se);
            }
        } catch (Throwable e) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.removeSearchIndex(keyStr, indexPrefix, indexLength);
                } catch (Exception ee) {
                    throw new OkuyamaClientException(e);
                }
            } else {
                throw new OkuyamaClientException(e);
            }
        }
        return ret;
    }



    /**
     * MasterNodeからKeyでValueを取得する(バイナリ).<br>
     * setByteValueで登録したValueはこちらで取得する.<br>
     *
     * @param keyStr Key値
     * @return Object[] 要素1(String)(データ有無):"true" or "false",要素2(byte[])(データ):{バイト配列}
     * @throws OkuyamaClientException
     */
    public Object[] getByteValue(String keyStr) throws OkuyamaClientException {
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
            throw new OkuyamaClientException(e);
        }
        return ret;
    }


    /**
     * MasterNodeからKeyでValueを取得する(バイナリ).<br>
     * setByteValueメソッドで登録したValueはこちらで取得する.<br>
     * getByteValueよりもこちらのほうが高速に動作する.<br>
     *
     * @param keyStr Key値
     * @return Object[] 要素1(String)(データ有無):"true" or "false",要素2(byte[])(データ):{バイト配列}
     * @throws OkuyamaClientException
     */
    public Object[] getByteValueVer2(String keyStr) throws OkuyamaClientException {
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
            throw new OkuyamaClientException(e);
        }
        return ret;
    }


    /**
     * MasterNodeからKeyでデータを取得する(バイナリ).<br>
     *
     * @param keyStr Key値
     * @return Object[] 要素1(String)(データ有無):"true" or "false",要素2(byte[])(データ):{バイト配列}
     * @throws OkuyamaClientException
     */
    protected Object[] getByteData(String keyStr) throws OkuyamaClientException {
        Object[] ret = new Object[2];
        byte[] byteRet = null;
        String serverRetStr = null;
        String[] serverRet = null;

        StringBuilder serverRequestBuf = null;

        try {
            if (this.socket == null) throw new OkuyamaClientException("No ServerConnect!!");

            // エラーチェック
            // Keyに対する無指定チェック
            if (keyStr == null ||  keyStr.trim().equals(""))
                throw new OkuyamaClientException("The blank is not admitted on a key");

            // Keyに対するLengthチェック
            if (keyStr.getBytes().length > maxKeySize) throw new OkuyamaClientException("Save Key Max Size " + keyStr + " Byte");


            // 文字列バッファ初期化
            serverRequestBuf = new StringBuilder(ImdstDefine.stringBufferSmallSize);


            // 処理番号連結
            serverRequestBuf.append("2");
            // セパレータ連結
            serverRequestBuf.append(OkuyamaClient.sepStr);


            // Key連結(Keyはデータ送信時には必ず文字列が必要)
            serverRequestBuf.append(new String(this.dataEncoding(keyStr.getBytes())));


            // サーバ送信
            pw.println(serverRequestBuf.toString());
            pw.flush();

            // サーバから結果受け取り
            serverRetStr = br.readLine();

            serverRet = serverRetStr.split(OkuyamaClient.sepStr);

            // 処理の妥当性確認
            if (serverRet[0].equals("2")) {
                if (serverRet[1].equals("true")) {

                    // データ有り
                    ret[0] = serverRet[1];

                    // Valueがブランク文字か調べる
                    if (serverRet[2].equals(OkuyamaClient.blankStr)) {
                        byteRet = new byte[0];
                        ret[1] = byteRet;
                    } else {

                        // Value文字列をBase64でデコードし、圧縮解除
                        ret[1] = this.execDecompres(this.dataDecoding(serverRet[2].getBytes()));
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
                throw new OkuyamaClientException("Execute Violation of validity");
            }
        } catch (OkuyamaClientException ice) {
            throw ice;
        } catch (ConnectException ce) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.getByteData(keyStr);
                } catch (Exception e) {
                    throw new OkuyamaClientException(ce);
                }
            } else {
                throw new OkuyamaClientException(ce);
            }
        } catch (SocketException se) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.getByteData(keyStr);
                } catch (Exception e) {
                    throw new OkuyamaClientException(se);
                }
            } else {
                throw new OkuyamaClientException(se);
            }
        } catch (Throwable e) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.getByteData(keyStr);
                } catch (Exception ee) {
                    throw new OkuyamaClientException(e);
                }
            } else {
                throw new OkuyamaClientException(e);
            }
        }
        return ret;
    }


    /**
     * MasterNodeからKeyでデータを取得する(バイナリ).<br>
     * sendByteValueで登録したValueはこちらで取得する.<br>
     *
     * @param keyStr Key値
     * @return Object[] 要素1(String)(データ有無):"true" or "false",要素2(byte[])(データ):{バイト配列}
     * @throws OkuyamaClientException
     */
    public Object[] readByteValue(String keyStr) throws OkuyamaClientException {
        Object[] ret = new Object[2];
        byte[] byteRet = null;
        String serverRetStr = null;
        String[] serverRet = null;

        StringBuilder serverRequestBuf = null;

        try {
            if (this.socket == null) throw new OkuyamaClientException("No ServerConnect!!");

            // エラーチェック
            // Keyに対する無指定チェック
            if (keyStr == null ||  keyStr.trim().equals(""))
                throw new OkuyamaClientException("The blank is not admitted on a key");

            // Keyに対するLengthチェック
            if (keyStr.getBytes().length > maxKeySize) throw new OkuyamaClientException("Save Key Max Size " + keyStr + " Byte");

            // 文字列バッファ初期化
            serverRequestBuf = new StringBuilder(ImdstDefine.stringBufferSmallSize);

            // 処理番号連結
            serverRequestBuf.append("2");
            // セパレータ連結
            serverRequestBuf.append(OkuyamaClient.sepStr);

            // Key連結(Keyはデータ送信時には必ず文字列が必要)
            serverRequestBuf.append(new String(this.dataEncoding(keyStr.getBytes())));

            // サーバ送信
            pw.println(serverRequestBuf.toString());
            pw.flush();

            // サーバから結果受け取り
            serverRetStr = br.readLine();

            serverRet = serverRetStr.split(OkuyamaClient.sepStr);

            // 処理の妥当性確認
            if (serverRet[0].equals("2")) {
                if (serverRet[1].equals("true")) {

                    // データ有り
                    ret[0] = serverRet[1];

                    // Valueがブランク文字か調べる
                    if (serverRet[2].equals(OkuyamaClient.blankStr)) {
                        byteRet = new byte[0];
                        ret[1] = byteRet;
                    } else {

                        // Value文字列をBase64でデコード
                        ret[1] = this.dataDecoding(serverRet[2].getBytes());
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
                throw new OkuyamaClientException("Execute Violation of validity");
            }
        } catch (OkuyamaClientException ice) {
            throw ice;
        } catch (ConnectException ce) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.readByteValue(keyStr);
                } catch (Exception e) {
                    throw new OkuyamaClientException(ce);
                }
            } else {
                throw new OkuyamaClientException(ce);
            }
        } catch (SocketException se) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.readByteValue(keyStr);
                } catch (Exception e) {
                    throw new OkuyamaClientException(se);
                }
            } else {
                throw new OkuyamaClientException(se);
            }
        } catch (Throwable e) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.readByteValue(keyStr);
                } catch (Exception ee) {
                    throw new OkuyamaClientException(e);
                }
            } else {
                throw new OkuyamaClientException(e);
            }
        }
        return ret;
    }


    /**
     * MasterNodeからTagでKey値配列を取得する.<br>
     *
     * @param tagStr Tag値
     * @return Object[] 要素1(データ有無):"true" or "false",要素2(Key値配列):Stringの配列
     * @throws OkuyamaClientException
     */
    public Object[] getTagKeys(String tagStr) throws OkuyamaClientException {
        return this.getTagKeys(tagStr, true);
    }


    /**
     * MasterNodeからTagでKey値配列を取得する.<br>
     * Tagは打たれているが実際は既に存在しないValueをどのように扱うかを指定できる.<br>
     *
     * @param tagStr Tag値
     * @param noExistsData 存在していないデータを取得するかの指定(true:取得する false:取得しない)
     * @return Object[] 要素1(データ有無):"true" or "false",要素2(Key値配列):Stringの配列
     * @throws OkuyamaClientException
     */
    public Object[] getTagKeys(String tagStr, boolean noExistsData) throws OkuyamaClientException {
        Object[] ret = new Object[2]; 
        String serverRetStr = null;
        String[] serverRet = null;

        StringBuilder serverRequestBuf = null;

        try {
            if (this.socket == null) throw new OkuyamaClientException("No ServerConnect!!");

            // エラーチェック
            // Keyに対する無指定チェック
            if (tagStr == null ||  tagStr.equals("")) {
                throw new OkuyamaClientException("The blank is not admitted on a tag");
            }

            // Tagに対するLengthチェック
            if (tagStr.getBytes().length > maxKeySize) throw new OkuyamaClientException("Save Tag Max Size " + maxKeySize + " Byte");

            // 文字列バッファ初期化
            serverRequestBuf = new StringBuilder(ImdstDefine.stringBufferSmallSize);


            // 処理番号連結
            serverRequestBuf.append("3");
            // セパレータ連結
            serverRequestBuf.append(OkuyamaClient.sepStr);


            // tag値連結(Keyはデータ送信時には必ず文字列が必要)
            serverRequestBuf.append(new String(this.dataEncoding(tagStr.getBytes())));
            // セパレータ連結
            serverRequestBuf.append(OkuyamaClient.sepStr);

            // 存在データ取得指定連結
            serverRequestBuf.append(new Boolean(noExistsData).toString());


            // サーバ送信
            pw.println(serverRequestBuf.toString());

            pw.flush();

            // サーバから結果受け取り
            serverRetStr = br.readLine();

            serverRet = serverRetStr.split(OkuyamaClient.sepStr);

            // 処理の妥当性確
            if (serverRet[0].equals("4")) {
                if (serverRet[1].equals("true")) {

                    // データ有り
                    ret[0] = serverRet[1];

                    // Valueがブランク文字か調べる
                    if (serverRet[2].equals(OkuyamaClient.blankStr)) {
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
                throw new OkuyamaClientException("Execute Violation of validity [" + serverRet[0] + "]");
            }
        } catch (OkuyamaClientException ice) {
            throw ice;
        } catch (ConnectException ce) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.getTagKeys(tagStr);
                } catch (Exception e) {
                    throw new OkuyamaClientException(ce);
                }
            } else {
                throw new OkuyamaClientException(ce);
            }
        } catch (SocketException se) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.getTagKeys(tagStr);
                } catch (Exception e) {
                    throw new OkuyamaClientException(se);
                }
            } else {
                throw new OkuyamaClientException(se);
            }
        } catch (Throwable e) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.getTagKeys(tagStr);
                } catch (Exception ee) {
                    throw new OkuyamaClientException(e);
                }
            } else {
                throw new OkuyamaClientException(e);
            }
        }
        return ret;
    }


    /**
     * MasterNodeからsetValueAndCreateIndexで作成されたIndexを使って検索して該当する値を取得する.<br>
     * 検索可能な文字列は1文字からで、最大は128文字(ソフトリミット).<br>
     * Prefxiなし.<br>
     *
     * @param searchCharacterList 取得したい値の文字配列(エンコードはUTF-8固定)
     * @param  searchType "1":AND検索　"2":OR検索
     * @return Object[] 要素1(データ有無):"true" or "false",要素2(該当のKey値配列):Stringの配列
     * @throws OkuyamaClientException
     */
    public Object[] searchValue(String[] searchCharacterList, String searchType) throws OkuyamaClientException {
        return this.searchValue(searchCharacterList, searchType, null);
    }

    /**
     * MasterNodeからsetValueAndCreateIndexで作成されたIndexを使って検索して該当する値を取得する.<br>
     * 検索可能な文字列は1文字からで、最大は128文字(ソフトリミット).<br>
     * Prefxiなし.<br>
     *
     * @param searchCharacterList 取得したい値の文字配列(エンコードはUTF-8固定)
     * @param  searchType 1:AND検索　2:OR検索
     * @param searchIndexLen 検索するIndexをヒストグラムIndex以上にする場合にそのIndexの長さを指定
     * @return Object[] 要素1(データ有無):"true" or "false",要素2(該当のKey値配列):Stringの配列
     * @throws OkuyamaClientException
     */
    public Object[] searchValue(String[] searchCharacterList, String searchType, int searchIndexLen) throws OkuyamaClientException {
        return this.searchValue(searchCharacterList, searchType, null, searchIndexLen);
    }

    /**
     * MasterNodeからsetValueAndCreateIndexで作成されたIndexを使って検索して該当する値を取得する.<br>
     * 検索可能な文字列は1文字からで、最大は128文字(ソフトリミット).<br>
     * Prefxiあり.<br>
     * 
     * @param searchCharacterList 取得したい値の文字配列(エンコードはUTF-8固定)
     * @param  searchType 1:AND検索　2:OR検索
     * @param  prefix 検索Index作成時に指定したPrefix値
     * @return Object[] 要素1(データ有無):"true" or "false",要素2(該当のKey値配列):Stringの配列
     * @throws OkuyamaClientException
     */
    public Object[] searchValue(String[] searchCharacterList, String searchType, String prefix) throws OkuyamaClientException {

        try {
            this.sendSearchValueRequest(searchCharacterList, searchType, prefix, 3);
            return this.readSearchValueResponse(searchCharacterList, searchType, prefix, 3);
        } catch (Throwable e) {
            throw new OkuyamaClientException(e);
        }
    }

    /**
     * MasterNodeからsetValueAndCreateIndexで作成されたIndexを使って検索して該当する値を取得する.<br>
     * 検索可能な文字列は1文字からで、最大は128文字(ソフトリミット).<br>
     * Prefxiあり.<br>
     * 
     * @param searchCharacterList 取得したい値の文字配列(エンコードはUTF-8固定)
     * @param  searchType 1:AND検索　2:OR検索
     * @param  prefix 検索Index作成時に指定したPrefix値
     * @param searchIndexLen 検索するIndexをヒストグラムIndex以上にする場合にそのIndexの長さを指定
     * @return Object[] 要素1(データ有無):"true" or "false",要素2(該当のKey値配列):Stringの配列
     * @throws OkuyamaClientException
     */
    public Object[] searchValue(String[] searchCharacterList, String searchType, String prefix, int searchIndexLen) throws OkuyamaClientException {

        try {
            this.sendSearchValueRequest(searchCharacterList, searchType, prefix, searchIndexLen);
            return this.readSearchValueResponse(searchCharacterList, searchType, prefix, searchIndexLen);
        } catch (Throwable e) {
            throw new OkuyamaClientException(e);
        }
    }

    /**
     * MasterNodeからsetValueAndCreateIndexで作成されたIndexを使って検索して該当する値を取得する.<br>
     * 検索可能な文字列は1文字からで、最大は128文字(ソフトリミット).<br>
     * Prefxiなし.<br>
     *
     * @param searchCharacterList 取得したい値の文字配列(エンコードはUTF-8固定)
     * @param  searchType 1:AND検索　2:OR検索
     * @return Object[] 要素1(データ有無):"true" or "false",要素2(該当のKey値配列):Stringの配列
     * @throws OkuyamaClientException
     */
    protected void sendSearchValueRequest(String[] searchCharacterList, String searchType) throws OkuyamaClientException {
        this.sendSearchValueRequest(searchCharacterList, searchType, null, 3);
    }


    /**
     * MasterNodeからsetValueAndCreateIndexで作成されたIndexを使って検索して該当する値を取得する.<br>
     * 検索可能な文字列は1文字からで、最大は128文字(ソフトリミット).<br>
     * Prefxiなし.<br>
     *
     * @param searchCharacterList 取得したい値の文字配列(エンコードはUTF-8固定)
     * @param  searchType 1:AND検索　2:OR検索
     * @param searchIndexLen 検索するIndexをヒストグラムIndex以上にする場合にそのIndexの長さを指定
     * @return Object[] 要素1(データ有無):"true" or "false",要素2(該当のKey値配列):Stringの配列
     * @throws OkuyamaClientException
     */
    protected void sendSearchValueRequest(String[] searchCharacterList, String searchType, int searchIndexLen) throws OkuyamaClientException {
        this.sendSearchValueRequest(searchCharacterList, searchType, null, searchIndexLen);
    }

    /**
     * MasterNodeからsetValueAndCreateIndexで作成されたIndexを使って検索して該当する値を取得する.<br>
     * 検索可能な文字列は1文字からで、最大は128文字(ソフトリミット).<br>
     * Prefxiあり.<br>
     * 
     * @param searchCharacterList 取得したい値の文字配列(エンコードはUTF-8固定)
     * @param  searchType 1:AND検索　2:OR検索
     * @param  prefix 検索Index作成時に指定したPrefix値
     * @param searchIndexLen 検索するIndexをヒストグラムIndex以上にする場合にそのIndexの長さを指定
     * @return Object[] 要素1(データ有無):"true" or "false",要素2(該当のKey値配列):Stringの配列
     * @throws OkuyamaClientException
     */
    protected void sendSearchValueRequest(String[] searchCharacterList, String searchType, String prefix, int searchIndexLen) throws OkuyamaClientException {

        Object[] ret = new Object[2]; 
        String serverRetStr = null;
        String[] serverRet = null;

        StringBuilder serverRequestBuf = null;

        try {
            if (this.socket == null) throw new OkuyamaClientException("No ServerConnect!!");

            // エラーチェック
            // Keyに対する無指定チェック
            if (searchCharacterList == null ||  searchCharacterList.length == 0) {
                throw new OkuyamaClientException("The blank is not admitted on a searchCharacterList");
            }

            // 検索ワードに対するLengthチェック
            for (int idx = 0; idx < searchCharacterList.length; idx++) {

                if (searchCharacterList[idx].length() > 128) throw new OkuyamaClientException("SearchCharacter MaxSize 128Character");
            }

            // 検索Typeを調整
            if (searchType == null || !(searchType.equals("1") || searchType.equals("2"))) searchType = "2";

            // 文字列バッファ初期化
            serverRequestBuf = new StringBuilder(ImdstDefine.stringBufferSmallSize);


            // 処理番号連結
            serverRequestBuf.append("43");
            // セパレータ連結
            serverRequestBuf.append(OkuyamaClient.sepStr);


            // 検索ワード値連結
            // 複数の検索Wordは":"で連結して送る
            String sep = "";
            for (int idx = 0; idx < searchCharacterList.length; idx++) {
                serverRequestBuf.append(sep);
                serverRequestBuf.append(new String(this.dataEncoding(searchCharacterList[idx].getBytes(ImdstDefine.characterDecodeSetBySearch))));
                sep = ":";
            }

            // セパレータ連結
            serverRequestBuf.append(OkuyamaClient.sepStr);

            // searchType連結
            serverRequestBuf.append(searchType);

            // セパレータ連結
            serverRequestBuf.append(OkuyamaClient.sepStr);

            // prefix連結
            // Indexプレフィックス指定の有無を調べてIndexプレフィックス連結
            if (prefix == null || prefix.length() < 1) {
                // ブランク規定文字列を連結
                serverRequestBuf.append(OkuyamaClient.blankStr);
            } else {

                // Indexプレフィックス連結
                serverRequestBuf.append(new String(this.dataEncoding(prefix.getBytes(ImdstDefine.characterDecodeSetBySearch))));
            }

            // セパレータ連結
            serverRequestBuf.append(OkuyamaClient.sepStr);
            // searchIndexLen連結
            serverRequestBuf.append(searchIndexLen);


            // サーバ送信
            pw.println(serverRequestBuf.toString());
            pw.flush();

            this.sendSearchFlg = true;
        } catch (OkuyamaClientException ice) {
            throw ice;
        } catch (Throwable e) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    this.sendSearchValueRequest(searchCharacterList, searchType, prefix, searchIndexLen);
                } catch (Exception ee) {
                    throw new OkuyamaClientException(e);
                }
            } else {
                throw new OkuyamaClientException(e);
            }
        }
    }


    /**
     * MasterNodeからsetValueAndCreateIndexで作成されたIndexを使って検索して該当する値を取得する.<br>
     * 検索可能な文字列は1文字からで、最大は128文字(ソフトリミット).<br>
     * Prefxiあり.<br>
     * 
     * @param searchCharacterList 取得したい値の文字配列(エンコードはUTF-8固定)
     * @param  searchType 1:AND検索　2:OR検索
     * @param  prefix 検索Index作成時に指定したPrefix値
     * @return Object[] 要素1(データ有無):"true" or "false",要素2(該当のKey値配列):Stringの配列
     * @throws OkuyamaClientException
     */
    protected Object[] readSearchValueResponse(String[] searchCharacterList, String searchType, String prefix) throws OkuyamaClientException {
        return readSearchValueResponse(searchCharacterList, searchType, prefix, 3);
    }


    /**
     * MasterNodeからsetValueAndCreateIndexで作成されたIndexを使って検索して該当する値を取得する.<br>
     * 検索可能な文字列は1文字からで、最大は128文字(ソフトリミット).<br>
     * Prefxiあり.<br>
     * 
     * @param searchCharacterList 取得したい値の文字配列(エンコードはUTF-8固定)
     * @param  searchType 1:AND検索　2:OR検索
     * @param  prefix 検索Index作成時に指定したPrefix値
     * @param  searchIndexLen 検索するIndexをヒストグラムIndex以上にする場合にそのIndexの長さを指定
     * @return Object[] 要素1(データ有無):"true" or "false",要素2(該当のKey値配列):Stringの配列
     * @throws OkuyamaClientException
     */
    protected Object[] readSearchValueResponse(String[] searchCharacterList, String searchType, String prefix, int searchIndexLen) throws OkuyamaClientException {

        Object[] ret = new Object[2]; 
        String serverRetStr = null;
        String[] serverRet = null;

        StringBuilder serverRequestBuf = null;

        try {

            if (this.sendSearchFlg == false) throw new OkuyamaClientException("Not Request Send");
            // サーバから結果受け取り
            serverRetStr = br.readLine();
            serverRet = serverRetStr.split(OkuyamaClient.sepStr);

            // 処理の妥当性確
            if (serverRet[0].equals("43")) {
                if (serverRet[1].equals("true")) {

                    // データ有り
                    ret[0] = serverRet[1];

                    String[] keys = null;

                    keys = serverRet[2].split(tagKeySep);
                    String[] decKeys = new String[keys.length];
                    for (int i = 0; i < keys.length; i++) {
                        decKeys[i] = new String(this.dataDecoding(keys[i].getBytes(ImdstDefine.characterDecodeSetBySearch)));

                    }
                    ret[1] = decKeys;

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
                throw new OkuyamaClientException("Execute Violation of validity [" + serverRet[0] + "]");
            }
        } catch (OkuyamaClientException ice) {
            throw ice;
        } catch (ConnectException ce) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    this.sendSearchFlg = false;
                    this.sendSearchValueRequest(searchCharacterList, searchType, prefix, searchIndexLen);
                    ret = this.readSearchValueResponse(searchCharacterList, searchType, prefix, searchIndexLen);
                } catch (Exception e) {
                    throw new OkuyamaClientException(ce);
                }
            } else {
                throw new OkuyamaClientException(ce);
            }
        } catch (SocketException se) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    this.sendSearchFlg = false;
                    this.sendSearchValueRequest(searchCharacterList, searchType, prefix, searchIndexLen);
                    ret = this.readSearchValueResponse(searchCharacterList, searchType, prefix, searchIndexLen);
                } catch (Exception e) {
                    throw new OkuyamaClientException(se);
                }
            } else {
                throw new OkuyamaClientException(se);
            }
        } catch (Throwable e) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    this.sendSearchFlg = false;
                    this.sendSearchValueRequest(searchCharacterList, searchType, prefix, searchIndexLen);
                    ret = this.readSearchValueResponse(searchCharacterList, searchType, prefix, searchIndexLen);
                } catch (Exception ee) {
                    throw new OkuyamaClientException(e);
                }
            } else {
                throw new OkuyamaClientException(e);
            }
        } finally {
            this.sendSearchFlg = false;
        }
        return ret;
    }


    // Base64でエンコード
    protected byte[] dataEncoding(byte[] datas) {
        return BASE64EncoderStream.encode(datas);
    }

    // Base64でデコード
    protected byte[] dataDecoding(byte[] datas) {
        return BASE64DecoderStream.decode(datas);
    }

    // 圧縮メソッド
    protected byte[] execCompress(byte[] bytes) throws Exception {
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
    protected byte[] execDecompres(byte[] bytes)  throws Exception {
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
