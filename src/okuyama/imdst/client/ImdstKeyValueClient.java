package okuyama.imdst.client;

import java.util.*;
import java.io.*;
import java.net.*;

import com.sun.mail.util.BASE64DecoderStream;
import com.sun.mail.util.BASE64EncoderStream;

import okuyama.imdst.util.ImdstDefine;


/**
 * MasterNodeと通信を行うプログラムインターフェース<br>
 * okuyamaが内部で使用するClient<br>
 * 一般的なクライアントアプリケーションが使用する想定ではない<br>
 * okuyama.imdst.client.OkuyamaClientを使用してください.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class ImdstKeyValueClient extends OkuyamaClient {


    /**
     * コンストラクタ
     *
     */
    public ImdstKeyValueClient() {
        super();
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
        this.connect(server, port, OkuyamaClient.connectDefaultEncoding, ImdstDefine.clientConnectionOpenTimeout * 3, ImdstDefine.clientConnectionTimeout * 3);
    }


    /**
     * 保存するデータの最大長を変更する.<br>
     * 接続時に
     *
     * @param size 保存サイズ(バイト長)
     */
    public void setSaveMaxDataSize(int size) {
        super.saveSize = size;
        super.maxValueSize = size;
    }


    /**
     * バイナリデータ分割保存サイズを変更<br>
     *
     * @param size サイズ
     */
    public void changeByteSaveSize(int size) {
        super.saveSize = size;
    }


    /**
     * 設定されたMasterNodeの接続情報を元に自動的に接続を行う.<br>
     * 接続出来ない場合自動的に別ノードへ再接続を行う.<br>
     *
     * @param masterNodes 接続情報の配列 "IP:PORT"の形式
     * @throws OkuyamaClientException
     */
    public void nextConnect() throws OkuyamaClientException {
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
     * トランザクションを開始している場合、自身のトランザクションを一意に表す
     * コードを返す.
     * このコードをsetNowTransactionCodeに渡すと、別クライアントのTransactionを引き継げる
     * !! 他クライアントの処理を横取ることが出来るため、使用を推奨しない !! 
     * 
     * @return String
     */
    public String getNowTransactionCode() {
        return super.getNowTransactionCode();
    }


    /**
     * 他のクライアントが実施しているトランザクションコードを設定することで、
     * トランザクション処理を引き継ぐことが出来る。
     * !!! 他クライアントの処理を横取ることが出来るため、使用を推奨しない !!!
     * 
     * @return String
     */
    public void setNowTransactionCode(String transactionCode) {
        super.setNowTransactionCode(transactionCode);
    }


    /**
     * MasterNodeの生死を確認する.<br>
     *
     * @return boolean true:開始成功 false:開始失敗
     * @throws OkuyamaClientException
     */
    public boolean arrivalMasterNode() throws OkuyamaClientException {
        boolean ret = false;
        String serverRetStr = null;
        String[] serverRet = null;

        StringBuilder serverRequestBuf = null;

        try {
            if (this.socket == null) {
                // 処理失敗(メッセージ格納)
                throw new OkuyamaClientException("No ServerConnect!!");
            }

            // 文字列バッファ初期化
            serverRequestBuf = new StringBuilder();


            // 処理番号連結
            serverRequestBuf.append("12");


            // サーバ送信
            pw.println(serverRequestBuf.toString());
            pw.flush();

            // サーバから結果受け取り
            serverRetStr = br.readLine();
            serverRet = serverRetStr.split(OkuyamaClient.sepStr);

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
        } catch (OkuyamaClientException ice) {
            throw ice;
        } catch (ConnectException ce) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.autoConnect();
                    ret = this.arrivalMasterNode();
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
                    ret = this.arrivalMasterNode();
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
                    ret = this.arrivalMasterNode();
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
     * DataNodeのステータスを取得する.<br>
     * DataNodのステータスは常にメインマスターノードが管理しているので、メインマスターノードに<br>
     * 接続している場合のみ取得可能.<br>
     *
     * @param nodeInfo DataNodeとPortの組み合わせ文字列 "NodeName:PortNo"
     * @return String 結果文字列
     * @throws OkuyamaClientException
     */
    public String getDataNodeStatus(String nodeInfo) throws OkuyamaClientException {
        String ret = null;
        String serverRetStr = null;
        String[] serverRet = null;

        StringBuilder serverRequestBuf = null;

        try {
            if (this.socket == null) throw new OkuyamaClientException("No ServerConnect!!");


            // 文字列バッファ初期化
            serverRequestBuf = new StringBuilder(ImdstDefine.stringBufferSmallSize);


            // 処理番号連結
            serverRequestBuf.append("10");
            // セパレータ連結
            serverRequestBuf.append(OkuyamaClient.sepStr);
            // ノード名連結
            serverRequestBuf.append(nodeInfo);

            // サーバ送信
            pw.println(serverRequestBuf.toString());
            pw.flush();

            // サーバから結果受け取り
            serverRetStr = br.readLine();

            serverRet = serverRetStr.split(OkuyamaClient.sepStr);

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
                throw new OkuyamaClientException("Execute Violation of validity");
            }
        } catch (OkuyamaClientException ice) {
            throw ice;
        } catch (ConnectException ce) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.nextConnect();
                    ret = this.getDataNodeStatus(nodeInfo);
                } catch (Exception e) {
                    throw new OkuyamaClientException(ce);
                }
            } else {
                throw new OkuyamaClientException(ce);
            }
        } catch (SocketException se) {
            if (this.masterNodesList != null && masterNodesList.size() > 1) {
                try {
                    this.nextConnect();
                    ret = this.getDataNodeStatus(nodeInfo);
                } catch (Exception e) {
                    throw new OkuyamaClientException(se);
                }
            } else {
                throw new OkuyamaClientException(se);
            }
        } catch (Exception e) {
            throw new OkuyamaClientException(e);
        }
        return ret;
    }
}
