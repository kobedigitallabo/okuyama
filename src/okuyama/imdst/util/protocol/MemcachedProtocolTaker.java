package okuyama.imdst.util.protocol;

import java.io.*;
import java.util.*;
import java.net.*;

import com.sun.mail.util.BASE64DecoderStream;
import com.sun.mail.util.BASE64EncoderStream;

import okuyama.imdst.util.StatusUtil;
import okuyama.imdst.util.SystemUtil;
import okuyama.imdst.util.ImdstDefine;
import okuyama.imdst.util.JavaSystemApi;
import okuyama.imdst.util.io.CustomReader;


/**
 * クライアントとのProtocolの差を保管する.<br>
 * memcachedのProtocol用のTaker.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class MemcachedProtocolTaker extends AbstractProtocolTaker implements IProtocolTaker {

    private int nextExec = 0;
    private boolean methodMatch = true;

    // リクエスト文字列
    private String requestLine = null;
    private String[] requestSplit = null;

    private int mgetReturnIndex = 1;

    private StringBuilder retGetBuf = new StringBuilder(ImdstDefine.stringBufferMiddleSize);

    private String clientInfo = null;


    /**
     * 初期化
     *
     */
    public void init() {
        this.nextExec = 0;
        this.methodMatch = true;
        this.requestLine = null;
        this.requestSplit = null;
        this.mgetReturnIndex = 1;
    }


    /**
     * 自身が担当する通信対象の情報を設定する.<br>
     *
     * @param clientInfo 通信対象の情報
     */
    public void setClientInfo(String clientInfo) {
        this.clientInfo = clientInfo;
    }


    public String takeRequestLine(BufferedReader br, PrintWriter pw) throws Exception {
        return null;
    }

    /**
     * memcached用のリクエストをパースし共通のプロトコルに変換.<br>
     * 対応しているメソッドはset,get,delete,add,versionのみ.<br>
     *
     * @param br
     * @param pw
     * @return String 
     * @throw Exception
     */
    public String takeRequestLine(CustomReader br, PrintWriter pw) throws Exception {
        return null;
    }

    /**
     * memcached用のリクエストをパースし共通のプロトコルに変換.<br>
     * 対応しているメソッドはset,get,delete,add,versionのみ.<br>
     *
     * @param br
     * @param pw
     * @return String 
     * @throw Exception
     */
    public String[] takeRequestLine4List(CustomReader br, PrintWriter pw) throws Exception {
        String[] retStrs = null;
        this.nextExec = 1;

        String executeMethodStr = br.readLine();

        // Debugログ書き出し
        if (StatusUtil.getDebugOption()) 
            SystemUtil.debugLine(clientInfo + " : Request_head : " + executeMethodStr);

        this.requestLine = executeMethodStr;


        // 切断指定確認
        if (executeMethodStr == null ||
            executeMethodStr.trim().equals("quit") ||
                executeMethodStr.equals(ImdstDefine.imdstConnectExitRequest)) {

            // 接続を切断
            this.nextExec = 3;
            retStrs = new String[1];
            retStrs[0] = executeMethodStr;
            return retStrs;
        }

        // memcacheクライアントの内容からリクエストを作り上げる
        retStrs = this.memcacheMethodCnv(executeMethodStr, br, pw);

        // 以降の処理支持を決定
        if (retStrs == null) this.nextExec = 2;


        // Debugログ書き出し
        if (StatusUtil.getDebugOption()) {

            if(retStrs != null) {
                for (int i = 0; i < retStrs.length; i++) {
                    SystemUtil.debugLine(clientInfo + " : Request_body : " + retStrs[i]);
                }
            } else {
                SystemUtil.debugLine(clientInfo + " : Request_body : null");
            }
        }

        return retStrs;
    }


    /**
     * memcached用のレスポンスを作成.<br>
     * 対応しているメソッドははset,get,delete,add,versionのみ.<br>
     *
     * @param retParams
     * @return String
     * @throw Exception
     */
    public String takeResponseLine(String[] retParams) throws Exception {
        String retStr = "";

        if (retParams != null && retParams.length > 1) {

            retStr = this.memcacheReturnCnv(retParams, null);
            this.nextExec = 1;
        }

        // Debugログ書き出し
        if (StatusUtil.getDebugOption()) 
            SystemUtil.debugLine(clientInfo + " : Response  : " + retStr);

        return retStr;
    }

    /**
     * memcached用のレスポンスを作成.<br>
     * 対応しているメソッドははset,get,delete,add,versionのみ.<br>
     *
     * @param retParams
     * @return String
     * @throw Exception
     */
    public String takeResponseLine(String[] retParams, BufferedOutputStream bos) throws Exception {
        String retStr = "";

        if (retParams != null && retParams.length > 1) {

            retStr = this.memcacheReturnCnv(retParams, bos);
            this.nextExec = 1;
        }

        // Debugログ書き出し
        if (StatusUtil.getDebugOption()) 
            SystemUtil.debugLine(clientInfo + " : Response  : " + retStr);

        return retStr;
    }


    /**
     * 次の動きを指示.<br>
     *
     * @return int 1=正しく処理完了 2=クライアントからのデータ不正 3=接続切断要求
     */
    public int nextExecution() {
        return this.nextExec;
    }

    /**
     * memcachedのプロトコルにマッチしたかを返す.<br>
     *
     * @return boolean true:マッチ false:ノーマッチ
     */
    public boolean isMatchMethod() {
        return this.methodMatch;
    }

    /**
     * memcache用にリクエスト文字を変換する.<br>
     * set,get,delete,add,version<br>
     *
     * @param executeMethodStr
     * @param br
     * @param pw
     * @param soc
     * @return String
     * @Exception
     */
    private String[] memcacheMethodCnv(String executeMethodStr, CustomReader br, PrintWriter pw) throws Exception{
        String[] retStrs = null;
        this.methodMatch = true;

        try {
            executeMethodStr = executeMethodStr.trim();

            String[] executeMethods = executeMethodStr.split(ImdstDefine.memcacheExecuteMethodSep);
            this.requestSplit = executeMethods;
            

            // memcacheの処理方法で分岐
            if (executeMethods[0].equals(ImdstDefine.memcacheExecuteMethodSet)) {
                // Set
                // 分解すると コマンド,key,特有32bit値(Flags),有効期限,格納バイト数

                // 命令文字列の数をチェック
                if (executeMethods.length != 5) {
                    pw.println(ImdstDefine.memcacheMethodReturnErrorComn);
                    pw.flush();
                    return retStrs;
                }

                // 読み込みサイズ指定
                int readSize = Integer.parseInt(executeMethods[4]);

                // サイズチェック
                if (readSize > ImdstDefine.saveDataMaxSize) {
                    br.readLine();
                    pw.print("SERVER_ERROR <Regis Max Byte Over>");
                    pw.print("\r\n");
                    pw.flush();
                    return retStrs;
                }

                // okuyamaプロトコルに変更
                retStrs = new String[5];
                retStrs[0] = "1";
                retStrs[1] = new String(BASE64EncoderStream.encode(executeMethods[1].getBytes()));
                retStrs[2] = ImdstDefine.imdstBlankStrData;
                retStrs[3] = "0";

                byte[] strs = new byte[readSize];
                br.read(strs);

                if (new Integer(br.read()).byteValue() == 13 && new Integer(br.read()).byteValue() == 10) {

                    retStrs[4] = new StringBuilder(new String(BASE64EncoderStream.encode(strs))).append(ImdstDefine.keyHelperClientParamSep).append(this.checkFlagsVal(executeMethods[2])).append(AbstractProtocolTaker.metaColumnSep).append(AbstractProtocolTaker.calcExpireTime(executeMethods[3])).append(AbstractProtocolTaker.metaColumnSep).append(executeMethods[3]).toString();
                }  else {

                    pw.print("CLIENT_ERROR bad data chunk");
                    pw.print("\r\n");
                    pw.flush();
                    retStrs = null;
                    return retStrs;
                }
            } else if (executeMethods[0].equals(ImdstDefine.memcacheExecuteMethodAdd) || executeMethods[0].equals(ImdstDefine.memcacheExecuteMethodAppend)) {

                // Add
                // 分解すると コマンド,key,特有32bit値(Flags),有効期限,格納バイト数
                // 読み込みサイズ指定
                int readSize = Integer.parseInt(executeMethods[4]);

                // 命令文字列の数をチェック
                if (executeMethods.length != 5) {
                    pw.println(ImdstDefine.memcacheMethodReturnErrorComn);
                    pw.flush();
                    return retStrs;
                }

                // サイズチェック
                if (readSize > ImdstDefine.saveDataMaxSize) {
                    br.readLine();
                    pw.print("SERVER_ERROR <Regis Max Byte Over>");
                    pw.print("\r\n");
                    pw.flush();
                    return retStrs;
                }

                // okuyamaプロトコルに変更
                retStrs = new String[5];
                retStrs[0] = "6";
                retStrs[1] = new String(BASE64EncoderStream.encode(executeMethods[1].getBytes()));
                retStrs[2] = ImdstDefine.imdstBlankStrData;
                retStrs[3] = "0";

                byte[] strs = new byte[readSize];
                br.read(strs);

                if (new Integer(br.read()).byteValue() == 13 && new Integer(br.read()).byteValue() == 10) {

                    retStrs[4] = new StringBuilder(new String(BASE64EncoderStream.encode(strs))).append(ImdstDefine.keyHelperClientParamSep).append(this.checkFlagsVal(executeMethods[2])).append(AbstractProtocolTaker.metaColumnSep).append(AbstractProtocolTaker.calcExpireTime(executeMethods[3])).append(AbstractProtocolTaker.metaColumnSep).append(executeMethods[3]).toString();
                }  else {

                    pw.print("CLIENT_ERROR bad data chunk");
                    pw.print("\r\n");
                    pw.flush();
                    retStrs = null;
                    return retStrs;
                }
            } else if (executeMethods[0].equals(ImdstDefine.memcacheExecuteMethodGet)) {

                // Get or Mget
                // 分解すると コマンド,key
                // 命令文字列の数をチェック
                if (executeMethods.length == 1) {
                    pw.println(ImdstDefine.memcacheMethodReturnErrorComn);
                    pw.flush();
                    return retStrs;
                }

                // okuyamaプロトコルに変更
                if (executeMethods.length == 2) {

                    // get
                    retStrs = new String[2];
                    retStrs[0] = "2";
                    retStrs[1] = new String(BASE64EncoderStream.encode(executeMethods[1].getBytes()));
                } else {

                    // mget 
                    // okuyamaプロトコルに変更
                    this.mgetReturnIndex = 1;

                    ArrayList requestWorkList = new ArrayList();
                    ArrayList replaceRequestWorkList = new ArrayList();
                    requestWorkList.add("22");
                    replaceRequestWorkList.add("22");

                    for (int i = 1; i < executeMethods.length; i++) {
                        if (executeMethods[i].length() > 0) {
                            replaceRequestWorkList.add(executeMethods[i]);
                            requestWorkList.add(new String(BASE64EncoderStream.encode(executeMethods[i].getBytes())));
                        }
                    }

                    retStrs = new String[requestWorkList.size()];
                    this.requestSplit = new String[requestWorkList.size()];

                    for (int idx = 0; idx < requestWorkList.size(); idx++) {

                        retStrs[idx] = (String)requestWorkList.get(idx);
                        requestSplit[idx] = (String)replaceRequestWorkList.get(idx);
                    }
                }
            } else if (executeMethods[0].equals(ImdstDefine.memcacheExecuteMethodGets)) {

                // Gets
                // 分解すると コマンド,key
                // 命令文字列の数をチェック
                if (executeMethods.length != 2) {
                    pw.println(ImdstDefine.memcacheMethodReturnErrorComn);
                    pw.flush();
                    return retStrs;
                }

                // okuyamaプロトコルに変更
                retStrs = new String[2];
                retStrs[0] = "15";
                retStrs[1] = new String(BASE64EncoderStream.encode(executeMethods[1].getBytes()));
            } else if (executeMethods[0].equals(ImdstDefine.memcacheExecuteMethodCas)) {

                // cas
                // 分解すると コマンド,key,特有32bit値(Flags),有効期限,格納バイト数,cas値
                // 読み込みサイズ指定
                int readSize = Integer.parseInt(executeMethods[4]);

                // 命令文字列の数をチェック
                if (executeMethods.length != 6) {
                    pw.println(ImdstDefine.memcacheMethodReturnErrorComn);
                    pw.flush();
                    return retStrs;
                }

                // サイズチェック
                if (readSize > ImdstDefine.saveDataMaxSize) {
                    br.readLine();
                    pw.print("SERVER_ERROR <Regis Max Byte Over>");
                    pw.print("\r\n");
                    pw.flush();
                    return retStrs;
                }

                // okuyamaプロトコルに変更
                retStrs = new String[6];
                retStrs[0] = "16";
                retStrs[1] = new String(BASE64EncoderStream.encode(executeMethods[1].getBytes()));
                retStrs[2] = ImdstDefine.imdstBlankStrData;
                retStrs[3] = "0";
                retStrs[5] = executeMethods[5]; // cas値

                byte[] strs = new byte[readSize];
                br.read(strs);

                if (new Integer(br.read()).byteValue() == 13 && new Integer(br.read()).byteValue() == 10) {

                    retStrs[4] = new StringBuilder(new String(BASE64EncoderStream.encode(strs))).append(ImdstDefine.keyHelperClientParamSep).
                                                                                                 append(this.checkFlagsVal(executeMethods[2])).
                                                                                                 append(AbstractProtocolTaker.metaColumnSep).
                                                                                                 append(AbstractProtocolTaker.calcExpireTime(executeMethods[3])).
                                                                                                 append(AbstractProtocolTaker.metaColumnSep).
                                                                                                 append(executeMethods[3]).
                                                                                                 toString();
                }  else {

                    pw.print("CLIENT_ERROR bad data chunk");
                    pw.print("\r\n");
                    pw.flush();
                    retStrs = null;
                    return retStrs;
                }

            } else if (executeMethods[0].equals(ImdstDefine.memcacheExecuteMethodDelete)) {

                // Delete
                // 分解すると コマンド,key
                // 命令文字列の数をチェック
                if (executeMethods.length != 3 && executeMethods.length != 2) {
                    pw.println(ImdstDefine.memcacheMethodReturnErrorComn);
                    pw.flush();
                    return retStrs;
                }

                // okuyamaプロトコルに変更
                retStrs = new String[3];
                retStrs[0] = "5";
                retStrs[1] = new String(BASE64EncoderStream.encode(executeMethods[1].getBytes()));
                retStrs[2] = "0"; // TransactionCode("0"固定)
            } else if (executeMethods[0].equals(ImdstDefine.memcacheExecuteMethodIncr)) {

                // Incr
                // 分解すると コマンド,key
                // 命令文字列の数をチェック
                if (executeMethods.length != 3) {
                    pw.println(ImdstDefine.memcacheMethodReturnErrorComn);
                    pw.flush();
                    return retStrs;
                }

                // okuyamaプロトコルに変更
                retStrs = new String[4];
                retStrs[0] = "13";
                retStrs[1] = new String(BASE64EncoderStream.encode(executeMethods[1].getBytes()));
                retStrs[2] = "0"; // TransactionCode("0"固定)
                retStrs[3] = new String(BASE64EncoderStream.encode(executeMethods[2].getBytes()));
            } else if (executeMethods[0].equals(ImdstDefine.memcacheExecuteMethodDecr)) {

                // Decr
                // 分解すると コマンド,key
                // 命令文字列の数をチェック
                if (executeMethods.length != 3) {
                    pw.println(ImdstDefine.memcacheMethodReturnErrorComn);
                    pw.flush();
                    return retStrs;
                }

                // okuyamaプロトコルに変更
                retStrs = new String[4];
                retStrs[0] = "14";
                retStrs[1] = new String(BASE64EncoderStream.encode(executeMethods[1].getBytes()));
                retStrs[2] = "0"; // TransactionCode("0"固定)
                retStrs[3] = new String(BASE64EncoderStream.encode(executeMethods[2].getBytes()));
            } else if (executeMethods[0].equals(ImdstDefine.memcacheExecuteMethodVersion)) {
            
                // version
                retStrs = new String[1];
                retStrs[0] = "999";
            } else if (executeMethods[0].equals(ImdstDefine.memcacheExecuteMethodFlushall)) {
            
                // version
                retStrs = new String[1];
                retStrs[0] = "61";
            } else {

                // 存在しないプロトコルはokuyama用として処理する。
                try {
                    // 数値変換出来ない場合はエラー
                    retStrs = executeMethodStr.split(ImdstDefine.keyHelperClientParamSep);
                    Integer.parseInt(retStrs[0]);
                } catch (NumberFormatException e) {

                    pw.print("ERROR");
                    pw.print("\r\n");
                    pw.flush();
                    retStrs = null;
                    return retStrs;
                }

                this.methodMatch = false;
            }
        } catch(Exception e) {
            throw e;
        }
        return retStrs;
    }


    /**
     * memcache用に返却文字を加工する
     */
    private String memcacheReturnCnv(String[] retParams, BufferedOutputStream bos) throws Exception{
        String retStr = null;

        if (retParams[0] == null) {

            retStr = ImdstDefine.memcacheMethodRetrunServerError;
        } else if (retParams[0].equals("1")) {

            // Set
            // 返却値は<STORED> or <SERVER_ERROR>
            if (retParams[1].equals("true")) {
                retStr = ImdstDefine.memcacheMethodReturnSuccessSet;
            } else if (retParams[1].equals("false") || retParams[1].equals("null")) {
                retStr = ImdstDefine.memcacheMethodRetrunServerError + retParams[2];
            }
        } else if (retParams[0].equals("2")) {

            // Get
            // 返却値は"VALUE キー値 hashcode byteサイズ \r\n 値 \r\n END
            String[] valueSplit = null;


            byte[] valueByte = null;
            String[] metaColumns = null;

            if (retParams[1].equals("true")) {

                valueSplit = retParams[2].split(ImdstDefine.keyHelperClientParamSep);

                if (valueSplit.length > 1) 
                    metaColumns = valueSplit[1].split(AbstractProtocolTaker.metaColumnSep);

                // 有効期限チェックも同時に行う(memcachedのみ)
                if (valueSplit.length < 2 || AbstractProtocolTaker.expireCheck(metaColumns[1])) {

                    this.writeStreamData("VALUE", bos);
                    this.writeStreamData(ImdstDefine.memcacheExecuteMethodSep, bos);
                    this.writeStreamData(this.requestSplit[1], bos);
                    this.writeStreamData(ImdstDefine.memcacheExecuteMethodSep, bos);

                    if (valueSplit.length < 2) {
                        this.writeStreamData("0", bos);
                    } else {
                        this.writeStreamData(metaColumns[0], bos);
                    }

                    valueByte = BASE64DecoderStream.decode(valueSplit[0].getBytes());
                    this.writeStreamData(ImdstDefine.memcacheExecuteMethodSep, bos);
                    this.writeStreamData(valueByte.length, bos);
                    this.writeStreamData("\r\n", bos);

                    this.writeStreamData(valueByte, bos);
                    this.writeStreamData("\r\n", bos);
                }
            }
            this.writeStreamData("END\r\n", bos);
            bos.flush();

            // ブランクを返却することで、上位レイヤでflushをさせないように制御
            retStr = "";

        } else if (retParams[0].equals("22")) {

            // Mget
            // 返却値は"VALUE キー値 hashcode byteサイズ \r\n 値 \r\n
            String[] valueSplit = null;

            byte[] valueByte = null;
            String[] metaColumns = null;

            if (retParams[1].equals("true")) {

                valueSplit = retParams[2].split(ImdstDefine.keyHelperClientParamSep);

                if (valueSplit.length > 1) 
                    metaColumns = valueSplit[1].split(AbstractProtocolTaker.metaColumnSep);

                // 有効期限チェックも同時に行う(memcachedのみ)
                if (valueSplit.length < 2 || AbstractProtocolTaker.expireCheck(metaColumns[1])) {

                    this.writeStreamData("VALUE", bos);
                    this.writeStreamData(ImdstDefine.memcacheExecuteMethodSep, bos);
                    this.writeStreamData(this.requestSplit[this.mgetReturnIndex], bos);
                    this.writeStreamData(ImdstDefine.memcacheExecuteMethodSep, bos);

                    if (valueSplit.length < 2) {
                        this.writeStreamData("0", bos);
                    } else {
                        this.writeStreamData(metaColumns[0], bos);
                    }

                    valueByte = BASE64DecoderStream.decode(valueSplit[0].getBytes());
                    this.writeStreamData(ImdstDefine.memcacheExecuteMethodSep, bos);
                    this.writeStreamData(valueByte.length, bos);
                    this.writeStreamData("\r\n", bos);
                    this.writeStreamData(valueByte, bos);
                    this.writeStreamData("\r\n", bos);
                    bos.flush();
                }
            }

            // ブランクを返却することで、上位レイヤでflushをさせないように制御
            retStr = "";
            this.mgetReturnIndex++;
        } else if (retParams[0].equals("22-f")) {

            // MGetの最終値
            // 返却値は"VALUE キー値 hashcode byteサイズ \r\n 値 \r\n END
            String[] valueSplit = null;

            byte[] valueByte = null;
            String[] metaColumns = null;

            if (retParams[1].equals("true")) {

                valueSplit = retParams[2].split(ImdstDefine.keyHelperClientParamSep);

                if (valueSplit.length > 1) 
                    metaColumns = valueSplit[1].split(AbstractProtocolTaker.metaColumnSep);

                // 有効期限チェックも同時に行う(memcachedのみ)
                if (valueSplit.length < 2 || AbstractProtocolTaker.expireCheck(metaColumns[1])) {

                    this.writeStreamData("VALUE", bos);
                    this.writeStreamData(ImdstDefine.memcacheExecuteMethodSep, bos);
                    this.writeStreamData(this.requestSplit[this.mgetReturnIndex], bos);
                    this.writeStreamData(ImdstDefine.memcacheExecuteMethodSep, bos);

                    if (valueSplit.length < 2) {
                        this.writeStreamData("0", bos);
                    } else {
                        this.writeStreamData(metaColumns[0], bos);
                    }

                    valueByte = BASE64DecoderStream.decode(valueSplit[0].getBytes());
                    this.writeStreamData(ImdstDefine.memcacheExecuteMethodSep, bos);
                    this.writeStreamData(valueByte.length, bos);
                    this.writeStreamData("\r\n", bos);
                    this.writeStreamData(valueByte, bos);
                    this.writeStreamData("\r\n", bos);

                }
            }
            this.writeStreamData("END\r\n", bos);
            bos.flush();

            // ブランクを返却することで、上位レイヤでflushをさせないように制御
            retStr = "";
        } else if (retParams[0].equals("6")) {

            // Add
            // 返却値は<STORED> or <SERVER_ERROR> or <NOT_STORED>
            if (retParams[1].equals("true")) {
                retStr = ImdstDefine.memcacheMethodReturnSuccessSet;
            } else if (retParams[1].equals("false") || retParams[1].equals(ImdstDefine.keyNodeKeyNewRegistErrMsg)) {
                retStr = ImdstDefine.memcacheMethodReturnErrorAdd;
            } else if (retParams[1].equals("false") || retParams[1].equals("null")) {
                retStr = ImdstDefine.memcacheMethodRetrunServerError + retParams[2];
            }
        } else if (retParams[0].equals("15")) {

            // Gets
            // 返却値は"VALUE キー値 hashcode byteサイズ casユニーク値 \r\n 値 \r\n END
            String[] valueSplit = null;

            byte[] valueByte = null;
            String[] metaColumns = null;

            if (retParams[1].equals("true")) {

                valueSplit = retParams[2].split(ImdstDefine.keyHelperClientParamSep);

                if (valueSplit.length > 1) 
                    metaColumns = valueSplit[1].split(AbstractProtocolTaker.metaColumnSep);

                if (valueSplit.length < 2 || AbstractProtocolTaker.expireCheck(metaColumns[1])) {

                    this.writeStreamData("VALUE", bos);
                    this.writeStreamData(ImdstDefine.memcacheExecuteMethodSep, bos);
                    this.writeStreamData(this.requestSplit[1], bos);
                    this.writeStreamData(ImdstDefine.memcacheExecuteMethodSep, bos);

                    if (valueSplit.length < 2) {
                        this.writeStreamData("0", bos);
                    } else {
                        this.writeStreamData(metaColumns[0], bos);
                    }

                    valueByte = BASE64DecoderStream.decode(valueSplit[0].getBytes());
                    this.writeStreamData(ImdstDefine.memcacheExecuteMethodSep, bos);
                    this.writeStreamData(valueByte.length, bos);
                    this.writeStreamData(ImdstDefine.memcacheExecuteMethodSep, bos);
                    this.writeStreamData(retParams[3], bos);
                    this.writeStreamData("\r\n", bos);
                    this.writeStreamData(valueByte, bos);
                    this.writeStreamData("\r\n", bos);
                    bos.flush();
                }
            }

            this.writeStreamData("END\r\n", bos);
            bos.flush();

            // ブランクを返却することで、上位レイヤでflushをさせないように制御
            retStr = "";
        } else if (retParams[0].equals("16")) {

            // cas
            // 返却値は<STORED> or <EXISTS>
            if (retParams[1].equals("true")) {
                retStr = ImdstDefine.memcacheMethodReturnSuccessSet;
            } else if (retParams[1].equals("false") || retParams[1].equals(ImdstDefine.keyNodeKeyUpdatedErrMsg)) {
                retStr = ImdstDefine.memcacheMethodReturnErrorCas;
            } else if (retParams[1].equals("false") || retParams[1].equals("null")) {
                retStr = ImdstDefine.memcacheMethodRetrunServerError + retParams[2];
            }
        } else if (retParams[0].equals("5")) {

            // Delete
            // 返却値は"DELETED or NOT_FOUND

            if (retParams[1].equals("true")) {

                retStr = ImdstDefine.memcacheMethodReturnSuccessDelete;
            } else if (retParams[1].equals("false")) {
                retStr = ImdstDefine.memcacheMethodReturnErrorDelete;
            } else {
                retStr = ImdstDefine.memcacheMethodRetrunServerError;
            }
        } else if (retParams[0].equals("13")) {

            // Incr
            // 返却値は"数値 or NOT_FOUND"

            if (retParams[1].equals("true")) {

                retStr = new String(BASE64DecoderStream.decode(retParams[2].getBytes()));
            } else if (retParams[1].equals("false") || retParams[1].equals("error")) {
                retStr = ImdstDefine.memcacheMethodReturnErrorDelete;
            }
        } else if (retParams[0].equals("14")) {

            // Decr
            // 返却値は"数値 or NOT_FOUND"

            if (retParams[1].equals("true")) {

                retStr = new String(BASE64DecoderStream.decode(retParams[2].getBytes()));
            } else if (retParams[1].equals("false") || retParams[1].equals("error")) {
                retStr = ImdstDefine.memcacheMethodReturnErrorDelete;
            }
        } else if (retParams[0].equals("999")) {

            // version
            // 返却値は"okuyama-?-?-?
            retStr = retParams[1];
        } else if (retParams[0].equals("61")) {

            // flush_all
            // 返却値は"OK or ERROR
            if (retParams.length < 2 || !retParams[1].equals("true")) {
                retStr = "ERROR";
            } else if (retParams[1].equals("true")) {
                retStr = "OK";
            }
        } else {
            StringBuilder retParamBuf = new StringBuilder(ImdstDefine.stringBufferSmallSize);

            // 存在しないメソッドはokuyama用として処理する
            retParamBuf.append(retParams[0]);
            retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
            retParamBuf.append(retParams[1]);
            retParamBuf.append(ImdstDefine.keyHelperClientParamSep);

            // 返却値に区切り文字が入っている場合は区切り文字より左辺のみ返す
            if (retParams[2] != null) {
                retParamBuf.append(((String[])retParams[2].split(ImdstDefine.keyHelperClientParamSep))[0]);
            }
            retStr = retParamBuf.toString();
        }

        this.nextExec = 1;
        return retStr;
    }

    private void responseDebug (byte[] strBytes) {
        if (StatusUtil.getDebugOption()) {
            try {
                SystemUtil.debugLine(clientInfo + " : Response  : " + new String(strBytes, "utf-8"));
            } catch (Exception e) {}
        }
    }

    private void writeStreamData(int num, BufferedOutputStream bos) throws Exception {
        this.writeStreamData(new Integer(num).toString().getBytes(), bos);
    }

    private void writeStreamData(String str, BufferedOutputStream bos) throws Exception {
        this.writeStreamData(str.getBytes(), bos);
    }

    private void writeStreamData(byte[] data, BufferedOutputStream bos) throws Exception {
        try {
            if (data.length > 0) {
                bos.write(data, 0, data.length);
                responseDebug(data);
            }
        } catch (Exception e) {
            throw e;
        }
    }
}