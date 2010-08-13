package org.imdst.util.protocol;

import java.io.*;
import java.util.*;
import java.net.*;

import com.sun.mail.util.BASE64DecoderStream;
import com.sun.mail.util.BASE64EncoderStream;

import org.imdst.util.ImdstDefine;

/**
 * クライアントとのProtocolの差を保管する.<br>
 * memcacheProtocol用のTaker.<br>
 *
 */
public class MemcachedProtocolTaker implements IProtocolTaker {

    private int nextExec = 0;
    private boolean methodMatch = true;

    // リクエスト文字列
    private String requestLine = null;
    private String[] requestSplit = null;

    /**
     * memcache用のリクエストをパースし共通のプロトコルに変換.<br>
     * 対応しているメソッドはset,get,deleteのみ.<br>
     *
     *
     */
    public String takeRequestLine(BufferedReader br, PrintWriter pw) throws Exception {
        String retStr = null;
        this.nextExec = 1;

        // memcache時に使用するのは取り合えずは命令部分と、データ部分のみ
        StringBuffer methodBuf = new StringBuffer();

        String executeMethodStr = br.readLine();

        this.requestLine = executeMethodStr;

        // 切断指定確認
        if (executeMethodStr == null ||
            executeMethodStr.trim().equals("quit") ||
                executeMethodStr.equals(ImdstDefine.imdstConnectExitRequest)) {

            // 接続を切断
            this.nextExec = 3;
            return executeMethodStr;
        }

        // memcacheクライアントの内容からリクエストを作り上げる
        retStr = this.memcacheMethodCnv(executeMethodStr, br, pw);

        if (retStr == null) this.nextExec = 2;

        return retStr;
    }


    /**
     * memcache用のレスポンスを作成.<br>
     * 対応しているメソッドはset,get,deleteのみ.<br>
     *
     */
    public String takeResponseLine(String[] retParams) throws Exception {
        String retStr = "";

        if (retParams != null && retParams.length > 1) {

            retStr = this.memcacheReturnCnv(retParams);
            this.nextExec = 1;
        }
        return retStr;
    }


    /**
     * 次の動きを指示.<br>
     *
     */
    public int nextExecution() {
        return this.nextExec;
    }

    /**
     * memcacheのプロトコルにマッチしたかを返す.<br>
     * @return boolean true:マッチ false:ノーマッチ
     */
    public boolean isMatchMethod() {
        return this.methodMatch;
    }

    /**
     * memcache用にリクエスト文字を変換する.<br>
     *
     */
    private String memcacheMethodCnv(String executeMethodStr, BufferedReader br, PrintWriter pw) throws Exception{
        String retStr = null;
        this.methodMatch = true;

        try {
            executeMethodStr = executeMethodStr.trim();
            String[] executeMethods = executeMethodStr.split(ImdstDefine.memcacheExecuteMethodSep);
            this.requestSplit = executeMethods;
            StringBuffer methodBuf = new StringBuffer();

            // memcacheの処理方法で分岐
            if (executeMethods[0].equals(ImdstDefine.memcacheExecuteMethodSet)) {
                // Set
                // 分解すると コマンド,key,特有32bit値(Flags),有効期限,格納バイト数

                // 命令文字列の数をチェック
                if (executeMethods.length != 5) {
                    pw.println(ImdstDefine.memcacheMethodReturnErrorComn);
                    pw.flush();
                    return retStr;
                }

                // サイズチェック
                if (Integer.parseInt(executeMethods[4]) > ImdstDefine.saveDataMaxSize) {
                    br.readLine();
                    pw.println("SERVER_ERROR <Regis Max Byte Over>");
                    pw.flush();
                    return retStr;
                }

                // TODO:連結してまた分解って。。。後で考えます
                methodBuf.append("1");
                methodBuf.append(ImdstDefine.keyHelperClientParamSep);
                methodBuf.append(new String(BASE64EncoderStream.encode(executeMethods[1].getBytes())));
                methodBuf.append(ImdstDefine.keyHelperClientParamSep);
                methodBuf.append(ImdstDefine.imdstBlankStrData);
                methodBuf.append(ImdstDefine.keyHelperClientParamSep);
                methodBuf.append("0");                                  // TransactionCode(0固定)
                methodBuf.append(ImdstDefine.keyHelperClientParamSep);

                byte[] strs = new byte[Integer.parseInt(executeMethods[4])];
                for (int i = 0; i < Integer.parseInt(executeMethods[4]); i++) {
                    strs[i] = new Integer(br.read()).byteValue();
                }
                if (new Integer(br.read()).byteValue() == 13 && new Integer(br.read()).byteValue() == 10) {

                    String workStr = new String(strs);
                    //String workStr = br.readLine();

                    // 改行文字が含まれているため切除する
                    /*byte[] workBytes = workStr.getBytes();
                    byte[] cnvBytes = new byte[workBytes.length - 1];
                    System.arraycopy(workBytes, 1, cnvBytes, 0, (workBytes.length - 1));*/
                    byte[] cnvBytes = workStr.getBytes();
                    methodBuf.append(new String(BASE64EncoderStream.encode(cnvBytes)) + ImdstDefine.keyHelperClientParamSep + executeMethods[2]);
                    retStr = methodBuf.toString();
                }  else {

                    pw.println("CLIENT_ERROR bad data chunk");
                    pw.flush();
                    return retStr;
                }
            } else if (executeMethods[0].equals(ImdstDefine.memcacheExecuteMethodAdd) || executeMethods[0].equals(ImdstDefine.memcacheExecuteMethodAppend)) {

                // Append
                // 分解すると コマンド,key,特有32bit値(Flags),有効期限,格納バイト数

                // 命令文字列の数をチェック
                if (executeMethods.length != 5) {
                    pw.println(ImdstDefine.memcacheMethodReturnErrorComn);
                    pw.flush();
                    return retStr;
                }

                // サイズチェック
                if (Integer.parseInt(executeMethods[4]) > ImdstDefine.saveDataMaxSize) {
                    br.readLine();
                    pw.println("SERVER_ERROR <Regis Max Byte Over>");
                    pw.flush();
                    return retStr;
                }

                // TODO:連結してまた分解って。。。後で考えます
                methodBuf.append("6");
                methodBuf.append(ImdstDefine.keyHelperClientParamSep);
                methodBuf.append(new String(BASE64EncoderStream.encode(executeMethods[1].getBytes())));
                methodBuf.append(ImdstDefine.keyHelperClientParamSep);
                methodBuf.append(ImdstDefine.imdstBlankStrData);
                methodBuf.append(ImdstDefine.keyHelperClientParamSep);
                methodBuf.append("0");                                  // TransactionCode(0固定)
                methodBuf.append(ImdstDefine.keyHelperClientParamSep);
                
                String workStr = br.readLine();

                // 改行文字が含まれているため切除する
                /*byte[] workBytes = workStr.getBytes();
                byte[] cnvBytes = new byte[workBytes.length - 1];
                System.arraycopy(workBytes, 1, cnvBytes, 0, (workBytes.length - 1));*/
                byte[] cnvBytes = workStr.getBytes();
                methodBuf.append(new String(BASE64EncoderStream.encode(cnvBytes)) + ImdstDefine.keyHelperClientParamSep + executeMethods[2]);
                retStr = methodBuf.toString();

                
            } else if (executeMethods[0].equals(ImdstDefine.memcacheExecuteMethodGet)) {

                // Get
                // 分解すると コマンド,key
                // 命令文字列の数をチェック
                if (executeMethods.length != 2) {
                    pw.println(ImdstDefine.memcacheMethodReturnErrorComn);
                    pw.flush();
                    return retStr;
                }

                // TODO:連結してまた分解って。。。後で考えます
                methodBuf.append("2");
                methodBuf.append(ImdstDefine.keyHelperClientParamSep);
                methodBuf.append(new String(BASE64EncoderStream.encode(executeMethods[1].getBytes())));
                retStr = methodBuf.toString();
            } else if (executeMethods[0].equals(ImdstDefine.memcacheExecuteMethodDelete)) {

                // Delete
                // 分解すると コマンド,key
                // 命令文字列の数をチェック
                if (executeMethods.length != 2) {
                    pw.println(ImdstDefine.memcacheMethodReturnErrorComn);
                    pw.flush();
                    return retStr;
                }

                // TODO:連結してまた分解って。。。後で考えます
                methodBuf.append("5");
                methodBuf.append(ImdstDefine.keyHelperClientParamSep);
                methodBuf.append(new String(BASE64EncoderStream.encode(executeMethods[1].getBytes())));
                methodBuf.append(ImdstDefine.keyHelperClientParamSep);
                methodBuf.append("0"); // TransactionCode("0"固定)
                retStr = methodBuf.toString();

            } else {
                // 存在しないプロトコルはokuyama用として処理する。
                retStr = executeMethodStr;
                this.methodMatch = false;
            }
        } catch(Exception e) {
            throw e;
        }
        return retStr;
    }

    /**
     * memcache用に返却文字を加工する
     */
    private String memcacheReturnCnv(String[] retParams) throws Exception{
        String retStr = null;

        if (retParams[0].equals("1")) {

            // Set
            // 返却値は<STORED> or <SERVER_ERROR>
            if (retParams[1].equals("true")) {
                retStr = ImdstDefine.memcacheMethodReturnSuccessSet;
            } else if (retParams[1].equals("false") || retParams[1].equals("null")) {
                retStr = ImdstDefine.memcacheMethodRetrunServerError + retParams[2];
            }
        } else if (retParams[0].equals("6")) {
            // Set
            // 返却値は<STORED> or <SERVER_ERROR> or <NOT_STORED>
            if (retParams[1].equals("true")) {
                retStr = ImdstDefine.memcacheMethodReturnSuccessSet;
            } else if (retParams[1].equals("false") || retParams[1].equals(ImdstDefine.keyNodeKeyNewRegistErrMsg)) {
                retStr = ImdstDefine.memcacheMethodReturnErrorAdd;
            } else if (retParams[1].equals("false") || retParams[1].equals("null")) {
                retStr = ImdstDefine.memcacheMethodRetrunServerError + retParams[2];
            }
        } else if (retParams[0].equals("2")) {

            // Get
            // 返却値は"VALUE キー値 hashcode byteサイズ \r\n 値 \r\n END
            StringBuffer retBuf = new StringBuffer();
            String[] valueSplit = null;
            byte[] valueByte = null;

            if (retParams[1].equals("true")) {
                retBuf.append("VALUE");
                retBuf.append(ImdstDefine.memcacheExecuteMethodSep);
                retBuf.append(this.requestSplit[1]);
                retBuf.append(ImdstDefine.memcacheExecuteMethodSep);

                valueSplit = retParams[2].split(ImdstDefine.keyHelperClientParamSep);
                if (valueSplit.length < 2) {
                    retBuf.append("0");
                } else {
                    retBuf.append(valueSplit[1]);
                }

                valueByte = BASE64DecoderStream.decode(valueSplit[0].getBytes());
                retBuf.append(ImdstDefine.memcacheExecuteMethodSep);
                retBuf.append(valueByte.length);
                retBuf.append("\r\n");
                retBuf.append(new String(valueByte));
                retBuf.append("\r\n");

            }
            retBuf.append("END");

            retStr = retBuf.toString();
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

        } else {
            StringBuffer retParamBuf = new StringBuffer();

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

}