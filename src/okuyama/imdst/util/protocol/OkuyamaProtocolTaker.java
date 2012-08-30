package okuyama.imdst.util.protocol;

import java.io.*;
import java.util.*;
import java.net.*;

import okuyama.imdst.util.StatusUtil;
import okuyama.imdst.util.SystemUtil;
import okuyama.imdst.util.ImdstDefine;
import okuyama.imdst.util.io.CustomReader;


/**
 * クライアントとのProtocolの差を保管する.<br>
 * okuyamaの標準Protocol用のTaker.<br>
 *
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class OkuyamaProtocolTaker extends AbstractProtocolTaker implements IProtocolTaker {

    private int nextExec = 0;

    private boolean methodMatch = true;

    private StringBuilder retParamBuf = new StringBuilder(ImdstDefine.stringBufferMiddleSize);

    private String clientInfo = null;

    private static String checkSetMethodCodeSet = "1";

    private static String checkSetMethodCodeAdd = "6";


    /**
     * 初期化
     *
     */
    public void init() {
        this.nextExec = 0;
        this.methodMatch = true;
    }


    /**
     * 自身が担当する通信対象の情報を設定する.<br>
     *
     * @param clientInfo 通信対象の情報
     */
    public void setClientInfo(String clientInfo) {
        this.clientInfo = clientInfo;
    }


    /**
     * okuyama用のリクエストをパースし共通のプロトコルに変換.<br>
     * 未使用.<br>
     *
     * @param is
     * @param pw
     * @return String[] 
     * @throw Exception
     */
    public String[] takeRequestLine(InputStream is, PrintWriter pw) throws Exception {
        String retStrs[] = new String[5];

        byte[] data = new byte[1];
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        int counter = 0;
        while (is.read(data, 0, 1) != -1) {

            if (data[0] == 44) {

                retStrs[counter] = baos.toString().trim();
                baos = new ByteArrayOutputStream();
                counter++;
            } else if (data[0] == 10) {

                retStrs[counter] = baos.toString().trim();
                break;
            } else if (data[0] != 13) {
                baos.write(data, 0, 1);
            }
        }

        // 切断指定確認
        if (retStrs[0] == null ||
                retStrs[0].equals("") ||
                    retStrs[0].equals(ImdstDefine.imdstConnectExitRequest)) {
            // 接続を切断
            this.nextExec = 3;
        }

        // Debugログ書き出し
        if (StatusUtil.getDebugOption()) 
            SystemUtil.debugLine("Request  : " + retStrs.toString());

        return retStrs;
    }


    public String takeRequestLine(BufferedReader br, PrintWriter pw) throws Exception {
        return null;
    }

    /**
     * okuyama用のリクエストをパースし共通のプロトコルに変換.<br>
     *
     * @param br
     * @param pw
     * @return String[] 
     * @throw Exception
     */
    public String takeRequestLine(CustomReader br, PrintWriter pw) throws Exception {
        String retStr = null;
        retStr = br.readLine();

        // 切断指定確認
        if (retStr == null ||
                retStr.equals("") ||
                    retStr.equals(ImdstDefine.imdstConnectExitRequest)) {
            // 接続を切断
            this.nextExec = 3;
        }

        // Debugログ書き出し
        if (StatusUtil.getDebugOption()) 
            SystemUtil.debugLine(clientInfo + " : Request  : " + retStr);

        
        return retStr;
    }


    public String[] takeRequestLine4List(CustomReader br, PrintWriter pw) throws Exception {
        String retStr = null;
        retStr = br.readLine();

        // 切断指定確認
        if (retStr == null ||
                retStr.equals("") ||
                    retStr.equals(ImdstDefine.imdstConnectExitRequest)) {
            // 接続を切断
            this.nextExec = 3;
        }

        // Debugログ書き出し
        if (StatusUtil.getDebugOption()) 
            SystemUtil.debugLine(clientInfo + " : Request  : " + retStr);

        
        return okuyamaMethodCnv(retStr);
    }


    private String[] okuyamaMethodCnv(String executeMethodStr) {
        int methodLen = executeMethodStr.length();

        String[] splitMethodSet = null;
        if (executeMethodStr.indexOf("2,") == 0) {

            splitMethodSet = new String[2];
            splitMethodSet[0] = "2";
            splitMethodSet[1] = executeMethodStr.substring(2);
        } else if (executeMethodStr.indexOf("1,") == 0) {

            splitMethodSet = SystemUtil.fastSplit(executeMethodStr, ImdstDefine.keyHelperClientParamSep);
        } else {
            splitMethodSet = executeMethodStr.split(ImdstDefine.keyHelperClientParamSep);
        }

        if (splitMethodSet[0].equals(checkSetMethodCodeSet) || splitMethodSet[0].equals(checkSetMethodCodeAdd)) {
            
            if (executeMethodStr.charAt(methodLen - 1) == ',') {

                // 有効期限付き
                String[] retMethod = new String[5] ;

                StringBuilder requestStrBuf = new StringBuilder(methodLen + 20);
                
                executeMethodStr = null;

                retMethod[0] = splitMethodSet[0];
                retMethod[1] = splitMethodSet[1];
                retMethod[2] = splitMethodSet[2];
                retMethod[3] = splitMethodSet[3];

                requestStrBuf.append(splitMethodSet[4]);
                requestStrBuf.append(ImdstDefine.keyHelperClientParamSep);
                requestStrBuf.append("0");
                requestStrBuf.append(AbstractProtocolTaker.metaColumnSep).
                              append(AbstractProtocolTaker.calcExpireTime(splitMethodSet[5])).
                              append(AbstractProtocolTaker.metaColumnSep).
                              append(splitMethodSet[5]);
                retMethod[4] = requestStrBuf.toString();
                return retMethod;
            } else {
                return splitMethodSet;
            }
        } else {
            
            return splitMethodSet;
        }
    }

    /**
     * okuyama用のレスポンスを作成.<br>
     *
     * @param retParams
     * @return String
     * @throw Exception
     */
    public String takeResponseLine(String[] retParams,  BufferedOutputStream bos) throws Exception {
        return this.takeResponseLine(retParams);
    }

    /**
     * okuyama用のレスポンスを作成.<br>
     *
     * @param retParams
     * @return String
     * @throw Exception
     */
    public String takeResponseLine(String[] retParams) throws Exception {
        if (this.retParamBuf.length() > 0)
            this.retParamBuf.delete(0, Integer.MAX_VALUE);

        if (retParams != null && retParams.length > 1) {

            // getValue or getValueVerionCheckの場合
            if (retParams[0].equals("2") || retParams[0].equals("15") || retParams[0].equals("17")) {
                String[] metaColumns = null;

                String[] valueSplit = null;
                int metaDataSepPoint = retParams[2].indexOf(ImdstDefine.keyHelperClientParamSep);
                if(metaDataSepPoint != -1) {
                    valueSplit = new String[2];
                    valueSplit[0] = retParams[2].substring(0, metaDataSepPoint);
                    valueSplit[1] = retParams[2].substring(metaDataSepPoint+1);
                } else {
                    valueSplit = new String[1];
                    valueSplit[0] = retParams[2];
                }

                if (valueSplit.length > 1) 
                    metaColumns = valueSplit[1].split(AbstractProtocolTaker.metaColumnSep);

                // 有効期限チェックも同時に行う
                if (valueSplit.length < 2 || AbstractProtocolTaker.expireCheck(metaColumns[1])) {
                    this.retParamBuf.append(retParams[0]);
                    this.retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                    this.retParamBuf.append(retParams[1]);
                    this.retParamBuf.append(ImdstDefine.keyHelperClientParamSep);

                    // 返却値に区切り文字が入っている場合は区切り文字より左辺のみ返す
                    if (retParams.length > 2 && retParams[2] != null) {
                        this.retParamBuf.append(((String[])retParams[2].split(ImdstDefine.keyHelperClientParamSep))[0]);
                    }

                    // 返却値に区切り文字が入っている場合は区切り文字より左辺のみ返す
                    if (retParams.length > 3 && retParams[3] != null) {
                        this.retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                        this.retParamBuf.append(retParams[3]);
                    }
                } else {
                    this.retParamBuf.append(retParams[0]);
                    this.retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                    this.retParamBuf.append("false");
                    this.retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                }
            } else if (retParams[0].equals("22")) {
                String[] metaColumns = null;
                String[] valueSplit = retParams[2].split(ImdstDefine.keyHelperClientParamSep);

                if (valueSplit.length > 1) 
                    metaColumns = valueSplit[1].split(AbstractProtocolTaker.metaColumnSep);

                // 有効期限チェックも同時に行う
                if (valueSplit.length < 2 || AbstractProtocolTaker.expireCheck(metaColumns[1])) {
                    this.retParamBuf.append(retParams[0]);
                    this.retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                    this.retParamBuf.append(retParams[1]);
                    this.retParamBuf.append(ImdstDefine.keyHelperClientParamSep);

                    // 返却値に区切り文字が入っている場合は区切り文字より左辺のみ返す
                    if (retParams.length > 2 && retParams[2] != null) {
                        this.retParamBuf.append(((String[])retParams[2].split(ImdstDefine.keyHelperClientParamSep))[0]);
                    }

                    // 返却値に区切り文字が入っている場合は区切り文字より左辺のみ返す
                    if (retParams.length > 3 && retParams[3] != null) {
                        this.retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                        this.retParamBuf.append(retParams[3]);
                    }
                } else {
                    this.retParamBuf.append(retParams[0]);
                    this.retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                    this.retParamBuf.append("false");
                    this.retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                }
                this.retParamBuf.append("\n");
            } else if (retParams[0].equals("22-f")) {
                String[] metaColumns = null;
                String[] valueSplit = retParams[2].split(ImdstDefine.keyHelperClientParamSep);
                
                retParams[0] = "22";
                if (valueSplit.length > 1) 
                    metaColumns = valueSplit[1].split(AbstractProtocolTaker.metaColumnSep);

                // 有効期限チェックも同時に行う
                if (valueSplit.length < 2 || AbstractProtocolTaker.expireCheck(metaColumns[1])) {
                    this.retParamBuf.append(retParams[0]);
                    this.retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                    this.retParamBuf.append(retParams[1]);
                    this.retParamBuf.append(ImdstDefine.keyHelperClientParamSep);

                    // 返却値に区切り文字が入っている場合は区切り文字より左辺のみ返す
                    if (retParams.length > 2 && retParams[2] != null) {
                        this.retParamBuf.append(((String[])retParams[2].split(ImdstDefine.keyHelperClientParamSep))[0]);
                    }

                    // 返却値に区切り文字が入っている場合は区切り文字より左辺のみ返す
                    if (retParams.length > 3 && retParams[3] != null) {
                        this.retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                        this.retParamBuf.append(retParams[3]);
                    }
                } else {
                    this.retParamBuf.append(retParams[0]);
                    this.retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                    this.retParamBuf.append("false");
                    this.retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                }
                this.retParamBuf.append("\n");
                this.retParamBuf.append(ImdstDefine.getMultiEndOfDataStr);
            } else if (retParams[0].equals("23")) {

                String[] metaColumns = null;
                String[] valueSplit = retParams[3].split(ImdstDefine.keyHelperClientParamSep);

                if (valueSplit.length > 1) 
                    metaColumns = valueSplit[1].split(AbstractProtocolTaker.metaColumnSep);

                // 有効期限チェックも同時に行う
                if (valueSplit.length < 2 || AbstractProtocolTaker.expireCheck(metaColumns[1])) {
                    this.retParamBuf.append(retParams[0]);
                    this.retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                    this.retParamBuf.append(retParams[1]);
                    this.retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                    this.retParamBuf.append(retParams[2]);
                    this.retParamBuf.append(ImdstDefine.keyHelperClientParamSep);

                    // 返却値に区切り文字が入っている場合は区切り文字より左辺のみ返す
                    if (retParams.length > 3 && retParams[3] != null) {
                        this.retParamBuf.append(((String[])retParams[3].split(ImdstDefine.keyHelperClientParamSep))[0]);
                    }
                    this.retParamBuf.append("\n");
                } else {
                    this.retParamBuf.append("");
                }

            } else if (retParams[0].equals("23-f")) {
                if (retParams.length < 4 || retParams[1].equals("false")) {

                    this.retParamBuf.append(ImdstDefine.getMultiEndOfDataStr);
                } else {

                    String[] metaColumns = null;
                    String[] valueSplit = retParams[3].split(ImdstDefine.keyHelperClientParamSep);
                    
                    retParams[0] = "23";
                    if (valueSplit.length > 1) 
                        metaColumns = valueSplit[1].split(AbstractProtocolTaker.metaColumnSep);

                    // 有効期限チェックも同時に行う
                    if (!retParams[1].equals("true")) {

                        this.retParamBuf.append("");
                    } else if (valueSplit.length < 2 || AbstractProtocolTaker.expireCheck(metaColumns[1])) {

                        this.retParamBuf.append(retParams[0]);
                        this.retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                        this.retParamBuf.append(retParams[1]);
                        this.retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                        this.retParamBuf.append(retParams[2]);
                        this.retParamBuf.append(ImdstDefine.keyHelperClientParamSep);

                        // 返却値に区切り文字が入っている場合は区切り文字より左辺のみ返す
                        if (retParams.length > 3 && retParams[3] != null) {
                            this.retParamBuf.append(((String[])retParams[3].split(ImdstDefine.keyHelperClientParamSep))[0]);
                        }
                        this.retParamBuf.append("\n");
                    } else {

                        this.retParamBuf.append("");
                    }

                    this.retParamBuf.append(ImdstDefine.getMultiEndOfDataStr);
                }
            } else {
                this.retParamBuf.append(retParams[0]);
                this.retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                this.retParamBuf.append(retParams[1]);
                this.retParamBuf.append(ImdstDefine.keyHelperClientParamSep);

                // 返却値に区切り文字が入っている場合は区切り文字より左辺のみ返す
                if (retParams.length > 2 && retParams[2] != null) {
                    this.retParamBuf.append(((String[])retParams[2].split(ImdstDefine.keyHelperClientParamSep))[0]);
                }

                // 返却値に区切り文字が入っている場合は区切り文字より左辺のみ返す
                if (retParams.length > 3 && retParams[3] != null) {
                    this.retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                    this.retParamBuf.append(retParams[3]);
                }
            }
            this.nextExec = 1;
        }

        // Debugログ書き出し
        if (StatusUtil.getDebugOption()) 
            SystemUtil.debugLine(clientInfo + " : Response : " + retParamBuf);

        return this.retParamBuf.toString();
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
     * okuyamaのプロトコルにマッチしたかを返す.<br>
     *
     * @return boolean true:マッチ false:ノーマッチ
     */
    public boolean isMatchMethod() {
        return this.methodMatch;
    }
}