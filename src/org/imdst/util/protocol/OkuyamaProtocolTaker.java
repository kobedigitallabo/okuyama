package org.imdst.util.protocol;

import java.io.*;
import java.util.*;
import java.net.*;

import org.imdst.util.ImdstDefine;

/**
 * クライアントとのProtocolの差を保管する.<br>
 * okuyamaの標準Protocol用のTaker.<br>
 *
 */
public class OkuyamaProtocolTaker implements IProtocolTaker {

    private int nextExec = 0;

    public String takeRequestLine(BufferedReader br, PrintWriter pw) throws Exception {
        String retStr = null;
        retStr = br.readLine();

        // 切断指定確認
        if (retStr == null ||
                retStr.equals("") ||
                    retStr.equals(ImdstDefine.imdstConnectExitRequest)) {
            // 接続を切断
            this.nextExec = 3;
        }

        return retStr;
    }

    public String takeResponseLine(String[] retParams) throws Exception {
        StringBuffer retParamBuf = new StringBuffer();

        retParamBuf.append(retParams[0]);
        retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
        retParamBuf.append(retParams[1]);
        retParamBuf.append(ImdstDefine.keyHelperClientParamSep);

        // 返却値に区切り文字が入っている場合は区切り文字より左辺のみ返す
        if (retParams[2] != null) {
            retParamBuf.append(((String[])retParams[2].split(ImdstDefine.keyHelperClientParamSep))[0]);
        }
        this.nextExec = 1;

        return retParamBuf.toString();
    }

    public int nextExecution() {
        return this.nextExec;
    }

}