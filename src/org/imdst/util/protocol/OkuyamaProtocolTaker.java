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

        //retStr = br.readLine();

        // 切断指定確認
        if (retStrs[0] == null ||
                retStrs[0].equals("") ||
                    retStrs[0].equals(ImdstDefine.imdstConnectExitRequest)) {
            // 接続を切断
            this.nextExec = 3;
        }

        return retStrs;
    }

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

		if (retParams != null && retParams.length > 1) {
	        retParamBuf.append(retParams[0]);
	        retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
	        retParamBuf.append(retParams[1]);
	        retParamBuf.append(ImdstDefine.keyHelperClientParamSep);

	        // 返却値に区切り文字が入っている場合は区切り文字より左辺のみ返す
	        if (retParams[2] != null) {
	            retParamBuf.append(((String[])retParams[2].split(ImdstDefine.keyHelperClientParamSep))[0]);
	        }
	        this.nextExec = 1;
		}
        return retParamBuf.toString();
    }

    public int nextExecution() {
        return this.nextExec;
    }

}