package okuyama.imdst.util.protocol;

import java.io.*;
import java.util.*;
import java.net.*;

import okuyama.imdst.util.ImdstDefine;
import okuyama.imdst.util.JavaSystemApi;

/**
 * ProtocolTakerの共通処理をまとめる.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
abstract public class AbstractProtocolTaker {

    protected static String metaColumnSep = "-";

    protected String calcExpireTime(String timeStr) {
        String ret = "0";
        try {
            if (!timeStr.equals("0")) {
                // 数値変換出来ない場合はエラー
                long plusTime = Integer.parseInt(timeStr) * 1000;
                ret = new Long(JavaSystemApi.currentTimeMillis + plusTime).toString();
            }
        } catch (NumberFormatException e) {
            ret = "0";
        }
        return ret;
    }


    protected boolean expireCheck(String expirTimeStr) {
        boolean ret = true;

        try {
            // 数値変換出来ない場合はエラー
            if (!expirTimeStr.trim().equals("0")) {

                long expireTime = Long.parseLong(expirTimeStr);

                if (expireTime <= JavaSystemApi.currentTimeMillis) ret = false;
            }
        } catch (NumberFormatException e) {
            ret = false;
        }

        return ret;
    }

}
