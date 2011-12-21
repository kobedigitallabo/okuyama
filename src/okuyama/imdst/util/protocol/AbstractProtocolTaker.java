package okuyama.imdst.util.protocol;

import java.io.*;
import java.util.*;
import java.net.*;

import okuyama.imdst.util.ImdstDefine;
import okuyama.imdst.util.JavaSystemApi;
import okuyama.imdst.util.SystemUtil;

/**
 * ProtocolTakerの共通処理をまとめる.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
abstract public class AbstractProtocolTaker {

    public static String metaColumnSep = ImdstDefine.valueMetaColumnSep;

    public static String calcExpireTime(String timeStr) {
        String ret = "0";
        long nowMilliTime = JavaSystemApi.currentTimeMillis;
        long nowSecTime = nowMilliTime / 1000;
        long timeStrLong = 0L;

        try {
            if (!timeStr.equals("0")) {

                // 送られてきた時間指定がUTCの場合
                timeStrLong = Long.parseLong(timeStr);

                // 数値変換出来ない場合はエラー
                long plusTime = timeStrLong * 1000;
                ret = new Long(JavaSystemApi.currentTimeMillis + plusTime).toString();
            }
        } catch (NumberFormatException e) {
            ret = "0";
        }

        return ret;
    }


    /**
     * memcachedのFlagsチェック.<br>
     * int整数値でない場合は0に変換
     */
    protected String checkFlagsVal(String flags) {
        String ret = "0";
        try {
            ret = new Integer(flags).toString();
        } catch (NumberFormatException e) {
            ret = "0";
        }

        return ret;
    }


    public static boolean expireCheck(String expirTimeStr) {
        return SystemUtil.expireCheck(expirTimeStr);
    }

}
