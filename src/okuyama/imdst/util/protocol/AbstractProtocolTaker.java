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

    protected static String metaColumnSep = ImdstDefine.valueMetaColumnSep;

    protected String calcExpireTime(String timeStr) {
        String ret = "0";
        long nowMilliTime = JavaSystemApi.currentTimeMillis;
        long nowSecTime = nowMilliTime / 1000;
        long timeStrLong = 0L;

        try {
            if (!timeStr.equals("0")) {

                // 送られてきた時間指定がUTCの場合
                timeStrLong = Long.parseLong(timeStr);
                if (timeStrLong > 2592000) {

                    // 送られてきた値がUTCなのに、現在時間より小さい値の場合
                    if (timeStrLong > nowSecTime) {

                        // 送られてきた値が現在時間よりも大きい場合
                        // 指定時間から現在時間を引いた値に変換
                        timeStrLong = timeStrLong - nowSecTime;
                    } else {

                        // 送られてきた時間が現在時間よりも小さい場合は-1とする
                        timeStrLong = -1;
                    }
                }

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


    protected boolean expireCheck(String expirTimeStr) {
        return SystemUtil.expireCheck(expirTimeStr);
    }

}
