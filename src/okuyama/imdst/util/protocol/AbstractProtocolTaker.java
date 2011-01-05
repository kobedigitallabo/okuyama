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

        try {
            if (!timeStr.equals("0")) {
                // 送られてきた時間指定がUTCの場合
                if (Integer.parseInt(timeStr) > 2592000) {
                    // 送られてきた値がUTCなのに、現在時間より小さい値の場合
                    if (Long.parseLong(timeStr) > nowSecTime) {
                        // 送られてきた値が現在時間よりも大きい場合
                        // 指定時間から現在時間を引いた値に変換
                        timeStr = new Long((long)Long.parseLong(timeStr) - nowSecTime).toString();
                    } else {
                        // 送られてきた時間が現在時間よりも小さい場合は-1とする
                        timeStr = "-1";
                    }
                }
System.out.println(timeStr);
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
        return SystemUtil.expireCheck(expirTimeStr);
    }

}
