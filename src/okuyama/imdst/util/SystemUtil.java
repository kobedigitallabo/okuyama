package okuyama.imdst.util;

import java.util.Date;

import okuyama.base.util.ILogger;
import okuyama.base.util.LoggerFactory;

/**
 * okuyamaが使用する共通的なApiに対してアクセスする.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class SystemUtil {

    /**
     * 指定の文字を指定の桁数で特定文字列で埋める.<br>
     *
     * @param data
     * @param fixSize
     */
    public static String fillCharacter(String data, int fixSize) {
        return fillCharacter(data, fixSize, 38);
    }


    /**
     * 指定の文字を指定の桁数で特定文字列で埋める.<br>
     *
     * @param data
     * @param fixSize
     * @param fixByte
     */
    public static String fillCharacter(String data, int fixSize, int fillByte) {
        StringBuilder writeBuf = new StringBuilder(data);

        int valueSize = data.length();

        // 渡されたデータが固定の長さ分ない場合は足りない部分を補う
        byte[] appendDatas = new byte[fixSize - valueSize];

        for (int i = 0; i < appendDatas.length; i++) {
            appendDatas[i] = new Integer(fillByte).byteValue();
        }

        writeBuf.append(new String(appendDatas));
        return writeBuf.toString();
    }


    /**
     * 指定された値を時間に置き換えた場合に現在時間を過ぎているかをチェックする.<br>
     *
     * @param expirTimeStr
     * @return boolean
     */
    public static boolean expireCheck(String expirTimeStr) {
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


    /**
     * 指定された値を時間に置き換えた場合に現在時間を過ぎているかをチェックする.<br>
     * 引数のoverTimeで指定したミリ秒を過ぎている場合のみfalseを返す.<br>
     *
     * @param expirTimeStr
     * @param overTime
     * @return boolean
     */
    public static boolean expireCheck(String expirTimeStr, long overTime) {
        boolean ret = true;

        try {
            // 数値変換出来ない場合はエラー
            if (!expirTimeStr.trim().equals("0")) {

                long expireTime = Long.parseLong(expirTimeStr);

                if ((expireTime + overTime) <= JavaSystemApi.currentTimeMillis) ret = false;
            }
        } catch (NumberFormatException e) {
            ret = false;
        }

        return ret;
    }


    /**
     * -debugオプションを利用した際に、標準出力への出力を行う.<br>
     *
     * @param String outputStr
     */
    public static void debugLine(String outputStr) {
        if (StatusUtil.getDebugOption()) {
            StringBuffer strBuf = new StringBuffer(100);
            strBuf.append(new Date());
            strBuf.append(" DebugLine \"");
            strBuf.append(outputStr);
            strBuf.append("\"");
            System.out.println(strBuf.toString());
            strBuf = null;
        }
    }
}
