package okuyama.imdst.util;

import okuyama.base.util.ILogger;
import okuyama.base.util.LoggerFactory;

/**
 * システム系のApiに対してアクセスする.<br>
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
        StringBuffer writeBuf = new StringBuffer(data);

        int valueSize = data.length();

        // 渡されたデータが固定の長さ分ない場合は足りない部分を補う
        byte[] appendDatas = new byte[fixSize - valueSize];

        for (int i = 0; i < appendDatas.length; i++) {
            appendDatas[i] = new Integer(fillByte).byteValue();
        }

        writeBuf.append(new String(appendDatas));
        return writeBuf.toString();
    }


}
