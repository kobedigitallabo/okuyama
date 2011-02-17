package okuyama.imdst.util;

import java.io.ByteArrayOutputStream;

import com.sun.mail.util.BASE64DecoderStream;
import com.sun.mail.util.BASE64EncoderStream;

/**
 * 最終保存媒体がMemory時のConverter.<br>
 * Encode仕様:Key=BASE64でデコード後、バイト配列で返す
 *            Value=バイト配列で返す
 *
 * Decode仕様:Key=BASE64でエンコード後、Stringで返す
 *            Value=Stringで返す
 *
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class MemoryModeCoreValueCnv implements ICoreValueConverter {


    /**
     * 引数のObjectはBase64でエンコード後のString
     * 返却値はCoreMapKey
     *
     */
    public Object convertEncodeKey(Object key) {
        if (key == null) return null;
        //System.out.println("Encode Key[" + key + "]");
        return new CoreMapKey(((String)key).getBytes());
    }

    /**
     * 引数のObjectはBase64でエンコードされた文字とメタ情報の連結文字列
     * 返却値はbyte配列
     *
     */
    public Object convertEncodeValue(Object value) {
        if (value == null) return null;
        boolean tagMatch = false;

        byte[] registorData = null;


        String[] timeSepSplit = ((String)value).split("!");

        String timeVal = timeSepSplit[1];

        byte[] timeValBytes = timeVal.getBytes();

        String[] metaSep = timeSepSplit[0].split(",");

        String metaVal = null;
        byte[] metaValBytes = null;

        String realVal = metaSep[0];
        if (metaSep.length > 1) {
            metaVal = metaSep[1];
            metaValBytes = metaVal.getBytes();
        }

        // 真のValueの部分だけバイト化
        byte[] realValBytes = realVal.getBytes();

        // タグチェック
        int chackSize = realValBytes.length;
        if (chackSize > ImdstDefine.saveKeyMaxSize) {
            chackSize = ImdstDefine.saveKeyMaxSize;
        }

        for (int realValBytesIdx = 0; realValBytesIdx < chackSize; realValBytesIdx++) {
            if (realValBytes[realValBytesIdx] == 58) {
                tagMatch = true;
                break;
            }
        }

        // タグの場合はここで終了
        if (tagMatch) {
            // Tagとわかるようにbyte配列の先頭をブランクとする
            byte[] tagBytes = ((String)value).getBytes();
            
            byte[] returnTagBytes = new byte[tagBytes.length + 1];
            for (int i = 0; i < tagBytes.length; i++) {
                returnTagBytes[i+1] = tagBytes[i];
            }
            return returnTagBytes;
        }

        byte[] realDecodeValBytes = BASE64DecoderStream.decode(realValBytes);

        int size = 0;

        int realDecodeValBytesLen = realDecodeValBytes.length;

        if (metaVal == null) {

            int secIdx = 0;
            int idx = 0;

            size = realDecodeValBytesLen + 2 + timeValBytes.length;
            registorData = new byte[size];

            for (; idx < realDecodeValBytesLen; idx++) {
                registorData[idx] = realDecodeValBytes[idx];
            }

            // 1つ空白開けてデータとメタ情報との切れ目を示す
            idx++;

            // "!"を代入(メタデータ(Unique値)の切れ目)
            registorData[idx] = 33;
            idx++;

            for (; secIdx < timeValBytes.length; secIdx++) {
                registorData[idx] = timeValBytes[secIdx];
                idx++;
            }

        } else {

            int secIdx = 0;
            int thrIdx = 0;
            int idx = 0;

            size = realDecodeValBytesLen + 2 + metaValBytes.length + 1 + timeValBytes.length;
            registorData = new byte[size];

            for (; idx < realDecodeValBytesLen; idx++) {
                registorData[idx] = realDecodeValBytes[idx];
            }

            // 1つ空白開けてデータとメタ情報との切れ目を示す
            idx++;

            // ","を代入(メタデータ(Flagsと有効期限の切れ目)の切れ目)
            registorData[idx] = 44;
            idx++;

            for (; secIdx < metaValBytes.length; secIdx++) {
                registorData[idx] = metaValBytes[secIdx];
                idx++;
            }

            // "!"を代入(メタデータ(Unique値)の切れ目)
            registorData[idx] = 33;
            idx++;

            for (; thrIdx < timeValBytes.length; thrIdx++) {
                registorData[idx] = timeValBytes[thrIdx];
                idx++;
            }
        }


        timeSepSplit = null;
        timeVal = null;
        timeValBytes = null;
        metaSep =  null;
        metaVal = null;
        metaValBytes = null;
        //System.out.println("-------------------");
        //System.out.println(((byte[])",".getBytes())[0]); 44 
        //System.out.println(((byte[])"!".getBytes())[0]); 33

        return registorData;
    }



    /**
     * 引数のObjectはCoreMapKey
     * 返却値は文字列
     */
    public Object convertDecodeKey(Object key) {
        if (key == null) return null;
        return new String(((CoreMapKey)key).getDatas());
    }



    /**
     * 引数のObjectはBase64でエンコードされた文字とメタ情報の連結文字列のbyte配列
     * 返却値はbyte配列を文字列に変更したString
     *
     */
    public Object convertDecodeValue(Object value) {
        if (value == null) return null;
        boolean tagMatch = false;

        byte[] decodeBytes =  (byte[])value;
        int decodeDataLen = decodeBytes.length;



        // タグの場合はここで終了
        if (decodeBytes[0] == 0) {
            ByteArrayOutputStream tagBytes = new ByteArrayOutputStream(decodeDataLen - 1);
            tagBytes.write(decodeBytes, 1, decodeBytes.length - 1);
            return tagBytes.toString();
        }


        // タグ以外
        ByteArrayOutputStream workBytes = new ByteArrayOutputStream(decodeDataLen);
        int idx = 0;
        int metaLen = 0;

        StringBuilder workBuf = new StringBuilder(decodeDataLen + (decodeDataLen / 3) + (decodeDataLen / 30));

        for (; idx < decodeDataLen; idx++) {

            // 全てのバイト表記が見れる
            //System.out.println(decodeBytes[idx]);

            if (decodeBytes[idx] != 0) {
                workBytes.write(decodeBytes[idx]);
            } else {

                workBuf.append(new String(BASE64EncoderStream.encode(workBytes.toByteArray())));

                metaLen = decodeDataLen - idx;
                workBytes = null;
                break;
            }
        }

        idx++;
        workBytes = new ByteArrayOutputStream(metaLen);

        for (; idx < decodeDataLen; idx++) {
            workBytes.write(decodeBytes[idx]);
        }

        workBuf.append(workBytes.toString());
        workBytes = null;
        decodeBytes = null;

        return workBuf.toString();
    }
}
