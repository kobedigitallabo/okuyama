package org.imdst.util;


import com.sun.mail.util.BASE64DecoderStream;
import com.sun.mail.util.BASE64EncoderStream;

/**
 * 最終保存媒体がFile時のConverter.<br>
 * Encode仕様:Key=BASE64でデコード後、バイト配列で返す
 *            Value=なにもしない
 *
 * Decode仕様:Key=BASE64でエンコード後、Stringで返す
 *            Value=なにもしない
 *
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class FileModeCoreValueCnv implements ICoreValueConverter {


    /**
     * 引数のObjectはBase64でエンコード後のString
     * 返却値はBase64でデコード後のCoreMapKey
     *
     */
    public Object convertEncodeKey(Object key) {
        if (key == null) return null;
        return new CoreMapKey(decode(((String)key).getBytes()));
    }


    /**
     * 引数のLong型の値
     * 返却値は何もせずに返却
     *
     */
    public Object convertEncodeValue(Object value) {
        return value;
    }



    /**
     * 引数のObjectはCoreValue
     * 返却値はBase64でエンコード後の文字列
     */
    public Object convertDecodeKey(Object key) {
        if (key == null) return null;
        return new String(encode(((CoreMapKey)key).getDatas()));
    }

    /**
     * 引数のLong型の値
     * 返却値は何もせずに返却
     *
     */
    public Object convertDecodeValue(Object value) {
        return value;
    }


    private byte[] encode(byte[] datas) {
        return BASE64EncoderStream.encode(datas);
    }

    private byte[] decode(byte[] datas) {
        return BASE64DecoderStream.decode(datas);
    }


}
