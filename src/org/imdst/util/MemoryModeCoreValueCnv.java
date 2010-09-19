package org.imdst.util;


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
     * 返却値はBase64でデコード後のbyte配列
     *
     */
    public Object convertEncodeKey(Object key) {
        return decode(((String)key).getBytes());
    }

    /**
     * 引数のObjectはBase64でエンコードされた文字とメタ情報の連結文字列
     * 返却値はbyte配列
     *
     */
    public Object convertEncodeValue(Object value) {
        return ((String)value).getBytes();
    }



    /**
     * 引数のObjectはBase64でエンコード後直後のbyte配列
     * 返却値はBase64でエンコード後の文字列
     */
    public Object convertDecodeKey(Object key) {
        return new String(encode((byte[])key));
    }

    /**
     * 引数のObjectはBase64でエンコードされた文字とメタ情報の連結文字列のbyte配列
     * 返却値はbyte配列を文字列に変更したString
     *
     */
    public Object convertDecodeValue(Object value) {
        return new String((byte[])value);
    }



    private byte[] encode(byte[] datas) {
        return BASE64EncoderStream.encode(datas)
    }

    private byte[] decode(byte[] datas) {
        return BASE64DecoderStream.decode(datas)
    }


}
