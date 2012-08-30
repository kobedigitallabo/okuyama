package okuyama.imdst.util;

import java.io.ByteArrayOutputStream;

import com.sun.mail.util.BASE64DecoderStream;
import com.sun.mail.util.BASE64EncoderStream;

/**
 * 最終保存媒体がMemory時のConverter.<br>
 * Encode仕様:Key=String
 *            Value=String
 *
 * Decode仕様:Key=String
 *            Value=String
 *
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class OriginalValueMemoryModeCoreValueCnv implements ICoreValueConverter {


    /**
     * 引数のObjectはBase64でエンコード後のString
     * 返却値はString
     *
     */
    public Object convertEncodeKey(Object key) {
        if (key == null) return null;
        //System.out.println("Encode Key[" + key + "]");
        return key;
    }


    /**
     * 引数のObjectはBase64でエンコードされた文字とメタ情報の連結文字列
     * 返却値はString
     *
     */
    public Object convertEncodeValue(Object value) {
        if (value == null) return null;
        
        return ((String)value).getBytes();
    }
    

    /**
     * 引数のObjectはCoreMapKey
     * 返却値は文字列
     */
    public Object convertDecodeKey(Object key) {
        if (key == null) return null;
        return key;

    }



    /**
     * 引数のObjectはBase64でエンコードされた文字とメタ情報の連結文字列のbyte配列
     * 返却値はbyte配列を文字列に変更したString
     *
     */
    public Object convertDecodeValue(Object value) {
        if (value == null) return null;
        
        return new String((byte[])value);
    }
}
