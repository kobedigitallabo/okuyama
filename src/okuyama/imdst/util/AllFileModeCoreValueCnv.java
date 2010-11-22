package okuyama.imdst.util;

/**
 * 最終保存媒体がKey-ValueともにFile時のConverter.<br>
 *
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class AllFileModeCoreValueCnv implements ICoreValueConverter {


    /**
     * 引数のObjectはBase64でエンコード後のString
     * 返却値はCoreMapKeyに格納して返す
     *
     */
    public Object convertEncodeKey(Object key) {
        return key;
    }


    /**
     * 引数のInteger型の値
     * Stringに変換して返す
     *
     */
    public Object convertEncodeValue(Object value) {
        return value.toString();
    }



    /**
     * 引数のObjectはCoreValue
     * 返却値はBase64でエンコード後の文字列
     */
    public Object convertDecodeKey(Object key) {
        if (key == null) return null;
        return key;
    }

    /**
     * 引数のString型の値
     * Integerに変換して返却
     *
     */
    public Object convertDecodeValue(Object value) {
        if (value == null) return null;
		try {
			Integer ret = new Integer((String)value);
	        return ret;
		} catch(NumberFormatException nfe) {
			return (String)value;
		}
    }
}
