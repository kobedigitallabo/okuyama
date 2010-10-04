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
        return key;
    }

    /**
     * 引数のLong型の値
     * 返却値は何もせずに返却
     *
     */
    public Object convertDecodeValue(Object value) {
        return value;
    }
}
