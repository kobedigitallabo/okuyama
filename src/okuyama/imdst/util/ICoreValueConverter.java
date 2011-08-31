package okuyama.imdst.util;

import okuyama.base.lang.BatchException;


/**
 * okuyamaがデータを永続化するMapを抽象化するために<br>
 * 永続化時のKeyとValueを変換するクラスのインターフェース<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public interface ICoreValueConverter {

    /**
     *
     */
    public Object convertEncodeKey(Object key);

    /**
     *
     */
    public Object convertEncodeValue(Object value);



    /**
     *
     */
    public Object convertDecodeKey(Object key);

    /**
     *
     */
    public Object convertDecodeValue(Object value);

}   