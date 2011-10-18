package okuyama.imdst.client;

/**
 * ユーザが独自で実装するOkuyamaClientで値取得時に返却の有無を決めるFilterのインターフェース.<br>
 * 利用箇所はgetTagKeysResultなどで、本インターフェースを実装したクラスのインスタンスを渡すと、KeyとValueが<br>
 * ユーザプログラムに返る前に実装クラスに渡されfilterメソッドが実行されるので、そこで返却の有無を決めるように<br>
 * 実装すれば返却値をコントロールできる.<br>
 * 実装例)
 *&nbsp;&nbsp;&nbsp;&nbsp;public class ExampleUserFilter implements UserDataFilter {
 *&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;public boolean filter(Object key, Object value) {
 *&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;String targetKey = (Strin)key;
 *&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;String targetValue = (Strin)value;
 *&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;if (targetKey.indexOf("2011") != -1 && targetValue.indexOf("hogehoge") != -1) {
 *&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;return true;
 *&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;}
 *&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;return false;
 *&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;}
 *&nbsp;&nbsp;&nbsp;&nbsp;}
 *上記の実装例は、Keyに"2011"という文字列が含まれて、Valueに"hogehoge"という文字が含まれている場合のみ、値を変えすようになります<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public interface UserDataFilter {

    /**
     * 値の返却判定実装部分.<br>
     *
     * @param key 対象のKey値
     * @param value 対象のValue値
     * @return boolean 返却指定 true=返却/false=返却しない
     */
    public boolean filter(Object key, Object value);

}
