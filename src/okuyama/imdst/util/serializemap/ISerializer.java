package okuyama.imdst.util.serializemap;

import java.util.Map;

/**
 * SerializeMap用のシリアライザのインターフェース.<br>
 * 本インターフェースをインプリしたクラスは、okuyama.imdst.util.serializemap.SerializeMap内で一度だけインスタンス化されて<br>
 * 以降、マルチスレッド環境で並列利用される。そのため、スレッドセーフに実装する必要がある.<br>
 * serializeメソッドの第1引数のMapをシリアライズしてbyte配列として返却する.<br>
 * deSerializeメソッドの第1引数のbyte配列はserializeメソッドで返却した値となるので、デシリアライズしてMapオブジェクトとして返却.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public interface ISerializer {


    /**
     * 自身をインスタンス化したSerializeMapのユニーク名(インスタンスのHash値)を引数に1度だけ呼び出される.<br>
     *
     * @param mapName
     */
    public void setInstanceCreateMapName(String mapName);


    /**
     * シリアライズ処理インターフェース.<br>
     *
     * @param serializeTarget シリアライズするターゲットオブジェクト(具象クラスはHashMap)
     * @param mapKeyClazz シリアライズするターゲットオブジェクトのMapがKey値として持つクラス(シリアライス、デシリアライズ時の指標)
     * @param mapValueClazz シリアライズするターゲットオブジェクトのMapがValue値として持つクラス(シリアライス、デシリアライズ時の指標)
     * @param key 本処理を呼び出した処理が使用しようとしているKey値
     * @param uniqueNo 本処理の対象となるMapをあらわすユニークな値(この値が同じ場合は同一のMapを処理しようとしている)
     * @return シリアライズ済み返却値
     */
    public byte[] serialize(Map serializeTarget, Class mapKeyClazz, Class mapValueClazz, Object key, int uniqueNo);

    /**
     * デシリアライズ処理インターフェース.<br>
     *
     * @param deserializeTarget デシリアライズターゲット値(serializeメソッドで返却した値)
     * @param key 本処理を呼び出した処理が使用しようとしているKey値
     * @param uniqueNo 本処理の対象となるMapをあらわすユニークな値(この値が同じ場合は同一のMapを処理しようとしている)
     * @return デシリアライズ済み返却値
     */
    public Map deSerialize(byte[] deserializeTarget, Object key, int uniqueNo);


    /**
     * 自身をインスタンス化したSerializeMapのclearメソッドが呼び出されたタイミングで呼び出される
     *
     */
    public void clearParentMap();
}   