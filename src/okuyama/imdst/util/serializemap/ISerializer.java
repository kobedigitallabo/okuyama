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
     * シリアライズ処理インターフェース.<br>
     *
     * @param serializeTarget シリアライズするターゲットオブジェクト(具象クラスはHashMap)
     * @param mapKeyClazz シリアライズするターゲットオブジェクトのMapがKey値として持つクラス(シリアライス、デシリアライズ時の指標)
     * @param mapValueClazz シリアライズするターゲットオブジェクトのMapがValue値として持つクラス(シリアライス、デシリアライズ時の指標)
     * @return シリアライズ済み返却値
     */
    public byte[] serialize(Map serializeTarget, Class mapKeyClazz, Class mapValueClazz);

    /**
     * デシリアライズ処理インターフェース.<br>
     *
     * @param deserializeTarget デシリアライズターゲット値(serializeメソッドで返却した値)
     * @return デシリアライズ済み返却値
     */
    public Map deSerialize(byte[] deserializeTarget);
}   