package okuyama.imdst.util.serializemap;

import java.util.Map;

import okuyama.imdst.util.*;

/**
 * ISerializerの実装.<br>
 * 全てのObjectをObjectInput/OutputStreamでSerializeして処理を行う<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class ObjectStreamSerializer implements ISerializer {

    private String serializeMapName = null;

    public ObjectStreamSerializer() {}


    public void setInstanceCreateMapName(String mapName) {
        this.serializeMapName = mapName;
    }


    /**
     * シリアライザ.<br>
     * 内部ではObjectOutputStreamを利用している.<br>
     * スピードにやや難有り.<br>
     * 
     * @param serializeTarget シリアライズするターゲットオブジェクト(具象クラスはHashMap)
     * @param mapKeyClazz シリアライズするターゲットオブジェクトのMapがKey値として持つクラス(シリアライス、デシリアライズ時の指標)
     * @param mapValueClazz シリアライズするターゲットオブジェクトのMapがValue値として持つクラス(シリアライス、デシリアライズ時の指標)
     * @param 呼び出しに使われたKey値
     * @param uniqueNo 本処理の対象となるMapをあらわすユニークな値
     * @return シリアライズ済み返却値
     */
    public byte[] serialize(Map serializeTarget, Class mapKeyClazz, Class mapValueClazz, Object key, int uniqueNo) {
        return SystemUtil.dataCompress(SystemUtil.defaultSerializeMap(serializeTarget));
    }


    /**
     * デシリアライズ処理インターフェース.<br>
     * 内部ではObjectInputStreamを利用している.<br>
     * スピードにやや難有り.<br>
     *
     * @param deserializeTarget デシリアライズターゲット
     * @param 呼び出しに使われたKey値
     * @param uniqueNo 本処理の対象となるMapをあらわすユニークな値
     * @return デシリアライズ済み返却値
     */
    public Map deSerialize(byte[] deserializeTarget, Object key, int uniqueNo) {
        return SystemUtil.defaultDeserializeMap(SystemUtil.dataDecompress(deserializeTarget));
    }


    public void clearParentMap() {
    }
}