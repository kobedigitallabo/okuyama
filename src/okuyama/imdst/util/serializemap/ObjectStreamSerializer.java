package okuyama.imdst.util.serializemap;

import java.util.Map;

import okuyama.imdst.util.*;

public class ObjectStreamSerializer implements ISerializer {

    public ObjectStreamSerializer() {}

    /**
     * シリアライザ.<br>
     * 内部ではObjectOutputStreamを利用している.<br>
     * スピードにやや難有り.<br>
     * 
     * @param serializeTarget シリアライズするターゲットオブジェクト(具象クラスはHashMap)
     * @param mapKeyClazz シリアライズするターゲットオブジェクトのMapがKey値として持つクラス(シリアライス、デシリアライズ時の指標)
     * @param mapValueClazz シリアライズするターゲットオブジェクトのMapがValue値として持つクラス(シリアライス、デシリアライズ時の指標)
     * @return シリアライズ済み返却値
     */
    public byte[] serialize(Map serializeTarget, Class mapKeyClazz, Class mapValueClazz) {
        return SystemUtil.dataCompress(SystemUtil.defaultSerializeMap(serializeTarget));
    }


    /**
     * デシリアライズ処理インターフェース.<br>
     * 内部ではObjectInputStreamを利用している.<br>
     * スピードにやや難有り.<br>
     *
     * @param deserializeTarget デシリアライズターゲット
     * @return デシリアライズ済み返却値
     */
    public Map deSerialize(byte[] deserializeTarget) {
        return SystemUtil.defaultDeserializeMap(SystemUtil.dataDecompress(deserializeTarget));
    }

}