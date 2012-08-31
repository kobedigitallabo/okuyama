package okuyama.imdst.util.serializemap;

import java.util.Map;
import java.io.*;

import okuyama.imdst.util.*;

/**
 * ISerializerの実装.<br>
 * 全てのObjectをbyte配列に変換して保存するためのSerializer.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class ByteDataMemoryStoreSerializer implements Cloneable, Serializable, ISerializer {

    private String serializeMapName = null;

    public ByteDataMemoryStoreSerializer() {}


    public void setInstanceCreateMapName(String mapName) {
        this.serializeMapName = mapName;
    }


    /**
     * シリアライザ.<br>
     * byte配列化を利用しているスピードが重要な場合にObjectStreamSerializerよりも高速に稼働する.<br>
     * KeyもValueもメモリの場合のみ利用可能.<br>
     * 
     * @param serializeTarget シリアライズするターゲットオブジェクト(具象クラスはHashMap)
     * @param mapKeyClazz シリアライズするターゲットオブジェクトのMapがKey値として持つクラス(シリアライス、デシリアライズ時の指標)
     * @param mapValueClazz シリアライズするターゲットオブジェクトのMapがValue値として持つクラス(シリアライス、デシリアライズ時の指標)
     * @param 呼び出しに使われたKey値
     * @param uniqueNo 本処理の対象となるMapをあらわすユニークな値
     * @return シリアライズ済み返却値
     */
    public byte[] serialize(Map serializeTarget, Class mapKeyClazz, Class mapValueClazz, Object key, int uniqueNo) {
        return SystemUtil.dataCompress(SystemUtil.byteListStoreSerialize(serializeTarget));
    }


    /**
     * デシリアライズ処理インターフェース.<br>
     * byte配列化を利用しているスピードが重要な場合にObjectStreamSerializerよりも高速に稼働する.<br>
     * KeyもValueもメモリの場合のみ利用可能.<br>
     *
     * @param deserializeTarget デシリアライズターゲット
     * @param 呼び出しに使われたKey値
     * @param uniqueNo 本処理の対象となるMapをあらわすユニークな値
     * @return デシリアライズ済み返却値
     */
    public Map deSerialize(byte[] deserializeTarget, Object key, int uniqueNo) {
        return SystemUtil.byteListStoreDeserialize(SystemUtil.dataDecompress(deserializeTarget));
    }


    public void clearParentMap() {
    }
}