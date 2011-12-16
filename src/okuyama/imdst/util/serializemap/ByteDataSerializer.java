package okuyama.imdst.util.serializemap;

import java.util.*;

import okuyama.imdst.util.*;

/**
 * ISerializerの実装.<br>
 * あらかじめbyte配列で領域を固定してそこにデータを格納し、GCのold領域中の参照切れをなくす試験実装<br>
 * 指定方法) okuyama.imdst.util.serializemap.ByteDataSerializer;disk<br>
 * <br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class ByteDataSerializer implements ISerializer {

    private String serializeMapName = null;

    private boolean valueDisk = false;


    public ByteDataSerializer() {}

    public ByteDataSerializer(String mode) {
        if (mode.trim().toLowerCase().equals("disk")) valueDisk = true;
    }

    public void setInstanceCreateMapName(String mapName) {
        this.serializeMapName = mapName;
    }


    /**
     * シリアライザ.<br>
     * 
     * @param serializeTarget シリアライズするターゲットオブジェクト(具象クラスはHashMapもしくはByteDataSerializeCustomHashMap
     * @param mapKeyClazz シリアライズするターゲットオブジェクトのMapがKey値として持つクラス(シリアライス、デシリアライズ時の指標)
     * @param mapValueClazz シリアライズするターゲットオブジェクトのMapがValue値として持つクラス(シリアライス、デシリアライズ時の指標)
     * @param 呼び出しに使われたKey値
     * @param uniqueNo 本処理の対象となるMapをあらわすユニークな値
     * @return シリアライズ済み返却値
     */
    public byte[] serialize(Map serializeTarget, Class mapKeyClazz, Class mapValueClazz, Object key, int uniqueNo) {
        if (valueDisk == true) {
            if (serializeTarget instanceof  ByteDataSerializeCustomHashMap) {

                return ((ByteDataSerializeCustomHashMap)serializeTarget).byteData;
            } else if (serializeTarget instanceof  HashMap) {

                String val = (String)serializeTarget.get(key);
                String keyVal = key + ";" + val;
                byte[] newData = new byte[4096];

                byte[] keyValByte = keyVal.getBytes();
                for (int idx = 0; idx < keyValByte.length; idx++) {
                    newData[idx] = keyValByte[idx];
                }
                return newData;
            }
        } else {
            return SystemUtil.dataCompress(SystemUtil.defaultSerializeMap(serializeTarget));
        }
        return null;
    }


    /**
     * デシリアライズ処理インターフェース.<br>
     *
     * @param deserializeTarget デシリアライズターゲット
     * @param 呼び出しに使われたKey値
     * @param uniqueNo 本処理の対象となるMapをあらわすユニークな値
     * @return デシリアライズ済み返却値
     */
    public Map deSerialize(byte[] deserializeTarget, Object key, int uniqueNo) {
        if (valueDisk == true) {
            String dataStr = null;
            String[] keyVal = null;

            dataStr = new String(deserializeTarget);
            dataStr = dataStr.trim();

            Map customHashMap = new ByteDataSerializeCustomHashMap(deserializeTarget);

            if (!dataStr.equals("")) {
                keyVal = dataStr.split(",");

                for (int idx = 0; idx < keyVal.length; idx++) {
                    String[] singleData = keyVal[idx].split(";");
                    
                    ((ByteDataSerializeCustomHashMap)customHashMap).originalPut(new CoreMapKey(singleData[0]), new String(singleData[1]));
                }
            }
            return customHashMap;
        } else {
            return SystemUtil.defaultDeserializeMap(SystemUtil.dataDecompress(deserializeTarget));
        }
    }


    public void clearParentMap() {
    }
}