package okuyama.imdst.util.serializemap;

import java.util.Map;
import java.util.HashMap;

import okuyama.imdst.util.*;

public class ToStringSerializer implements ISerializer {

    private boolean typeInteger = false;

    private int count = 0;

    public ToStringSerializer() {
    }

    public ToStringSerializer(String type) {
        if (type != null && type.equals("Integer")) {
            typeInteger = true;
        }
    }

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

        if (typeInteger) {
            try {
                return SystemUtil.dataCompress(serializeTarget.toString().getBytes("UTF-8"));
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        } else {
            return SystemUtil.dataCompress(SystemUtil.defaultSerializeMap(serializeTarget));
        }
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

        if (typeInteger) {
            byte[] decompressData = SystemUtil.dataDecompress(deserializeTarget);
            try {
                String serializeStr = new String(decompressData, "UTF-8");
                return this.deserializeStringToMap(serializeStr.substring(1, (serializeStr.length() - 1)));
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
            
        } else {
            return SystemUtil.defaultDeserializeMap(SystemUtil.dataDecompress(deserializeTarget));
        }
    }


    private Map deserializeStringToMap(String serializeStr) {

        Map retMap = new HashMap();
        String[] dataStrList = serializeStr.split(", ");

        for (int idx = 0; idx < dataStrList.length; idx++) {
            
            if (!dataStrList[idx].trim().equals("")) {
                int lastIndex = dataStrList[idx].lastIndexOf("=");
                retMap.put(new CoreMapKey(dataStrList[idx].substring(0, lastIndex)), new Integer(dataStrList[idx].substring(lastIndex+1)));
            }
        }

        return retMap;
    }
}