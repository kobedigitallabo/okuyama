package okuyama.imdst.util.serializemap;

import java.util.*;

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
            

            String sep = "";
            StringBuilder strBuf = new StringBuilder(128);
            strBuf.append("{");
            
            Set entrySet = serializeTarget.entrySet();
            Iterator entryIte = entrySet.iterator(); 
            try {

                while(entryIte.hasNext()) {

                    Map.Entry obj = (Map.Entry)entryIte.next();
                    if (obj == null) continue;

                    CoreMapKey key = (CoreMapKey)obj.getKey();
                    
                    String data = new String((byte[])obj.getValue(), "UTF-8");
                    strBuf.append(sep);
                    strBuf.append(key.toString());
                    strBuf.append("=");
                    strBuf.append(data);
                    sep = ", ";
                }
                strBuf.append("}");

                return SystemUtil.dataCompress(strBuf.toString().getBytes("UTF-8"));
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
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

        byte[] decompressData = SystemUtil.dataDecompress(deserializeTarget);
        try {

            String serializeStr = new String(decompressData, "UTF-8");
            if (typeInteger) {

                return this.deserializeStringToMap(serializeStr.substring(1, (serializeStr.length() - 1)));
            } else {
                return this.deserializeStringToMapByByteData(serializeStr.substring(1, (serializeStr.length() - 1)));
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
            
    }


    private Map deserializeStringToMap(String serializeStr) {

        Map retMap = new HashMap();
        String[] dataStrList = serializeStr.split(", ");

        for (int idx = 0; idx < dataStrList.length; idx++) {
            
            if (!dataStrList[idx].trim().equals("")) {
                int lastIndex = dataStrList[idx].lastIndexOf("=");
                retMap.put(new CoreMapKey(dataStrList[idx].substring(0, lastIndex)), dataStrList[idx].substring(lastIndex+1));
            }
        }

        return retMap;
    }

    private Map deserializeStringToMapByByteData(String serializeStr) {

        Map retMap = new HashMap();
        String[] dataStrList = serializeStr.split(", ");
        try {

            for (int idx = 0; idx < dataStrList.length; idx++) {
                
                if (!dataStrList[idx].trim().equals("")) {
                    int lastIndex = dataStrList[idx].lastIndexOf("=");
                    retMap.put(new CoreMapKey(dataStrList[idx].substring(0, lastIndex)), ((String)dataStrList[idx].substring(lastIndex+1)).getBytes("UTF-8"));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

        return retMap;
    }
}