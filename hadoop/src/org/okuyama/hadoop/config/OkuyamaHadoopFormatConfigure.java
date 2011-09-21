package org.okuyama.hadoop.config;

import java.util.*;

/**
 * okuyama上でHadoopを走らす場合に設定を行うクラス.<br>
 * 新しく設定を行う場合はinitを必ず事前に呼び出す<br>
 * 新しく設定を完了したタイミングでfixConfig呼び出す<br>
 *
 */
public class OkuyamaHadoopFormatConfigure {

    private static Map configureMap = null;
    private static boolean fixFlg = false;

    private static String[] masterNodeInfos = null;
    private static List tagList = null;

    public static String CONFIGURE_STORE_KEY = "OkuyamaHadoopFormatConfigure_cofigure_key_";

    private OkuyamaHadoopFormatConfigure(){}

    public static void init() {
        OkuyamaHadoopFormatConfigure.configureMap = new Hashtable();
        OkuyamaHadoopFormatConfigure.masterNodeInfos = null;
        OkuyamaHadoopFormatConfigure.tagList = new ArrayList();
        OkuyamaHadoopFormatConfigure.fixFlg = false;
    }

    public static void setConfigure(Object key, Object value) throws Exception {
        if (OkuyamaHadoopFormatConfigure.fixFlg) throw new Exception("Error FixConfig = true");
        OkuyamaHadoopFormatConfigure.configureMap.put(key, value);
    }

    public static void setMasterNodeInfoList(String[] masterNodeList) {
        masterNodeInfos = masterNodeList;
    }

    public static void addTag(String tag) {
        tagList.add(tag);
    }

    public static String[] getMasterNodeInfoList() {
        return masterNodeInfos;
    }

    public static String[] getTagList() {

        String[] tagStringList = new String[tagList.size()];
        for (int idx = 0; idx < tagList.size(); idx++) {
            tagStringList[idx] = (String)tagList.get(idx);
        }
        return tagStringList;
    }



    public static void fixConfig() {
        fixFlg = true;
    }

    public static String serializeConfigureData() {
        return null;
    }
    
    public static void loadConfigureData(String data) {
    }
}
