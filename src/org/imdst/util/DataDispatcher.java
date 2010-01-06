package org.imdst.util;

import java.util.*;
import java.io.*;

import org.batch.util.ILogger;
import org.batch.util.LoggerFactory;
import org.batch.lang.BatchException;

/**
 * MasterNodeが使用するDataNode決定モジュール.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class DataDispatcher {

    private static String rule = null;

    private static int ruleInt = 0;
    
    private static HashMap keyMapNodeInfo = null;

    private static ArrayList dataNodeNameList = null;
    private static ArrayList dataNodePortList = null;

    private static ArrayList dataSubNodeNameList = null;
    private static ArrayList dataSubNodePortList = null;

    private static int nowPoint = 0;

    private static Object syncObj = new Object();

    /**
     * 初期化<br>
     * <br>
     * 以下の要素を設定する.<br>
     * KeyMapNodesRule=ルール値(2,9,99,999)<br>
     * KeyMapNodesInfo=Keyノードの設定(KeyNodeName1:11111, KeyNodeName2:22222)<br>
     * DataNodesInfo=Dataノードの設定(DataNodeName1:11111, DataNodeName2:22222, DataNodeName3:33333)<br>
     * DataSubNodesInfo=DataSubノードの設定(DataSubNodeName1:11111, DataSubNodeName2:22222, DataSubNodeName3:33333)<br>
     * <br>
     * 記述の決まり.<br>
     * <br>
     * KeyMapNodesRule:2は2台KeyNodeを指定できる<br>
     *                :9は10台KeyNodeを指定できる<br>
     *                :99は100台KeyNodeを指定できる<br>
     *                :999は1000台KeyNodeを指定できる<br>
     * <br>
     * KeyMapNodesInfo:KeyNodeを名前とポート番号で指定する。並び順通りにデータを保持されるので、一度保存しだすと変更は不可となる.<br>
     *
     *
     *
     *
     *
     *
     */
    public static void init(String ruleStr, String keyMapNodes, String dataNodes, String subDataNodes) {

        rule = ruleStr.trim();
        ruleInt = new Integer(rule).intValue();

        dataNodeNameList = new ArrayList();
        dataNodePortList = new ArrayList();
        dataSubNodeNameList = new ArrayList();
        dataSubNodePortList = new ArrayList();

        if (dataNodes != null && !dataNodes.equals("")) {

            String[] dataNodeInfo = dataNodes.split(",");
            for (int i = 0; i < dataNodeInfo.length; i ++) {
                String dataNode = dataNodeInfo[i].trim();

                String[] dataNodeDt = dataNode.split(":");
                dataNodeNameList.add(dataNodeDt[0]);
                dataNodePortList.add(dataNodeDt[1]);
            }
        }

        if (subDataNodes != null && !subDataNodes.equals("")) {
            String[] dataSubNodeInfo = subDataNodes.split(",");

            for (int i = 0; i < dataSubNodeInfo.length; i ++) {
                String dataSubNode = dataSubNodeInfo[i].trim();

                String[] dataSubNodeDt = dataSubNode.split(":");
                dataSubNodeNameList.add(dataSubNodeDt[0]);
                dataSubNodePortList.add(dataSubNodeDt[1]);
            }
        }

        keyMapNodeInfo = new HashMap();

        String[]  keyMapNodesInfo = keyMapNodes.split(",");
        
        for (int index = 0; index < keyMapNodesInfo.length; index++) {
            String keyNode = keyMapNodesInfo[index].trim();
            String[] keyNodeDt = keyNode.split(":");

            keyMapNodeInfo.put(index + "_node", keyNodeDt[0]);
            keyMapNodeInfo.put(index + "_port", keyNodeDt[1]);
        }

    }

    /**
     * Rule値に従って、キー値を渡すことで、KeyNodeの名前とポートの配列を返す.<br>
     * ルールはKeyNodeの台数を記述する。また、システム稼動後KeyNodeを増やす場合、<br>
     * 増やしたルールを先頭にして古いルールを後ろにカンマ区切りで連結する<br>
     *
     * @param key キー値
     * @return String 対象キーノードの情報(サーバ名、ポート番号)
     */
    public static String[] dispatchKeyNode(String key) {
        return dispatchKeyNode(key, ruleInt);

    }
    /**
     * Rule値に従って、キー値を渡すことで、KeyNodeの名前とポートの配列を返す.<br>
     * 存在するルールは2,10,100,1000 それぞれ2台、10台,100台、1000台である<br>
     *
     * @param key キー値
     * @return String 対象キーノードの情報(サーバ名、ポート番号)
     */
    public static String[] dispatchKeyNode(String key, int useRule) {
        String[] ret = new String[2];
    
        int execKeyInt = key.hashCode();
        if (execKeyInt < 0) {
            String work = new Integer(execKeyInt).toString();
            execKeyInt = new Integer(work.substring(1,work.length())).intValue();
        }

        int nodeNo = execKeyInt % new Integer(useRule).intValue();
        if (nodeNo == 0) {
            nodeNo = useRule;
        }

        nodeNo = nodeNo - 1;
        ret[0] = (String)keyMapNodeInfo.get(nodeNo + "_node");
        ret[1] = (String)keyMapNodeInfo.get(nodeNo + "_port");

/*        String keyIntStr = ((Integer)new Integer(key.hashCode())).toString();
        String keyNodeNo = null;
        String[] ret = new String[2];

        if (rule.equals("2")) {
            keyNodeNo = keyIntStr.substring(keyIntStr.length() - 1, keyIntStr.length());
            if (new Integer(keyNodeNo).intValue() < 5) {
                keyNodeNo = "0";
            } else {
                keyNodeNo = "1";
            }
        } else if (rule.equals("10")) {
            keyNodeNo = keyIntStr.substring(keyIntStr.length() - 1, keyIntStr.length());
        } else if (rule.equals("100")) {
            keyNodeNo = keyIntStr.substring(keyIntStr.length() - 2, keyIntStr.length());
        } else if (rule.equals("1000")) {
            keyNodeNo = keyIntStr.substring(keyIntStr.length() - 3, keyIntStr.length());
        }

        ret[0] = (String)keyMapNodeInfo.get(keyNodeNo + "_node");
        ret[1] = (String)keyMapNodeInfo.get(keyNodeNo + "_port");
*/
        return ret;
    }

/*
    public static String[] dispatchKeyNode(String key, String useRule) {

        String keyIntStr = ((Integer)new Integer(key.hashCode())).toString();
        String keyNodeNo = null;
        String[] ret = new String[2];

        if (useRule.equals("2")) {
            keyNodeNo = keyIntStr.substring(keyIntStr.length() - 1, keyIntStr.length());
            if (new Integer(keyNodeNo).intValue() < 5) {
                keyNodeNo = "0";
            } else {
                keyNodeNo = "1";
            }
        } else if (useRule.equals("10")) {
            keyNodeNo = keyIntStr.substring(keyIntStr.length() - 1, keyIntStr.length());
        } else if (useRule.equals("100")) {
            keyNodeNo = keyIntStr.substring(keyIntStr.length() - 2, keyIntStr.length());
        } else if (useRule.equals("1000")) {
            keyNodeNo = keyIntStr.substring(keyIntStr.length() - 3, keyIntStr.length());
        }

        ret[0] = (String)keyMapNodeInfo.get(keyNodeNo + "_node");
        ret[1] = (String)keyMapNodeInfo.get(keyNodeNo + "_port");
        return ret;
    }
*/
    /**
     * DataNodeの名前と、ポートの配列を返す.<br>
     * SubDataNodeが存在する場合は配列の3番目と4番目にはSubDataNodeの名前と、ポートが格納される.<br>
     *
     */
    public static String[] dispatchDataNode() {
        int point = 0;
        String[] ret = new String[4];

        synchronized(syncObj) {
            if (nowPoint < dataNodeNameList.size()) {
                point = nowPoint;
                nowPoint++;
            } else {
                nowPoint = 0;
            }
        }

        ret[0] = (String)dataNodeNameList.get(point);
        ret[1] = (String)dataNodePortList.get(point);

        if (dataSubNodeNameList.size() > 0) {
            ret[2] = (String)dataSubNodeNameList.get(point);
            ret[3] = (String)dataSubNodePortList.get(point);
        }
        return ret;
    }
}