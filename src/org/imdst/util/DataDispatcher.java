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

    private static HashMap subKeyMapNodeInfo = null;

    private static HashMap allNodeMap = null;

    private static boolean standby = false;

    private static Object syncObj = new Object();

    /**
     * 初期化<br>
     * <br>
     * 以下の要素を設定する.<br>
     * KeyMapNodesRule=ルール値(2,9,99,999)<br>
     * KeyMapNodesInfo=Keyノードの設定(KeyNodeName1:11111, KeyNodeName2:22222)<br>
     * SubKeyMapNodesInfo=スレーブKeyノードの設定(KeyNodeName1:11111, KeyNodeName2:22222)<br>
     * SubKeyMapNodesInfoは設定なしも可能。その場合はnullを設定<br>
     * <br>
     * 記述の決まり.<br>
     * <br>
     * KeyMapNodesRule:KeyNodeの数を記載<br>
     *                 ここでの記述は過去の台数の経緯を記載する必要がある<br>
     *                 たとえは5台でまずKeyNodeを稼動させその後10台に増やした場合の記述は「10,5」となる。その後15台にした場合は<br>
     *                 「15,10,5」となる<br>
     * <br>
     * KeyMapNodesInfo:KeyNode(データ保存ノード)をIPアドレス(マシン名)とポート番号を":"で連結した状態で記述<br>
     * <br>
     * SubKeyMapNodesInfo:スレーブとなるのKeyNodeをKeyMapNodesInfoと同様の記述方法で記述。KeyMapNodesInfoと同様の数である必要がある。<br>
     *
     * @param ruleStr ルール設定
     * @param keyMapNodes データノードを指定
     * @param subKeyMapNodes スレーブデータノードを指定
     */
    public static void init(String ruleStr, String keyMapNodes, String subKeyMapNodes) {

        String[]  keyMapNodesInfo = null;
        String[]  subkeyMapNodesInfo = null;

        ArrayList keyNodeList = new ArrayList();
        ArrayList subKeyNodeList = new ArrayList();
        allNodeMap = new HashMap();

        rule = ruleStr.trim();
        ruleInt = new Integer(rule).intValue();

        keyMapNodeInfo = new HashMap();

        keyMapNodesInfo = keyMapNodes.split(",");
        for (int index = 0; index < keyMapNodesInfo.length; index++) {
            String keyNode = keyMapNodesInfo[index].trim();
            keyNodeList.add(keyNode);
            String[] keyNodeDt = keyNode.split(":");

            keyMapNodeInfo.put(index + "_node", keyNodeDt[0]);
            keyMapNodeInfo.put(index + "_port", keyNodeDt[1]);
        }

        allNodeMap.put("main", keyNodeList);
        if (subKeyMapNodes != null && !subKeyMapNodes.equals("")) {
            subKeyMapNodeInfo = new HashMap();
            subkeyMapNodesInfo = subKeyMapNodes.split(",");

            for (int index = 0; index < subkeyMapNodesInfo.length; index++) {
                String subKeyNode = subkeyMapNodesInfo[index].trim();
                String[] subKeyNodeDt = subKeyNode.split(":");
                subKeyNodeList.add(subKeyNode);
                subKeyMapNodeInfo.put(index + "_node", subKeyNodeDt[0]);
                subKeyMapNodeInfo.put(index + "_port", subKeyNodeDt[1]);
            }
            allNodeMap.put("sub", subKeyNodeList);
        }
        standby = true;
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
     * スレーブノードの指定がある場合は同時に値を返す。その場合は配列のレングスが4となる<br>
     *
     * @param key キー値
     * @return String[] 対象キーノードの情報(サーバ名、ポート番号)
     */
    public static String[] dispatchKeyNode(String key, int useRule) {
        String[] ret = null;
        boolean noWaitFlg = false;
    
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

        // スレーブノードの有無に合わせて配列を初期化
        if (subKeyMapNodeInfo != null) {

            ret = new String[4];

            ret[2] = (String)subKeyMapNodeInfo.get(nodeNo + "_node");
            ret[3] = (String)subKeyMapNodeInfo.get(nodeNo + "_port");
        } else {
            ret = new String[2];
        }

        ret[0] = (String)keyMapNodeInfo.get(nodeNo + "_node");
        ret[1] = (String)keyMapNodeInfo.get(nodeNo + "_port");

        // どちらか一方でも一時停止の場合は中断
        while(true) {
            noWaitFlg = true;
            // 停止ステータスか確認する
            if (StatusUtil.isWaitStatus(ret[0] + ":" + ret[1])) noWaitFlg = false;

            if (ret.length > 2) {
                if(StatusUtil.isWaitStatus(ret[2] + ":" + ret[3])) noWaitFlg = false;
            }

            if  (noWaitFlg) break;

            try {
                //System.out.println("DataDispatcher - 停止中");
                Thread.sleep(50);
            } catch (Exception e) {}
        }

        // ノードの使用をマーク
        for (int i = 0; i < ret.length; i = i + 2) {
            StatusUtil.addNodeUse(ret[i] + ":" + ret[i+1]);
        }

        return ret;
    }


    /**
     * 全てのノードの情報を返す.<br>
     * その際返却値のMapには"main"と"sub"という文字列Keyで、それぞれArrayListに<br>
     * 名前とポート番号を":"で連結した状態で格納して返す.<br>
     * "sub"はスレーブノードが設定しれていない場合はなしとなる<br>
     *
     * @return 
     */
    public static HashMap getAllDataNodeInfo() {
        while(!standby) {
            ;
        }
        return allNodeMap;
    }
}