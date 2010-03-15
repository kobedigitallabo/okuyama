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

    private static int[] oldRules = null;

    // 全てのノード情報の詳細を格納
    private static Hashtable keyNodeMap = new Hashtable(6);

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
     * @param oldRules 過去ルール設定
     * @param keyMapNodes データノードを指定
     * @param subKeyMapNodes スレーブデータノードを指定
     */
    public static void init(String ruleStr, int[] oldRules, String keyMapNodes, String subKeyMapNodes) {
        standby = false;
        String[]  keyMapNodesInfo = null;
        String[]  subkeyMapNodesInfo = null;

        ArrayList keyNodeList = new ArrayList();
        ArrayList subKeyNodeList = new ArrayList();
        rule = ruleStr.trim();
        ruleInt = new Integer(rule).intValue();

        synchronized(syncObj) {
            allNodeMap = new HashMap();
        }


        // 全体格納配列初期化
        // 配列内容は
        // [0][*]=メインノードName
        // [1][*]=メインノードPort
        // [2][*]=メインノードFull
        // [3][*]=サブノードName
        // [4][*]=サブノードPort
        // [5][*]=サブノードFull
        keyMapNodesInfo = keyMapNodes.split(",");
        String[][] allNodeDetailList = new String[6][keyMapNodesInfo.length];

        // MainNode初期化
        for (int index = 0; index < keyMapNodesInfo.length; index++) {
            String keyNode = keyMapNodesInfo[index].trim();
            keyNodeList.add(keyNode);

            allNodeDetailList[2][index] = keyNode;

            String[] keyNodeDt = keyNode.split(":");

            allNodeDetailList[0][index] = keyNodeDt[0];
            allNodeDetailList[1][index] = keyNodeDt[1];
        }
        allNodeMap.put("main", keyNodeList);


        // SubNode初期化
        if (subKeyMapNodes != null && !subKeyMapNodes.equals("")) {
            subkeyMapNodesInfo = subKeyMapNodes.split(",");

            for (int index = 0; index < subkeyMapNodesInfo.length; index++) {
                String subKeyNode = subkeyMapNodesInfo[index].trim();
                String[] subKeyNodeDt = subKeyNode.split(":");
                subKeyNodeList.add(subKeyNode);

                allNodeDetailList[5][index] = subKeyNode;
                allNodeDetailList[3][index] = subKeyNodeDt[0];
                allNodeDetailList[4][index] = subKeyNodeDt[1];
            }
            allNodeMap.put("sub", subKeyNodeList);
        }

        keyNodeMap.put("list", allNodeDetailList);
        DataDispatcher.oldRules = oldRules;
        standby = true;
    }

    /**
     * 過去ルールを返す.<br>
     *
     * @return int[] 過去ルールリスト
     */
    public static int[] getOldRules() {
        return oldRules;
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
     * ルールはKeyNodeの台数を記述する。また、システム稼動後KeyNodeを増やす場合、<br>
     * 増やしたルールを先頭にして古いルールを後ろにカンマ区切りで連結する<br>
     * MainNodeとSubNodeの情報を返却値の配列内で逆転させて返すことが可能である.<br>
     *
     * @param key キー値
     * @param reverse 逆転指定
     * @return String 対象キーノードの情報(サーバ名、ポート番号)
     */
    public static String[] dispatchReverseKeyNode(String key, boolean reverse) {
        return dispatchReverseKeyNode(key, reverse, ruleInt);
    }

    /**
     * Rule値に従って、キー値を渡すことで、KeyNodeの名前とポートの配列を返す.<br>
     * ルールはKeyNodeの台数を記述する。また、システム稼動後KeyNodeを増やす場合、<br>
     * 増やしたルールを先頭にして古いルールを後ろにカンマ区切りで連結する<br>
     * MainNodeとSubNodeの情報を返却値の配列内で逆転させて返すことが可能である.<br>
     *
     * @param key キー値
     * @param reverse 逆転指定
     * @param useRule ルール指定
     * @return String 対象キーノードの情報(サーバ名、ポート番号)
     */
    public static String[] dispatchReverseKeyNode(String key, boolean reverse, int useRule) {
        String[] ret = null;
        String[] tmp = dispatchKeyNode(key, useRule);

        if (reverse) {
            // SubNodeが存在する場合は逆転させる
            if (tmp.length > 3) {
                ret = new String[6];
                ret[3] = tmp[0];
                ret[4] = tmp[1];
                ret[5] = tmp[2];

                ret[0] = tmp[3];
                ret[1] = tmp[4];
                ret[2] = tmp[5];
            }
        } else {
            ret = tmp;
        }
        return ret;
    }


    /**
     * Rule値に従って、キー値を渡すことで、KeyNodeの名前とポートの配列を返す.<br>
     * スレーブノードの指定がある場合は同時に値を返す。その場合は配列のレングスが6となる<br>
     * ノード振り分けアルゴリズムは除算のあまりより決定.<br>
	 * hash値 % ノード台数 = 振り分け先.<br>
	 *
     * @param key キー値
     * @param useRule ルール値
     * @return String[] 対象キーノードの情報(サーバ名、ポート番号)
     */
    public static String[] dispatchKeyNode(String key, int useRule) {
        String[] ret = null;
        boolean noWaitFlg = false;

        // ノード詳細取り出し
        String[][] allNodeDetailList = (String[][])keyNodeMap.get("list");

        // Key値からHash値作成
        int execKeyInt = key.hashCode();

        if (execKeyInt < 0) {
            String work = new Integer(execKeyInt).toString();
            execKeyInt = Integer.parseInt(work.substring(1,work.length()));
        }

        int nodeNo = execKeyInt % useRule;

        if (nodeNo == 0) {
            nodeNo = useRule;
        }

        nodeNo = nodeNo - 1;

        // スレーブノードの有無に合わせて配列を初期化
        if (allNodeDetailList[3][0] != null) {

            ret = new String[6];

            ret[3] = allNodeDetailList[3][nodeNo];
            ret[4] = allNodeDetailList[4][nodeNo];
            ret[5] = allNodeDetailList[5][nodeNo];
        } else {
            ret = new String[3];
        }

        ret[0] = allNodeDetailList[0][nodeNo];
        ret[1] = allNodeDetailList[1][nodeNo];
        ret[2] = allNodeDetailList[2][nodeNo];


        // 該当ノードが一時使用停止の場合は使用再開されるまで停止(データ復旧時に起こりえる)
        // どちらか一方でも一時停止の場合はWait
        while(true) {
            noWaitFlg = true;
            // 停止ステータスか確認する
            if (StatusUtil.isWaitStatus(allNodeDetailList[2][nodeNo])) noWaitFlg = false;

            if (ret.length > 3) {
                if(StatusUtil.isWaitStatus(allNodeDetailList[5][nodeNo])) noWaitFlg = false;
            }

            if  (noWaitFlg) break;

            try {
                //System.out.println("DataDispatcher - 停止中");
                Thread.sleep(50);
            } catch (Exception e) {}
        }

        // ノードの使用をマーク
        StatusUtil.addNodeUse(allNodeDetailList[2][nodeNo]);

        if (ret.length > 3) {
            StatusUtil.addNodeUse(allNodeDetailList[5][nodeNo]);
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
            try {
				Thread.sleep(50);
			} catch (Exception e) {
			}
        }

        HashMap retMap = null;
        ArrayList mainNodeList = new ArrayList();
        ArrayList subNodeList = new ArrayList();

        // 内容を複製して返す
        synchronized(syncObj) {

            retMap = new HashMap(2);

            ArrayList tmpNodeList = (ArrayList)allNodeMap.get("main");

            for (int i = 0; i < tmpNodeList.size(); i++) {
                mainNodeList.add(tmpNodeList.get(i));
            }
            retMap.put("main", mainNodeList);

            if (allNodeMap.containsKey("sub")) {
                tmpNodeList = (ArrayList)allNodeMap.get("sub");

                for (int i = 0; i < tmpNodeList.size(); i++) {
                    subNodeList.add(tmpNodeList.get(i));
                }
                retMap.put("sub", subNodeList);
            }
        }
        return retMap;
    }

    public static boolean isStandby() {
        while(!standby) {
            try {
				Thread.sleep(50);
			} catch (Exception e) {
			}
        }
        return standby;
    }
}