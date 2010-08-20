package org.imdst.util;

import java.util.*;
import java.io.*;
import java.util.concurrent.ConcurrentHashMap;

import org.batch.util.ILogger;
import org.batch.util.LoggerFactory;
import org.batch.lang.BatchException;

import org.apache.commons.codec.digest.DigestUtils;

/**
 * MasterNodeが使用するDataNode決定モジュール.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class DataDispatcher {

    public volatile static String rule = null;

    public volatile static int ruleInt = 0;

    public volatile static int[] oldRules = null;

    // 全てのノード情報の詳細を格納
    private static ConcurrentHashMap keyNodeMap = new ConcurrentHashMap(6, 6, 64);

    // ConsistentHash用Circle
    private static SortedMap nodeCircle = null;

    // ConsistentHash用OldCircle
    private static SortedMap oldCircle = null;


    private static HashMap allNodeMap = null;

    private static ArrayList transactionManagerList = null;

    private volatile static boolean standby = false;

    private static Object syncObj = new Object();

    private static int virtualNodeSize = ImdstDefine.consistentHashVirtualNode;

    // 振り分けモード 0=mod 1=ConsistentHash
    private static int dispatchMode = ImdstDefine.dispatchModeModInt;

    // 振り分けモードの初期化状態を表す
    private static boolean fixDispatchMode = false;

    // 自身を一度でも初期している場合はtrue;
    private static boolean initFlg = false;


    // 振り分けモードを設定
    // 1度のみ呼び出し可能
    public static void setDispatchMode(String mode) {
        if (!fixDispatchMode) {
            if (mode.equals(ImdstDefine.dispatchModeConsistentHash)) {
                dispatchMode = ImdstDefine.dispatchModeConsistentHashInt;
                fixDispatchMode = true;
            }
        }
    }


    /**
     * 振り分けモードを返す.<br>
     *
     * @return int 0=mod, 1=ConsistentHash
     */
    public static int getDispatchMode() {
        return dispatchMode;
    }


    /**
     * 初期化<br>
     * <br>
     * 以下の要素を設定する.<br>
     * KeyMapNodesRule=ルール値(2,9,99,999)<br>
     * KeyMapNodesInfo=Keyノードの設定(KeyNodeName1:11111, KeyNodeName2:22222)<br>
     * SubKeyMapNodesInfo=スレーブKeyノードの設定(KeyNodeName1:11111, KeyNodeName2:22222)<br>
     * SubKeyMapNodesInfoは設定なしも可能。その場合はnullを設定<br>
     * ThirdKeyMapNodesInfo=サードKeyノードの設定(KeyNodeName1:11111, KeyNodeName2:22222)<br>
     * ThirdKeyMapNodesInfoは設定なしも可能。その場合はnullを設定<br>
     * ※SubKeyMapNodesInfoは設定しないとThirdKeyMapNodesInfoは設定することは出来ない
     *
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
     * <br>
     * ThirdKeyMapNodesInfo:サードとなるのKeyNodeをKeyMapNodesInfoと同様の記述方法で記述。KeyMapNodesInfoと同様の数である必要がある。<br>
     *
     * @param ruleStr ルール設定
     * @param oldRules 過去ルール設定
     * @param keyMapNodes データノードを指定
     * @param subKeyMapNodes スレーブデータノードを指定
     * @param thirdKeyMapNodes スレーブデータノードを指定
     * @param transactionManagerStr トランザクションマネージャの指定
     */
    public static void init(String ruleStr, int[] oldRules, String keyMapNodes, String subKeyMapNodes, String thirdKeyMapNodes, String transactionManagerStr) {
        initFlg = true;
        standby = false;
        String[] keyMapNodesInfo = null;
        String[] subkeyMapNodesInfo = null;
        String[] thirdkeyMapNodesInfo = null;
        String[] transactionManagerInfo = null;

        ArrayList keyNodeList = new ArrayList();
        ArrayList subKeyNodeList = new ArrayList();
        ArrayList thirdKeyNodeList = new ArrayList();
        rule = ruleStr.trim();
        ruleInt = new Integer(rule).intValue();

        synchronized(syncObj) {
            allNodeMap = new HashMap();
            // TransactionManager設定初期化
            if (transactionManagerStr != null) {
                transactionManagerList = new ArrayList();
                transactionManagerList.add(transactionManagerStr);
            }
        }


        // 全体格納配列初期化
        // 配列内容は
        // [0][*]=メインノードName
        // [1][*]=メインノードPort
        // [2][*]=メインノードFull
        // [3][*]=サブノードName
        // [4][*]=サブノードPort
        // [5][*]=サブノードFull
        // [6][*]=サードノードName
        // [7][*]=サードノードPort
        // [8][*]=サードノードFull
        keyMapNodesInfo = keyMapNodes.split(",");
        String[][] allNodeDetailList = new String[9][keyMapNodesInfo.length];

        // MainNode初期化
        for (int index = 0; index < keyMapNodesInfo.length; index++) {
            String keyNode = keyMapNodesInfo[index].trim();
            keyNodeList.add(keyNode);

            allNodeDetailList[2][index] = keyNode;

            String[] keyNodeDt = keyNode.split(":");

            allNodeDetailList[0][index] = keyNodeDt[0];
            allNodeDetailList[1][index] = keyNodeDt[1];
        }

        synchronized(syncObj) {
            allNodeMap.put("main", keyNodeList);
        }

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

            synchronized(syncObj) {
                allNodeMap.put("sub", subKeyNodeList);
            }
        }


        // ThirdNode初期化
        if (thirdKeyMapNodes != null && !thirdKeyMapNodes.equals("")) {
            thirdkeyMapNodesInfo = thirdKeyMapNodes.split(",");

            for (int index = 0; index < thirdkeyMapNodesInfo.length; index++) {
                String thirdKeyNode = thirdkeyMapNodesInfo[index].trim();
                String[] thirdKeyNodeDt = thirdKeyNode.split(":");
                thirdKeyNodeList.add(thirdKeyNode);

                allNodeDetailList[8][index] = thirdKeyNode;
                allNodeDetailList[6][index] = thirdKeyNodeDt[0];
                allNodeDetailList[7][index] = thirdKeyNodeDt[1];
            }

            synchronized(syncObj) {
                allNodeMap.put("third", thirdKeyNodeList);
            }
        }

        DataDispatcher.oldRules = oldRules;
        keyNodeMap.put("list", allNodeDetailList);
        standby = true;
    }


    /**
     * 初期化<br>
     * ConsistentHash専用.<br>
     * <br>
     * 以下の要素を設定する.<br>
     * KeyMapNodesInfo=Keyノードの設定(KeyNodeName1:11111, KeyNodeName2:22222)<br>
     * SubKeyMapNodesInfo=スレーブKeyノードの設定(KeyNodeName1:11111, KeyNodeName2:22222)<br>
     * SubKeyMapNodesInfoは設定なしも可能。その場合はnullを設定<br>
     * ThirdKeyMapNodesInfo=スレーブKeyノードの設定(KeyNodeName1:11111, KeyNodeName2:22222)<br>
     * ThirdKeyMapNodesInfoは設定なしも可能。その場合はnullを設定<br>
     * ※SubKeyMapNodesInfoは設定しないがThirdKeyMapNodesInfoは設定することは出来ない
     * <br>
     * 記述の決まり.<br>
     * <br>
     * <br>
     * KeyMapNodesInfo:KeyNode(データ保存ノード)をIPアドレス(マシン名)とポート番号を":"で連結した状態で記述<br>
     * <br>
     * SubKeyMapNodesInfo:スレーブとなるのKeyNodeをKeyMapNodesInfoと同様の記述方法で記述。KeyMapNodesInfoと同様の数である必要がある。<br>
     * <br>
     * ThirdKeyMapNodesInfo:サードとなるのKeyNodeをKeyMapNodesInfoと同様の記述方法で記述。KeyMapNodesInfoと同様の数である必要がある。<br>
     *
     * @param keyMapNodes データノードを指定
     * @param subKeyMapNodes スレーブデータノードを指定
     * @param thirdKeyMapNodes スレーブデータノードを指定
     * @param transactionManagerStr トランザクションマネージャの指定
     */
    public static void initConsistentHashMode(String keyMapNodes, String subKeyMapNodes, String thirdKeyMapNodes, String transactionManagerStr) {
        initFlg = true;
        standby = false;
        String[] keyMapNodesInfo = null;
        String[] subkeyMapNodesInfo = null;
        String[] thirdkeyMapNodesInfo = null;
        String[] transactionManagerInfo = null;

        ArrayList keyNodeList = new ArrayList();
        ArrayList subKeyNodeList = new ArrayList();
        ArrayList thirdKeyNodeList = new ArrayList();

        synchronized(syncObj) {
            allNodeMap = new HashMap();
            // TransactionManager設定初期化
            if (transactionManagerStr != null) {
                transactionManagerList = new ArrayList();
                transactionManagerList.add(transactionManagerStr);
            }
        }


        // 全体格納配列初期化
        // 配列内容は
        // [0][*]=メインノードName
        // [1][*]=メインノードPort
        // [2][*]=メインノードFull
        // [3][*]=サブノードName
        // [4][*]=サブノードPort
        // [5][*]=サブノードFull
        // [6][*]=サードノードName
        // [7][*]=サードノードPort
        // [8][*]=サードノードFull
        keyMapNodesInfo = keyMapNodes.split(",");
        String[][] allNodeDetailList = new String[9][keyMapNodesInfo.length];

        // メインデータノード用ConsistentHashのサークル作成
        nodeCircle = new TreeMap();

        // MainNode初期化
        for (int index = 0; index < keyMapNodesInfo.length; index++) {
            String keyNode = keyMapNodesInfo[index].trim();
            String[] keyNodeDt = keyNode.split(":");
            keyNodeList.add(keyNode);

            allNodeDetailList[2][index] = keyNode;
            allNodeDetailList[0][index] = keyNodeDt[0];
            allNodeDetailList[1][index] = keyNodeDt[1];
            String[] mainNodeDt = {allNodeDetailList[0][index], allNodeDetailList[1][index], allNodeDetailList[2][index]};

            keyNodeMap.put(keyNode, mainNodeDt);

            // ConsistentHash用のサークルを初期化
            for (int i = 0; i < virtualNodeSize; i++) {

                // ノードFullNameを使用してその値のバーチャル数を連結した値をHash化してその、Circle登録時に
                // KeyNodeのフルネームを入れる
                // 後から、keyNodeMapから取り出すため
                nodeCircle.put(new Integer(sha1Hash4Int(keyNode + "_" + i)), keyNode);
            }
        }


        synchronized(syncObj) {
            allNodeMap.put("main", keyNodeList);
        }

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
                String[] subNodeDt = {allNodeDetailList[3][index], allNodeDetailList[4][index], allNodeDetailList[5][index]};

                keyNodeMap.put(allNodeDetailList[2][index] + "_sub", subNodeDt);
            }

            synchronized(syncObj) {
                allNodeMap.put("sub", subKeyNodeList);
            }
        }


        // ThirdNode初期化
        if (thirdKeyMapNodes != null && !thirdKeyMapNodes.equals("")) {
            thirdkeyMapNodesInfo = thirdKeyMapNodes.split(",");

            for (int index = 0; index < thirdkeyMapNodesInfo.length; index++) {
                String thirdKeyNode = thirdkeyMapNodesInfo[index].trim();
                String[] thirdKeyNodeDt = thirdKeyNode.split(":");
                thirdKeyNodeList.add(thirdKeyNode);

                allNodeDetailList[8][index] = thirdKeyNode;
                allNodeDetailList[6][index] = thirdKeyNodeDt[0];
                allNodeDetailList[7][index] = thirdKeyNodeDt[1];
                String[] thirdNodeDt = {allNodeDetailList[6][index], allNodeDetailList[7][index], allNodeDetailList[8][index]};

                keyNodeMap.put(allNodeDetailList[2][index] + "_third", thirdNodeDt);
            }

            synchronized(syncObj) {
                allNodeMap.put("third", thirdKeyNodeList);
            }
        }


        keyNodeMap.put("list", allNodeDetailList);
        // 準備完了
        standby = true;
    }


    /**
     * 初期化の状態を返す.<br>
     *
     */ 
    public static boolean getInitFlg() {
        return initFlg;
    }


    /**
     * ConsitentHashモード時にノードの追加をおこなう.<br>
     * 本メソッドを呼びだすと、新しいノードのサークルを作りなおす前に、旧ノードのサークルを作成し、keyNodeMapに<br>
     * "oldNodeCircle"という名前で登録する.<br>
     * 返却値はノード登録によって移動しなければいけないデータのHash化した数値の範囲データ。このデータをノードの<br>
     * FullNameをキー値としてHashMapに詰めて返す.<br>
     * メインデータノードは返却値のHashMapに"main"というキー値で、スレーブは"sub"というキーとで、サードは"third"というキーとでMapが格納されている.<br>
     * 移動先のノードはメインノードが"tomain"というキー値で、スレーブが"tosub"というキー値で、サードが"tothird"というキー値で格納されている.<br>
     *
     * @param keyNodeFullName 追加するメインデータノード フォーマット"192.168.1.3:5555"
     * @param subKeyNodeFullName 追加するスレーブデータノード フォーマット"192.168.2.3:5555"
     * @param thirdKeyNodeFullName 追加するサードデータノード フォーマット"192.168.2.3:5555"
     * @return HashMap 変更対象データの情報
     */
    public static HashMap addNode4ConsistentHash(String keyNodeFullName, String subKeyNodeFullName, String thirdKeyNodeFullName) {
        if (oldCircle != null) return null;
        HashMap retMap = new HashMap(2);
        HashMap convertMap = new HashMap();
        HashMap subConvertMap = new HashMap();
        HashMap thirdConvertMap = new HashMap();
        ArrayList keyNodeList = new ArrayList();
        ArrayList subKeyNodeList = new ArrayList();
        ArrayList thirdKeyNodeList = new ArrayList();

        oldCircle = new TreeMap();

        // 現状のサークルのコピーを作成
        Set set = nodeCircle.keySet();
        Iterator iterator = set.iterator();

        // oldCircle作成
        while(iterator.hasNext()) {
            Integer key = (Integer)iterator.next();
            String nodeFullName = (String)nodeCircle.get(key);
            oldCircle.put(key, nodeFullName);
        }

        // データ移動表作成
        convertMap = new HashMap();


        for (int i = 0; i < virtualNodeSize; i++) {

            int targetHash = sha1Hash4Int(keyNodeFullName + "_" + i);
            int targetHashStart = 0;
            int targetHashEnd = targetHash;
            String nodeName = null;

            SortedMap headMap = nodeCircle.headMap(targetHash);
            SortedMap tailMap = nodeCircle.tailMap(targetHash);

            // 登録されたノードの仮想ノード単位でどのレンジのデータを必要としているかを
            // 自身から大きい数値に1つめのノードまでの距離で求める
            // どのノードからどれだけのレンジのデータが必要か求める
            // たとえばNode01,Node02,Node03に対してNode04を追加した場合に、
            // Node01から5111～12430まで、Node02から45676～987654とか
            if (headMap.isEmpty()) {

                int hash = ((Integer)nodeCircle.lastKey()).intValue();
                targetHashStart = hash + 1;
                nodeName = (String)nodeCircle.get(nodeCircle.firstKey());
            } else {
                
                int hash = ((Integer)headMap.lastKey()).intValue();
                targetHashStart = hash + 1;
                if (tailMap.isEmpty()) {

                    nodeName = (String)nodeCircle.get(nodeCircle.firstKey());
                } else {

                    nodeName = (String)nodeCircle.get(tailMap.firstKey());
                }
            }


            // 求めたレンジを取得ノード名単位でまとめる
            // Node01:5553,"6756-9876,12345-987654"
            // Node02:5553,"342-3456,156456-178755"
            if (convertMap.containsKey(nodeName)) {

                String work = (String)convertMap.get(nodeName);
                convertMap.put(nodeName, work + "," + targetHashStart + "-" +  targetHashEnd);

                // サブ
                String[] subDataNodeInfo = (String[])keyNodeMap.get(nodeName + "_sub");
                if (subDataNodeInfo != null) {
                    subConvertMap.put(subDataNodeInfo[2], work + "," + targetHashStart + "-" +  targetHashEnd);
                }

                // サード
                String[] thirdDataNodeInfo = (String[])keyNodeMap.get(nodeName + "_third");
                if (thirdDataNodeInfo != null) {
                    thirdConvertMap.put(thirdDataNodeInfo[2], work + "," + targetHashStart + "-" +  targetHashEnd);
                }

            } else {

                convertMap.put(nodeName, targetHashStart + "-" +  targetHashEnd);

                // サブ
                String[] subDataNodeInfo = (String[])keyNodeMap.get(nodeName + "_sub");
                if (subDataNodeInfo != null) {
                    subConvertMap.put(subDataNodeInfo[2], targetHashStart + "-" +  targetHashEnd);
                }

                // サード
                String[] thirdDataNodeInfo = (String[])keyNodeMap.get(nodeName + "_third");
                if (thirdDataNodeInfo != null) {
                    thirdConvertMap.put(thirdDataNodeInfo[2], targetHashStart + "-" +  targetHashEnd);
                }

            }
        }

        // 返却用のデータ移動支持Mapに登録
        retMap.put("tomain",keyNodeFullName);
        retMap.put("tosub",subKeyNodeFullName);
        retMap.put("tothird",thirdKeyNodeFullName);
        retMap.put("main", convertMap);
        retMap.put("sub", subConvertMap);
        retMap.put("third", thirdConvertMap);


        // 全体格納配列に追加
        // 配列内容は
        // [0][*]=メインノードName
        // [1][*]=メインノードPort
        // [2][*]=メインノードFull
        // [3][*]=サブノードName
        // [4][*]=サブノードPort
        // [5][*]=サブノードFull
        // [6][*]=サードノードName
        // [7][*]=サードノードPort
        // [8][*]=サードノードFull
        String[][] allNodeDetailList = (String[][])keyNodeMap.get("list");
        String[][] newAllNodeDetailList = new String[9][allNodeDetailList.length + 1];
        keyNodeList = (ArrayList)allNodeMap.get("main");

        // allNodeDetailListに追加するために複製を作成
        for (int allNodeDetailListIdx = 0; allNodeDetailListIdx < allNodeDetailList[0].length; allNodeDetailListIdx++) {
            newAllNodeDetailList[0][allNodeDetailListIdx] = allNodeDetailList[0][allNodeDetailListIdx];
            newAllNodeDetailList[1][allNodeDetailListIdx] = allNodeDetailList[1][allNodeDetailListIdx];
            newAllNodeDetailList[2][allNodeDetailListIdx] = allNodeDetailList[2][allNodeDetailListIdx];
            newAllNodeDetailList[3][allNodeDetailListIdx] = allNodeDetailList[3][allNodeDetailListIdx];
            newAllNodeDetailList[4][allNodeDetailListIdx] = allNodeDetailList[4][allNodeDetailListIdx];
            newAllNodeDetailList[5][allNodeDetailListIdx] = allNodeDetailList[5][allNodeDetailListIdx];
            newAllNodeDetailList[6][allNodeDetailListIdx] = allNodeDetailList[6][allNodeDetailListIdx];
            newAllNodeDetailList[7][allNodeDetailListIdx] = allNodeDetailList[7][allNodeDetailListIdx];
            newAllNodeDetailList[8][allNodeDetailListIdx] = allNodeDetailList[8][allNodeDetailListIdx];


        }

        String keyNode = keyNodeFullName;
        String[] keyNodeDt = keyNode.split(":");
        keyNodeList.add(keyNode);

        // 新しいallNodeDetailListに追加
        newAllNodeDetailList[2][allNodeDetailList.length] = keyNode;
        newAllNodeDetailList[0][allNodeDetailList.length] = keyNodeDt[0];
        newAllNodeDetailList[1][allNodeDetailList.length] = keyNodeDt[1];
        String[] mainNodeDt = {keyNodeDt[0], keyNodeDt[1], keyNode};

        // keyNodeMapにも追加
        keyNodeMap.put(keyNode, mainNodeDt);

        // ConsistentHash用のサークルに追加
        for (int i = 0; i < virtualNodeSize; i++) {

            // ノードFullNameを使用してその値のバーチャル数を連結した値をHash化して、Circle登録時に
            // KeyNodeのフルネームを入れる
            // 後から、keyNodeMapから取り出すため
            nodeCircle.put(new Integer(sha1Hash4Int(keyNode + "_" + i)), keyNode);
        }


        synchronized(syncObj) {
            allNodeMap.put("main", keyNodeList);
        }


        // SubNode初期化
        if (subKeyNodeFullName != null && !subKeyNodeFullName.equals("")) {
            String subKeyNode = subKeyNodeFullName;
            String[] subKeyNodeDt = subKeyNode.split(":");
            subKeyNodeList = (ArrayList)allNodeMap.put("sub", subKeyNodeList);

            subKeyNodeList.add(subKeyNode);

            newAllNodeDetailList[5][allNodeDetailList.length] = subKeyNode;
            newAllNodeDetailList[3][allNodeDetailList.length] = subKeyNodeDt[0];
            newAllNodeDetailList[4][allNodeDetailList.length] = subKeyNodeDt[1];
            String[] subNodeDt = {subKeyNodeDt[0], subKeyNodeDt[1], subKeyNode};

            keyNodeMap.put(newAllNodeDetailList[2][allNodeDetailList.length] + "_sub", subNodeDt);

            synchronized(syncObj) {
                allNodeMap.put("sub", subKeyNodeList);
            }
        }


        // ThirdNode初期化
        if (thirdKeyNodeFullName != null && !thirdKeyNodeFullName.equals("")) {
            String thirdKeyNode = thirdKeyNodeFullName;
            String[] thirdKeyNodeDt = thirdKeyNode.split(":");
            thirdKeyNodeList = (ArrayList)allNodeMap.put("third", thirdKeyNodeList);

            thirdKeyNodeList.add(thirdKeyNode);

            newAllNodeDetailList[8][allNodeDetailList.length] = thirdKeyNode;
            newAllNodeDetailList[6][allNodeDetailList.length] = thirdKeyNodeDt[0];
            newAllNodeDetailList[7][allNodeDetailList.length] = thirdKeyNodeDt[1];
            String[] thirdNodeDt = {thirdKeyNodeDt[0], thirdKeyNodeDt[1], thirdKeyNode};

            keyNodeMap.put(newAllNodeDetailList[2][allNodeDetailList.length] + "_third", thirdNodeDt);

            synchronized(syncObj) {
                allNodeMap.put("third", thirdKeyNodeList);
            }
        }

        // 新しい全体リストを上書き
        keyNodeMap.put("list", newAllNodeDetailList);


        return retMap;
    }

    /**
     * 旧サークルが存在する場合は削除する.<br>
     *
     */
    public static void clearConsistentHashOldCircle() {
        if (oldCircle != null) {
            oldCircle = null;
        }
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
     * スレーブノードの指定がある場合は同時に値を返す。その場合は配列のレングスが6、Thirdが存在する場合は9となる<br>
     * 振り分けアルゴリズムはMod or ConsistentHash<br>
     * [Mod]<br>
     * ノード振り分けアルゴリズムは除算のあまりより決定<br>
     * hash値 % ノード台数 = 振り分け先<br>
     * <br>
     * [ConsistentHash]<br>
     * ConsistentHash法にもとづき決定.<br>
     *
     * @param key キー値
     * @param reverse 逆転指定
     * @return String 対象キーノードの情報(サーバ名、ポート番号)
     */
    public static String[] dispatchKeyNode(String key, boolean reverse) {
        String[] ret = null;
        if (dispatchMode == 0) {

            // mod
            ret = dispatchModKeyNode(key, reverse);
        } else if (dispatchMode == 1) {

            // Consistent Hash
            ret = dispatchConsistentHashKeyNode(key, reverse);
        }
        return ret;
    }


    /**
     * Rule値に従って、キー値を渡すことで、KeyNodeの名前とポートの配列を返す.<br>
     * スレーブノードの指定がある場合は同時に値を返す。その場合は配列のレングスが6、Thirdが存在する場合は9となる<br>
     * 振り分けアルゴリズムはMod or ConsistentHash<br>
     * 本メソッドはノードを追加後に旧ノード構成時のルールで格納しているノードを返す.<br>
     * Modではノード追加に合わせて過去の履歴でそれぞれのノードに問い合わせる<br>
     *
     *
     * [Mod]<br>
     * ノード振り分けアルゴリズムは除算のあまりより決定<br>
     * hash値 % ノード台数 = 振り分け先<br>
     * <br>
     * [ConsistentHash]<br>
     * ConsistentHash法にもとづき決定.<br>
     *
     * @param key キー値
     * @param reverse 逆転指定
     * @param useRule 旧ルール世代値
     * @return String[] 対象キーノードの情報(サーバ名、ポート番号)
     */
    public static String[] dispatchKeyNode(String key, boolean reverse, int useRule) {

        String[] ret = null;
        if (dispatchMode == 0) {

            // mod
            if (oldRules != null && oldRules.length > useRule)
                ret = dispatchModKeyNode(key, reverse, oldRules[useRule]);
        } else if (dispatchMode == 1) {

            // Consistent Hash
            if (useRule == 1 && oldCircle != null) 
                ret = dispatchConsistentHashKeyNode(key, reverse, true);
        }
        return ret;
    }



    /**
     * Rule値に従って、キー値を渡すことで、KeyNodeの名前とポートの配列を返す.<br>
     * Modアルゴリズム.<br>
     * ルールはKeyNodeの台数を記述する。また、システム稼動後KeyNodeを増やす場合、<br>
     * 増やしたルールを先頭にして古いルールを後ろにカンマ区切りで連結する<br>
     * MainNodeとSubNodeの情報を返却値の配列内で逆転させて返すことが可能である.<br>
     *
     * @param key キー値
     * @param reverse 逆転指定
     * @return String 対象キーノードの情報(サーバ名、ポート番号)
     */
    private static String[] dispatchModKeyNode(String key, boolean reverse) {
        return dispatchModKeyNode(key, reverse, ruleInt);
    }

    /**
     * Rule値に従って、キー値を渡すことで、KeyNodeの名前とポートの配列を返す.<br>
     * Modアルゴリズム.<br>
     * ルールはKeyNodeの台数を記述する。また、システム稼動後KeyNodeを増やす場合、<br>
     * 増やしたルールを先頭にして古いルールを後ろにカンマ区切りで連結する<br>
     * MainNodeとSubNodeの情報を返却値の配列内で逆転させて返すことが可能である.<br>
     *
     * @param key キー値
     * @param reverse 逆転指定
     * @param useRule ルール指定
     * @return String 対象キーノードの情報(サーバ名、ポート番号)
     */
    private static String[] dispatchModKeyNode(String key, boolean reverse, int useRule) {
        String[] ret = null;
        String[] tmp = decisionModKeyNode(key, useRule);

        if (reverse) {
            // SubNodeが存在する場合は逆転させる
            if (tmp.length > 6) {
                ret = new String[9];
                ret[3] = tmp[0];
                ret[4] = tmp[1];
                ret[5] = tmp[2];

                ret[0] = tmp[3];
                ret[1] = tmp[4];
                ret[2] = tmp[5];

                ret[6] = tmp[6];
                ret[7] = tmp[7];
                ret[8] = tmp[8];
            } else if (tmp.length > 3) {
                ret = new String[6];
                ret[3] = tmp[0];
                ret[4] = tmp[1];
                ret[5] = tmp[2];

                ret[0] = tmp[3];
                ret[1] = tmp[4];
                ret[2] = tmp[5];
            } else {
                ret = tmp;
            }
            
        } else {
            ret = tmp;
        }
        return ret;
    }


    /**
     * Rule値に従って、キー値を渡すことで、KeyNodeの名前とポートの配列を返す.<br>
     * Modアルゴリズム.<br>
     * スレーブノードの指定がある場合は同時に値を返す。その場合は配列のレングスが6、Thirdが存在する場合は9となる<br>
     * ノード振り分けアルゴリズムは除算のあまりより決定.<br>
     * hash値 % ノード台数 = 振り分け先.<br>
     *
     * @param key キー値
     * @param useRule ルール値
     * @return String[] 対象キーノードの情報(サーバ名、ポート番号)
     */
    private static String[] decisionModKeyNode(String key, int useRule) {
        String[] ret = null;
        boolean noWaitFlg = false;

        // ノード詳細取り出し
        String[][] allNodeDetailList = (String[][])keyNodeMap.get("list");

        // Key値からHash値作成
        int execKeyInt = key.hashCode();

        if (execKeyInt < 0) {
            execKeyInt = execKeyInt - execKeyInt - execKeyInt;
        }

        int nodeNo = execKeyInt % useRule;

        if (nodeNo == 0) {
            nodeNo = useRule;
        }

        nodeNo = nodeNo - 1;

        // サードノードの有無に合わせて配列を初期化
        if (allNodeDetailList[6][0] != null) {
            // サードノード有り
            ret = new String[9];

            ret[3] = allNodeDetailList[3][nodeNo];
            ret[4] = allNodeDetailList[4][nodeNo];
            ret[5] = allNodeDetailList[5][nodeNo];

            ret[6] = allNodeDetailList[6][nodeNo];
            ret[7] = allNodeDetailList[7][nodeNo];
            ret[8] = allNodeDetailList[8][nodeNo];
        } else if (allNodeDetailList[3][0] != null) {
            // スレーブノードの有無に合わせて配列を初期化
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
            noWaitFlg = false;
            // 停止ステータスか確認する
            if (!StatusUtil.isWaitStatus(allNodeDetailList[2][nodeNo])) noWaitFlg = true;

            if (ret.length > 3) {
                if(!StatusUtil.isWaitStatus(allNodeDetailList[5][nodeNo])) noWaitFlg = true;
            }

            if (ret.length > 6) {
                if(!StatusUtil.isWaitStatus(allNodeDetailList[8][nodeNo])) noWaitFlg = true;
            }

            if  (noWaitFlg) break;

            try {
                //System.out.println("DataDispatcher - 停止中");
                Thread.sleep(50);
            } catch (Exception e) {}
        }

        // ノードに対するアクセスを開始をマーク
        // 終了はMasterManagerHelperで行われる
        StatusUtil.addNodeUse(allNodeDetailList[2][nodeNo]);

        if (ret.length > 3) {
            StatusUtil.addNodeUse(allNodeDetailList[5][nodeNo]);
        }

        if (ret.length > 6) {
            StatusUtil.addNodeUse(allNodeDetailList[8][nodeNo]);
        }


        return ret;
    }



    /**
     * Rule値に従って、キー値を渡すことで、KeyNodeの名前とポートの配列を返す.<br>
     * ConsistentHashアルゴリズム.<br>
     * スレーブノードの指定がある場合は同時に値を返す。その場合は配列のレングスが6、Thirdが存在する場合は9となる<br>
     *
     * @param key キー値
     * @param reverse 逆転指定
     * @return String[] 対象キーノードの情報(サーバ名、ポート番号)
     */
    public static String[] dispatchConsistentHashKeyNode(String key, boolean reverse) {
        return dispatchConsistentHashKeyNode(key, reverse, false);
    }


    /**
     * Rule値に従って、キー値を渡すことで、KeyNodeの名前とポートの配列を返す.<br>
     * ConsistentHashアルゴリズム.<br>
     * スレーブノードの指定がある場合は同時に値を返す。その場合は配列のレングスが6、Thirdが存在する場合は9となる<br>
     *
     * @param key キー値
     * @param reverse 逆転指定
     * @param useOldCircle 旧サークル使用設定
     * @return String 対象キーノードの情報(サーバ名、ポート番号)
     */
    public static String[] dispatchConsistentHashKeyNode(String key, boolean reverse, boolean useOldCircle) {
        String[] ret = null;
        String[] tmp = decisionConsistentHashKeyNode(key, useOldCircle);

        if (reverse) {
            // SubNodeが存在する場合は逆転させる
            if (tmp.length > 6) {
                ret = new String[9];
                ret[3] = tmp[0];
                ret[4] = tmp[1];
                ret[5] = tmp[2];

                ret[0] = tmp[3];
                ret[1] = tmp[4];
                ret[2] = tmp[5];

                ret[6] = tmp[6];
                ret[7] = tmp[7];
                ret[8] = tmp[8];

            } else if (tmp.length > 3) {
                ret = new String[6];
                ret[3] = tmp[0];
                ret[4] = tmp[1];
                ret[5] = tmp[2];

                ret[0] = tmp[3];
                ret[1] = tmp[4];
                ret[2] = tmp[5];
            } else {
                ret = tmp;
            }
            
        } else {
            ret = tmp;
        }
        return ret;
    }




    /**
     * Rule値に従って、キー値を渡すことで、KeyNodeの名前とポートの配列を返す.<br>
     * ConsistentHashアルゴリズム.<br>
     * スレーブノードの指定がある場合は同時に値を返す。その場合は配列のレングスが6、Thirdが存在する場合は9となる<br>
     *
     * @param key キー値
     * @param useOldCircle 旧サークル使用設定
     * @return String[] 対象キーノードの情報(サーバ名、ポート番号)
     */
    public static String[] decisionConsistentHashKeyNode(String key, boolean useOldCircle) {
        String[] ret = null;
        boolean noWaitFlg = false;
        SortedMap useNodeCircle = null;
        String targetNode = null;
        String[] mainDataNodeInfo = null;
        String[] slaveDataNodeInfo = null;
        String[] thirdDataNodeInfo = null;

        // ノード詳細取り出し
        String[][] allNodeDetailList = (String[][])keyNodeMap.get("list");

        // Key値からHash値作成
        int execKeyInt = sha1Hash4Int(key);

        // ノードサークルを取り出し
        // useOldCircleフラグに合わせて旧サークルを使い分ける
        if (useOldCircle && oldCircle != null) {
            useNodeCircle = oldCircle;
        } else {
            useNodeCircle = nodeCircle;
        }

        // 該当ノード割り出し
        int hash = sha1Hash4Int(key);

        if (!useNodeCircle.containsKey(hash)) {
            SortedMap<Integer, Map> tailMap = useNodeCircle.tailMap(hash);
            if (tailMap.isEmpty()) {
                hash = ((Integer)useNodeCircle.firstKey()).intValue();
            } else {
                hash = ((Integer)tailMap.firstKey()).intValue();
            }
        }

        // 対象ノード名を取得
        targetNode = (String)useNodeCircle.get(hash);

        // 対象メインノード詳細取り出し
        mainDataNodeInfo = (String[])keyNodeMap.get(targetNode);
        // 対象スレーブノード詳細取り出し
        slaveDataNodeInfo = (String[])keyNodeMap.get(targetNode + "_sub");
        // 対象サードノード詳細取り出し
        thirdDataNodeInfo = (String[])keyNodeMap.get(targetNode + "_third");


        // サードノードの有無に合わせて配列を初期化
        if (thirdDataNodeInfo != null) {
            ret = new String[9];

            ret[3] = slaveDataNodeInfo[0];
            ret[4] = slaveDataNodeInfo[1];
            ret[5] = slaveDataNodeInfo[2];
            ret[6] = thirdDataNodeInfo[0];
            ret[7] = thirdDataNodeInfo[1];
            ret[8] = thirdDataNodeInfo[2];

        } else if (slaveDataNodeInfo != null) {
            // スレーブノードの有無に合わせて配列を初期化
            ret = new String[6];

            ret[3] = slaveDataNodeInfo[0];
            ret[4] = slaveDataNodeInfo[1];
            ret[5] = slaveDataNodeInfo[2];
        } else {
            ret = new String[3];
        }

        ret[0] = mainDataNodeInfo[0];
        ret[1] = mainDataNodeInfo[1];
        ret[2] = mainDataNodeInfo[2];


        // 該当ノードが一時使用停止の場合は使用再開されるまで停止(データ復旧時に起こりえる)
        // どちらか一方でも一時停止の場合はWait
        while(true) {
            noWaitFlg = false;
            // 停止ステータスか確認する
            if (!StatusUtil.isWaitStatus(mainDataNodeInfo[2])) noWaitFlg = true;

            if (ret.length > 3) {
                if(!StatusUtil.isWaitStatus(slaveDataNodeInfo[2])) noWaitFlg = true;
            }

            if (ret.length > 6) {
                if(!StatusUtil.isWaitStatus(thirdDataNodeInfo[2])) noWaitFlg = true;
            }

            if  (noWaitFlg) break;

            try {
                //System.out.println("DataDispatcher - 停止中");
                Thread.sleep(50);
            } catch (Exception e) {}
        }

        // ノードに対するアクセスを開始をマーク
        // 終了はMasterManagerHelperで行われる
        StatusUtil.addNodeUse(mainDataNodeInfo[2]);

        if (ret.length > 3) {
            StatusUtil.addNodeUse(slaveDataNodeInfo[2]);
        }

        if (ret.length > 6) {
            StatusUtil.addNodeUse(thirdDataNodeInfo[2]);
        }

        return ret;
    }



    /**
     * 引数のKey値が引数のルールのもと引数のmatchNoと合致するかを返す.<br>
     *
     * @param key 対象のキー値
     * @param rule 使用ルール
     * @param matchNo 検証No
     * @return boolean 結果
     */
    public static boolean isRuleMatchKey(String key ,int rule, int matchNo) {
        boolean ret = false;

        // Key値からHash値作成
        int execKeyInt = key.hashCode();

        if (execKeyInt < 0)
            execKeyInt = execKeyInt - execKeyInt - execKeyInt;

        int targetNo = execKeyInt % rule;

        if (targetNo == 0) {
            targetNo = rule;
        }

        targetNo = targetNo - 1;

        if (targetNo == matchNo) 
            ret = true;

        return ret;
    }


    /**
     * 引数のKey値が引数のレンジの範囲のデータか確認し結果を返す.<br>
     *
     * @param key 対象のキー値
     * @param rangs 範囲
     * @return boolean 結果
     */
    public static boolean isRangeData(String key ,int[][] rangs) {
        boolean ret = false;

        // 対象データ判定
        for (int rangsIdx = 0; rangsIdx < rangs.length; rangsIdx++) {

            // レンジ start < endの場合
            if (rangs[rangsIdx][0] < rangs[rangsIdx][1]) {

                if (rangs[rangsIdx][0] <= sha1Hash4Int(key) && sha1Hash4Int(key) <= rangs[rangsIdx][1]) {

                    // 移行対象データ
                    ret = true;
                    break;
                }
            } else {

                // レンジが逆転 一度サークルの最大値に達してファーストノードのまでの値
                // レンジ start > endの場合
                if (rangs[rangsIdx][0] <= sha1Hash4Int(key)) {

                    // 移行対象データ
                    ret = true;
                    break;
                } else if (rangs[rangsIdx][1] >= sha1Hash4Int(key)){

                    // 移行対象データ
                    ret = true;
                    break;
                }
            }
        }

        return ret;
    }


    /**
     * 全てのノードの情報を返す.<br>
     * その際返却値のMapには"main"と"sub"と"third"という文字列Keyで、それぞれArrayListに<br>
     * 名前とポート番号を":"で連結した状態で格納して返す.<br>
     * "sub"はスレーブノードが設定しれていない場合はなしとなる<br>
     * "third"はサードノードが設定しれていない場合はなしとなる<br>
     *※subは設定しないがthirdは設定することは出来ない
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
        ArrayList thirdNodeList = new ArrayList();

        // 内容を複製して返す
        synchronized(syncObj) {

            retMap = new HashMap(2);

            ArrayList tmpNodeList = (ArrayList)allNodeMap.get("main");

            for (int i = 0; i < tmpNodeList.size(); i++) {
                mainNodeList.add(tmpNodeList.get(i));
            }
            retMap.put("main", mainNodeList);

            // サブノード
            if (allNodeMap.containsKey("sub")) {
                tmpNodeList = (ArrayList)allNodeMap.get("sub");

                for (int i = 0; i < tmpNodeList.size(); i++) {
                    subNodeList.add(tmpNodeList.get(i));
                }
                retMap.put("sub", subNodeList);
            }

            // サードノード
            if (allNodeMap.containsKey("third")) {
                tmpNodeList = (ArrayList)allNodeMap.get("third");

                for (int i = 0; i < tmpNodeList.size(); i++) {
                    thirdNodeList.add(tmpNodeList.get(i));
                }
                retMap.put("third", thirdNodeList);
            }

        }
        return retMap;
    }


    /**
     * TransactionManagerの情報を返す.<br>
     *
     */
    public static ArrayList getTransactionManagerInfo() {
        while(!standby) {
            try {
                Thread.sleep(50);
            } catch (Exception e) {
            }
        }

        ArrayList retList = null;

        // 内容を複製して返す
        synchronized(syncObj) {
            if (transactionManagerList != null) 
                retList = (ArrayList)transactionManagerList.clone();
        }
        return retList;
    }


    /**
     * sha1のアルゴリズムでHashした値をjavaのhashCodeして返す.<br>
     * マイナス値は返さない<br>
     *
     * @param targete
     * @param int
     */
    public static int sha1Hash4Int(String target) {
        int ret = new String(DigestUtils.sha(target.getBytes())).hashCode();
        if (ret < 0) {
            ret = ret - ret - ret;
        }
        return ret;
    }


    /**
     * 本メソッド呼び出すと本クラスを使用できるまで呼び出し元をロック停止させる.<br>
     *
     * @return boolean 
     */
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