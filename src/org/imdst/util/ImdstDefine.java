package org.imdst.util;

/**
 * 定数をまとめる.<br>
 *
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class ImdstDefine {


    /* -- KeyMapファイルに関係する定数 ----------                        */
    // KeyNodeのWorkファイルでのセパレータ
    //public static final String keyWorkFileSep = "#imdst7386#";
    public static final String keyWorkFileSep = ",";

    // KeyNodeのWorkファイルの文字コード
    public static final String keyWorkFileEncoding = "UTF-8";

    // KeyNodeのWorkKeyファイルの終点文字列
    //public static final String keyWorkFileEndPointStr = "#imdstEndWFP8574";
    public static final String keyWorkFileEndPointStr = ";";


    /* --  クライアントとの転送内容に使用する定数 ----------              */
    // クライアントとの文字コード
    public static final String keyHelperClientParamEncoding = "UTF-8";

    // クライアントからのリクエスト文字列のセパレータ
    public static final String keyHelperClientParamSep = ",";

    // クライアントからのリクエスト文字列のセパレータ
    public static final String setTimeParamSep = "!";

    // クラインととの連携文字列でのブランクを表す代行文字列
    public static final String imdstBlankStrData = "(B)";

    // Tag値の文字列の前方カッコ
    public static final String imdstTagStartStr = "{imdst_tag#9641";

    // Tag値の文字列の後方カッコ
    public static final String imdstTagEndStr = "1469#tag_imdst}";

    // Tag値でキーを登録する際にKey値を連結するのでその際のセパレータ文字列
    //public static final String imdstTagKeyAppendSep = "#imdst8417#";
    public static final String imdstTagKeyAppendSep = ":";

    // データノードに対するKeyデータ登録時に成功した場合の返却文字列の先頭部分
    public static final String keyNodeKeyRegistSuccessStr = "1,true";

    // データノードに対するTagデータ登録時に成功した場合の返却文字列の先頭部分
    public static final String keyNodeTagRegistSuccessStr = "3,true";

    // データノードに対するKeyデータ新規登録時に成功した場合の返却文字列の先頭部分
    public static final String keyNodeKeyNewRegistSuccessStr = "6,true";

    // データノードに対するKeyデータ新規登録時に既に値が登録されていた場合のエラーメッセージ
    public static final String keyNodeKeyNewRegistErrMsg = "NG:Data has already been registered";

    // データノードに対するKeyデータ削除時に成功した場合の返却文字列の先頭部分
    public static final String keyNodeKeyRemoveSuccessStr = "5,true";

    // データノードに対するKeyデータ削除時に失敗した場合の返却文字列の先頭部分
    public static final String keyNodeKeyRemoveNotFoundStr = "5,false";

    // データノードに対するLock取得に成功した場合の返却文字列の先頭部分
    public static final String keyNodeLockingSuccessStr = "30,true";

    // データノードに対するLock開放に成功した場合の返却文字列の先頭部分
    public static final String keyNodeReleaseSuccessStr = "31,true";

    // データノードに対するLock取得状況の確認の成否の返却文字列の先頭部分
    public static final String hasKeyNodeLockSuccessStr = "32,true";

    // memcacheモード時の命令コマンドの区切り文字
    public static final String memcacheExecuteMethodSep = " ";

    // memcacheモード時の命令コマンドのset命令文字
    public static final String memcacheExecuteMethodSet = "set";

    // memcacheモード時の命令コマンドのadd命令文字
    public static final String memcacheExecuteMethodAdd = "add";
    // memcacheモード時の命令コマンドのappend命令文字
    public static final String memcacheExecuteMethodAppend = "append";

    // memcacheモード時の命令コマンドのdelete命令文字
    public static final String memcacheExecuteMethodDelete = "delete";

    // memcacheモード時の命令コマンドのget命令文字
    public static final String memcacheExecuteMethodGet = "get";

    // memcacheのSet時の32bit値をValue値に連結する場合のセパレータ
    public static final String memcacheUnique32BitSep = ":";

    // memcacheモード時の命令コマンドのset命令の成功結果文字
    public static final String memcacheMethodReturnSuccessSet = "STORED";

    // memcacheモード時の命令コマンドのset命令の成功結果文字
    public static final String memcacheMethodRetrunServerError = "SERVER_ERROR";

    // memcacheモード時の命令コマンドのadd命令の登録エラー結果文字
    public static final String memcacheMethodReturnErrorAdd = "NOT_STORED";

    // memcacheモード時の命令コマンドのdelete命令の削除成功結果文字
    public static final String memcacheMethodReturnSuccessDelete = "DELETED";

    // memcacheモード時の命令コマンドのdelete命令の削除失敗(存在しない)結果文字
    public static final String memcacheMethodReturnErrorDelete = "NOT_FOUND";

    // memcacheモード時の命令コマンドの共通エラー文字列
    public static final String memcacheMethodReturnErrorComn = "ERROR";


    /* --  通信時の固定文字列系定数  ----------                           */
    // クラインが接続を切断する際に通知する文字列
    public static final String imdstConnectExitRequest = "(&imdst9999&)";

    // 全てのKeyMapObjectファイルをKey=Valueの形式で接続した場合のデータ区切り文字
    public static final String imdstConnectAllDataSendDataSep = ";";

    /* --  設定ファイルの固定文字列系定数  ----------                     */
    public static final String Prop_KeyMapNodesRule = "KeyMapNodesRule";
    public static final String Prop_KeyMapNodesInfo = "KeyMapNodesInfo";
    public static final String Prop_SubKeyMapNodesInfo = "SubKeyMapNodesInfo";
    public static final String Prop_ThirdKeyMapNodesInfo = "ThirdKeyMapNodesInfo";
    public static final String Prop_LoadBalanceMode = "LoadBalanceMode";
    public static final String Prop_TransactionMode = "TransactionMode";
    public static final String Prop_TransactionManagerInfo = "TransactionManagerInfo";
    public static final String Prop_MainMasterNodeMode = "MainMasterNodeMode";
    public static final String Prop_SlaveMasterNodes = "SlaveMasterNodes";
    public static final String Prop_SystemConfigMode = "SystemConfigMode";
    public static final String Prop_MyNodeInfo = "MyNodeInfo";
    public static final String Prop_MainMasterNodeInfo = "MainMasterNodeInfo";
    public static final String Prop_AllMasterNodeInfo = "AllMasterNodeInfo";
    public static final String Prop_DistributionAlgorithm = "DistributionAlgorithm";
    public static final String Prop_MasterNodeMaxConnectParallelExecution = "MasterNodeMaxConnectParallelExecution";
    public static final String Prop_MasterNodeMaxAcceptParallelExecution = "MasterNodeMaxAcceptParallelExecution";
    public static final String Prop_MasterNodeMaxWorkerParallelExecution = "MasterNodeMaxWorkerParallelExecution";
    public static final String Prop_KeyNodeMaxConnectParallelExecution = "KeyNodeMaxConnectParallelExecution";
    public static final String Prop_KeyNodeMaxAcceptParallelExecution = "KeyNodeMaxAcceptParallelExecution";
    public static final String Prop_KeyNodeMaxWorkerParallelExecution = "KeyNodeMaxWorkerParallelExecution";
    public static final String Prop_DataConsistencyMode = "DataConsistencyMode";


    /* -- ここからプログラム内固定文字列系(Mapのキーとか)  ----------      */
    public static final String dataNodeParamKey_1 = "dataNodeNameList";
    public static final String dataNodeParamKey_2 = "dataNodePortList";
    public static final String dataNodeParamKey_3 = "dataSubNodeNameList";
    public static final String dataNodeParamKey_4 = "dataSubNodePortList";
    public static final String dataNodeParamKey_5 = "dataThirdNodeNameList";
    public static final String dataNodeParamKey_6 = "dataThirdNodePortList";
    // TODO:変更
    public static final String dataNodeParamKey_7 = "keyMapNodeInfo";


    public static final String keyNodeSocketKey = "socket";
    public static final String keyNodeStreamWriterKey = "stream_writer";
    public static final String keyNodeStreamReaderKey = "stream_reader";
    public static final String keyNodeWriterKey = "writer";
    public static final String keyNodeReaderKey = "reader";
    public static final int keyNodeConnectionMapKey = 0;
    public static final int keyNodeConnectionMapTime = 1;

    public static final String configModeFile = "file";
    public static final String configModeNode = "node";

    public static final String ConfigSaveNodePrefix = "MasterNode-MasterConfigSettingDataNodeSaveKeyPrefixString#112344%&987$#3# _ ";

    public static final int paramSocket = 0;
    public static final int paramPw     = 1;
    public static final int paramBr     = 2;
    public static final int paramStart  = 3;
    public static final int paramLast   = 4;

    public static final int dispatchModeModInt = 0;
    public static final int dispatchModeConsistentHashInt = 1;

    public static final String dispatchModeMod = "mod";
    public static final String dispatchModeConsistentHash = "consistenthash";

    public static final String addNode4ConsistentHashMode = "addNode4ConsistentHashMode";

    public static final int consistentHashVirtualNode = 50;

    /* -- プログラム規定数値 ----------------------------------------      */
    // 保存出来る、Key、Tag、Valueの最大長
    // Valueの最大長
    public static final int saveDataMaxSize = 512;

    // 大きいデータ保存する場合は以下の数値の用に最も保存する回数の多いサイズに合わせると
    // レスポンスが向上す。下記の場合は512KB
    //public static final int saveDataMaxSize =524288;

    // Key,Tagの最大長
    public static final int saveKeyMaxSize = 486;

    // ノードのDeadとする際のPing実行回数
    public static final int defaultDeadPingCount = 2;

    // クライアントのコネクションオープンタイムアウト時間(ミリ秒)
    public static final int clientConnectionOpenTimeout = 5000;

    // クライアントのコネクションタイムアウト時間(ミリ秒)
    public static final int clientConnectionTimeout = 30000;


    // Node間のコネクションオープンタイムアウト時間(ミリ秒)
    //public static final int nodeConnectionOpenShortTimeout = 1500;
    public static final int nodeConnectionOpenShortTimeout = 5000;

    // Node間のコネクションオープンタイムアウト時間(ミリ秒)
    //public static final int nodeConnectionOpenTimeout = 2000;
    public static final int nodeConnectionOpenTimeout = 10000;

    // Node間のコネクションReadタイムアウト時間(ミリ秒)
    //public static final int nodeConnectionTimeout = 3000;
    public static final int nodeConnectionTimeout = 15000;


    // Node間のコネクションオープンタイムアウト時間(PING)(ミリ秒)
    public static final int nodeConnectionOpenPingTimeout = 5000;

    // Node間のコネクションReadタイムアウト時間(PING)(ミリ秒)
    public static final int nodeConnectionPingTimeout = 10000;


   // Recoverのコネクションタイムアウト時間(ミリ秒)
    public static final int recoverConnectionTimeout = 60000 * 60;


}