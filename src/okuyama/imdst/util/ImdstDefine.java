package okuyama.imdst.util;

import java.util.zip.Deflater;

/**
 * 定数をまとめる.<br>
 *
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class ImdstDefine {

    public static final String okuyamaVersion = "VERSION okuyama-0.9.2";

    // -- KeyMapファイルに関係する定数 -------------------------------------------------
    // KeyNodeのWorkファイルでのセパレータ
    //public static final String keyWorkFileSep = "#imdst7386#";
    public static final String keyWorkFileSep = ",";

    // KeyNodeのWorkファイルの文字コード
    public static final String keyWorkFileEncoding = "UTF-8";

    // KeyNodeのWorkKeyファイルの終点文字列
    //public static final String keyWorkFileEndPointStr = "#imdstEndWFP8574";
    public static final String keyWorkFileEndPointStr = ";";


    // --  クライアントとの転送内容に使用する定数 -------------------------------------
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
    public static final String imdstTagBatchRegisterAppendSep = ";";

    // データノードに対するKeyデータ登録時に成功した場合の返却文字列の先頭部分
    public static final String keyNodeKeyRegistSuccessStr = "1,true";

    // データノードに対するTagデータ登録時に成功した場合の返却文字列の先頭部分
    public static final String keyNodeTagRegistSuccessStr = "3,true";

    // データノードに対するKeyデータ新規登録時に成功した場合の返却文字列の先頭部分
    public static final String keyNodeKeyNewRegistSuccessStr = "6,true";

    // データノードに対するバージョンチェック登録時に成功した場合の返却文字列の先頭部分
    public static final String keyNodeUpdateVersionCheckSuccessStr = "16,true";

    // データノードに対するKeyデータ新規登録時に既に値が登録されていた場合のエラーメッセージ
    public static final String keyNodeKeyNewRegistErrMsg = "NG:Data has already been registered";

    // データノードに対するKeyデータ登録時にバージョン値が変わっている場合のエラーメッセージ
    public static final String keyNodeKeyUpdatedErrMsg = "NG:Data has already been updated";

    // データノードに対するKeyデータ削除時に成功した場合の返却文字列の先頭部分
    public static final String keyNodeKeyRemoveSuccessStr = "5,true";

    // データノードに対するKeyデータ削除時に失敗した場合の返却文字列の先頭部分
    public static final String keyNodeKeyRemoveNotFoundStr = "5,false";

    // データノードに対するKeyデータ演算時に成功した場合の返却文字列の先頭部分
    public static final String keyNodeKeyCalcSuccessStr = "13,true";

    // データノードに対するLock取得に成功した場合の返却文字列の先頭部分
    public static final String keyNodeLockingSuccessStr = "30,true";

    // データノードに対するLock開放に成功した場合の返却文字列の先頭部分
    public static final String keyNodeReleaseSuccessStr = "31,true";

    // データノードに対するLock取得状況の確認の成否の返却文字列の先頭部分
    public static final String hasKeyNodeLockSuccessStr = "32,true";

    // データノードに対するKeyに紐付くTagデータ削除時に成功した場合の返却文字列の先頭部分
    public static final String keyNodeTgaInKeyRemoveSuccessStr = "40,true";

    // データノードに対するKeyに紐付くTagデータ削除時に失敗した場合の返却文字列の先頭部分
    public static final String keyNodeTgaInKeyRemoveNotFoundStr = "40,false";


    // MasterNodeのgetMultiValueを呼び出した際のClientへの戻り値
    public static final String getMultiEndOfDataStr = "END";

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

    // memcacheモード時の命令コマンドのincr命令文字
    public static final String memcacheExecuteMethodIncr = "incr";

    // memcacheモード時の命令コマンドのdecr命令文字
    public static final String memcacheExecuteMethodDecr = "decr";

    // memcacheモード時の命令コマンドのversion命令文字
    public static final String memcacheExecuteMethodVersion = "version";

    // memcacheモード時の命令コマンドのflush_all命令文字
    public static final String memcacheExecuteMethodFlushall = "flush_all";

    // memcacheモード時の命令コマンドのget命令文字
    public static final String memcacheExecuteMethodGet = "get";

    // memcacheモード時の命令コマンドのgets命令文字
    public static final String memcacheExecuteMethodGets = "gets";

    // memcacheモード時の命令コマンドのcas命令文字
    public static final String memcacheExecuteMethodCas = "cas";


    // memcacheのSet時の32bit値をValue値に連結する場合のセパレータ
    public static final String memcacheUnique32BitSep = ":";

    // memcacheモード時の命令コマンドのset命令の成功結果文字
    public static final String memcacheMethodReturnSuccessSet = "STORED";

    // memcacheモード時の命令コマンドのサーバエラー文字列
    public static final String memcacheMethodRetrunServerError = "SERVER_ERROR";

    // memcacheモード時の命令コマンドのadd命令の登録エラー結果文字
    public static final String memcacheMethodReturnErrorAdd = "NOT_STORED";

    // memcacheモード時の命令コマンドのcas命令の登録エラー結果文字
    public static final String memcacheMethodReturnErrorCas = "EXISTS";

    // memcacheモード時の命令コマンドのdelete命令の削除成功結果文字
    public static final String memcacheMethodReturnSuccessDelete = "DELETED";

    // memcacheモード時の命令コマンドのdelete命令の削除失敗(存在しない)結果文字
    public static final String memcacheMethodReturnErrorDelete = "NOT_FOUND";

    // memcacheモード時の命令コマンドの共通エラー文字列
    public static final String memcacheMethodReturnErrorComn = "ERROR";

    // Memcachedのsetコマンド変換用の部分文字列
    public static final String memcachedSetCommandPaddingStr =ImdstDefine.keyHelperClientParamSep + ImdstDefine.imdstBlankStrData + ImdstDefine.keyHelperClientParamSep + "0" + ImdstDefine.keyHelperClientParamSep; // // TransactionCode(0固定)

    // --  通信時の固定文字列系定数  --------------------------------------------------
    // クラインが接続を切断する際に通知する文字列
    public static final String imdstConnectExitRequest = "(&imdst9999&)";

    // 全てのKeyMapObjectファイルをKey=Valueの形式で接続した場合のデータ区切り文字
    public static final String imdstConnectAllDataSendDataSep = ";";


    // --  設定ファイルの固定文字列系定数  ---------------------------------------------

    public static final String Prop_KeyMapNodesRule = "KeyMapNodesRule";
    public static final String Prop_KeyMapDelayWrite = "KeyMapDelayWrite";
    public static final String Prop_KeyMapNodesInfo = "KeyMapNodesInfo";
    public static final String Prop_SubKeyMapNodesInfo = "SubKeyMapNodesInfo";
    public static final String Prop_ThirdKeyMapNodesInfo = "ThirdKeyMapNodesInfo";
    public static final String Prop_LoadBalanceMode = "LoadBalanceMode";
    public static final String Prop_BalanceRatio = "BalanceRatio";
    public static final String Prop_TransactionMode = "TransactionMode";
    public static final String Prop_TransactionManagerInfo = "TransactionManagerInfo";
    public static final String Prop_MainMasterNodeMode = "MainMasterNodeMode";
    public static final String Prop_SlaveMasterNodes = "SlaveMasterNodes";
    public static final String Prop_SystemConfigMode = "SystemConfigMode";
    public static final String Prop_MyNodeInfo = "MyNodeInfo";
    public static final String Prop_MainMasterNodeInfo = "MainMasterNodeInfo";
    public static final String Prop_AllMasterNodeInfo = "AllMasterNodeInfo";
    public static final String Prop_DistributionAlgorithm = "DistributionAlgorithm";
    public static final String Prop_DictonaryCharacters = "DictonaryCharacters";
    public static final String Prop_MasterNodeMaxConnectParallelExecution = "MasterNodeMaxConnectParallelExecution";
    public static final String Prop_MasterNodeMaxConnectParallelQueue = "MasterNodeMaxConnectParallelQueue";
    public static final String Prop_MasterNodeMaxAcceptParallelExecution = "MasterNodeMaxAcceptParallelExecution";
    public static final String Prop_MasterNodeMaxAcceptParallelQueue = "MasterNodeMaxAcceptParallelQueue";
    public static final String Prop_MasterNodeMaxWorkerParallelExecution = "MasterNodeMaxWorkerParallelExecution";
    public static final String Prop_MasterNodeMaxWorkerParallelQueue = "MasterNodeMaxWorkerParallelQueue";
    public static final String Prop_KeyNodeMaxConnectParallelExecution = "KeyNodeMaxConnectParallelExecution";
    public static final String Prop_KeyNodeMaxConnectParallelQueue = "KeyNodeMaxConnectParallelQueue";
    public static final String Prop_KeyNodeMaxAcceptParallelExecution = "KeyNodeMaxAcceptParallelExecution";
    public static final String Prop_KeyNodeMaxAcceptParallelQueue = "KeyNodeMaxAcceptParallelQueue";
    public static final String Prop_KeyNodeMaxWorkerParallelExecution = "KeyNodeMaxWorkerParallelExecution";
    public static final String Prop_KeyNodeMaxWorkerParallelQueue = "KeyNodeMaxWorkerParallelQueue";
    public static final String Prop_DataConsistencyMode = "DataConsistencyMode";
    public static final String Prop_IsolationMode = "IsolationMode";
    public static final String Prop_IsolationPrefix = "IsolationPrefix";
    public static final String Prop_ExecutionMethods = "ExecutionMethods";
    public static final String Prop_ConnectionAutoCloseTime = "connectionAutoCloseTime";
    public static final String Prop_MemoryMode = ".memoryMode";
    public static final String Prop_DataMemory = ".dataMemory";
    public static final String Prop_KeyMemory = ".keyMemory";
    public static final String Prop_KeySize = ".keySize";
    public static final String Prop_MemoryLimitSize = ".memoryLimitSize";
    public static final String Prop_VirtualStoreDirs = ".virtualStoreDirs";
    public static final String Prop_KeyStoreDirs = ".keyStoreDirs";
    public static final String Prop_DiskCacheFilePath = ".cacheFilePath";
    public static final String Prop_DataSaveTransactionFileEveryCommit = "DataSaveTransactionFileEveryCommit";
    public static final String Prop_ShareDataFileWriteDelayFlg = "ShareDataFileWriteDelayFlg";
    public static final String Prop_ShareDataFileMaxDelayCount = "ShareDataFileMaxDelayCount";
    public static final String Prop_SaveDataCompress = "SaveDataCompress";
    public static final String Prop_SaveDataCompressType = "SaveDataCompressType";
    public static final String Prop_SaveDataMemoryStoreLimitSize = "SaveDataMemoryStoreLimitSize";
    public static final String Prop_DataSaveMapType = "DataSaveMapType";
    public static final String Prop_DataSaveMapTypeSerialize = "serialize";
    public static final String Prop_SerializerClassName = "SerializerClassName";

    public static final String Prop_PacketBalancerParallelExecution = "PacketBalancerParallelExecution";
    public static final String Prop_PacketBalancerParallelQueue = "PacketBalancerParallelQueue";


    // -- ここからプログラム内固定文字列系(Mapのキーとか)  -------------------------------
    public static final String dataNodeParamKey_1 = "dataNodeNameList";
    public static final String dataNodeParamKey_2 = "dataNodePortList";
    public static final String dataNodeParamKey_3 = "dataSubNodeNameList";
    public static final String dataNodeParamKey_4 = "dataSubNodePortList";
    public static final String dataNodeParamKey_5 = "dataThirdNodeNameList";
    public static final String dataNodeParamKey_6 = "dataThirdNodePortList";
    public static final String dataNodeParamKey_7 = "keyMapNodeInfo";

    public static final String okuyamaProtocol = "okuyama";
    public static final String memcacheProtocol = "memcache";
    public static final String memcachedProtocol = "memcached";
    public static final String memcache4datanodeProtocol = "memcache_datanode";
    public static final String memcached4datanodeProtocol = "memcached_datanode";
    public static final String memcache4proxyProtocol = "memcache_proxy";
    public static final String memcached4proxyProtocol = "memcached_proxy";


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

    public static final int paramSocket       = 0;
    public static final int paramPw           = 1;
    public static final int paramBr           = 2;
    public static final int paramStart        = 3;
    public static final int paramLast         = 4;
    public static final int paramBalance      = 5;
    public static final int paramCheckCount   = 5;
    public static final int paramCheckCountMaster = 6;
    public static final int paramBis           = 7;
    public static final int paramBos           = 8;

    public static final int dispatchModeModInt = 0;
    public static final int dispatchModeConsistentHashInt = 1;

    public static final String dispatchModeMod = "mod";
    public static final String dispatchModeConsistentHash = "consistenthash";

    public static final String addNode4ConsistentHashMode = "addNode4ConsistentHashMode";

    // Value値に含まれるメタ情報の区切り文字
    public static final String valueMetaColumnSep = "-";

    // データ取得時にデータの有効期限をUpdateするかの指定
    // 起動引数で変更可能
    // TODO:未実装
    public volatile static boolean getAndExpireTimeUpdate = false;

    // GC呼び出しを行う指定
    public volatile static boolean jvmGcExecutionMode = true;


    // ---- プログラム規定数値 -------------------------------------------------------------
    // メモリ保存に利用するMapの種類指定 (true=SerializeMap(遅いが大量のデータ), false=通常のMap(速いが少量のデータ)) 
    public volatile static boolean useSerializeMap = false;
    public volatile static String serializerClassName = null;

    // 保存出来る、Key、Tag、Valueの最大長
    // Valueの最大長(base64エンコード前)
    public volatile static int saveDataMaxSize = 1572864;

    // 大きいデータ保存する場合は以下の数値の用に最も保存する回数の多いサイズに合わせると
    // レスポンスが向上す。下記の場合は512KB
    //public static final int saveDataMaxSize =524288;

    // Key,Tagの最大長(base64エンコード後)
    public volatile static int saveKeyMaxSize = 468;

    // 共通のデータファイルに書き出す最大サイズ
    public volatile static int dataFileWriteMaxSize = 12888;

    // FileBaseDataMapで1KeyファイルにどれだけのKey値を保存するかの指定
    // 少なければKeyの特定が高速になるので、1つのKey-Valueへのアクセスは高速化するが、
    // 同時アクセスが複数のKeyに発生した場合ディスク全体にかかる負荷は高くなる
    // SSDなどランダムReadが高速なディスクの場合は高めに調整すると高速になる可能性が高い
    // 起動引数の"-fbmnk"で調整可能
    public volatile static int fileBaseMapNumberOfOneFileKey = 7000;

    // TagのValueの1つ当たりの長さ(Keyの連結結果長)
    public volatile static int tagValueAppendMaxSize = 8192 * 3;


    // メモリモードで起動時にこのサイズを超えるValueはファイルに書き出され、メモリを使用しない
    public volatile static boolean bigValueFileStoreUse = false;
    public volatile static int memoryStoreLimitSize = 1024 * 128;

    public static final int stringBufferSmallSize = 128;

    public static final int stringBufferSmall_2Size = 160;

    public static final int stringBufferMiddleSize = 512;

    public static final int stringBufferLargeSize = 1024;

    public static final int stringBufferLarge_2Size = 2048;

    public static final int stringBufferLarge_3Size = 8192;

    // getMultiValueの際に一度にDataNodeに問い合わせるRequestKeyの数
    // 多きくし過ぎると一度に大量のValueがメモリを占有するので注意!!
    public volatile static int maxMultiGetRequestSize = 50;

    // Tag値の登録、取得時の最大バケット数
    public static final int tagRegisterParallelBucket = 150000000;
    // Tag値の登録、取得時の1バケット当たりの格納Tagリンク数
    public static final int tagBucketMaxLink = 500000;


    // ノードのDeadとする際のPing実行回数
    public static final int defaultDeadPingCount = 3;

    // クライアントのコネクションオープンタイムアウト時間(ミリ秒)
    public static final int clientConnectionOpenTimeout = 10000;

    // クライアントのコネクションタイムアウト時間(ミリ秒)
    public static final int clientConnectionTimeout = 60000 * 2;


    // Node間のコネクションオープンタイムアウト時間(ミリ秒)
    public static final int nodeConnectionOpenShortTimeout = 1000;

    public static final int nodeConnectiontReadShortTimeout = 1500;


    // Node間のコネクションオープンタイムアウト時間(ミリ秒)
    public volatile static int nodeConnectionOpenTimeout = 5000;

    // Node間のコネクションReadタイムアウト時間(ミリ秒)
    public volatile static int nodeConnectionTimeout = 10000;


    public static final int nodeConnectionTimeout4RecoverMode = 60000 * 5;

    // Node間のコネクションオープンタイムアウト時間(PING)(ミリ秒)
    public static final int nodeConnectionOpenPingTimeout = 3000;

    // Node間のコネクションReadタイムアウト時間(PING)(ミリ秒)
    public static final int nodeConnectionPingTimeout = 4000;


   // Recoverのコネクションタイムアウト時間(ミリ秒)
    public static final int recoverConnectionTimeout = 60000 * 360;


    // MasterNodeとの無操作コネクションタイムアウト時間(ミリ秒)
    public volatile static int masterNodeMaxConnectTime = 60000 * 60 * 12;

 
    // MasterNode -> DataNode間の通信が失敗した際に強制的に再接続を行う設定
    public volatile static boolean compulsionRetryConnectMode = false;


    // MasterNodeとDataNode間のコネクションを強制的に切断するまでの閾値
    public volatile static int datanodeConnectorMaxUseCount = 50000;


    // MasterConfigurationManagerHelperが設定情報を確認する時間間隔
    public static final int configurationCheckCycle = 1000 * 60;


    // FileでValueを保持する際のValueキャッシの利用指定
    public volatile static boolean useValueCache = true;
    // FileでValueを保持する際のValueキャッシュサイズ(キャッシュデータ数)
    public volatile static int valueCacheMaxSize = 128;


    // Valueをメモリに保存する際に圧縮を行う指定
    public volatile static boolean saveValueCompress = true;

    // Valueをメモリに保存する際に圧縮に利用するコンプレッサーをいくつプールしておくかの設定
    public volatile static int valueCompresserPoolSize = 40;
    
    // Valueをメモリに保存する際に圧縮する個別単位サイズ
    public volatile static int valueCompresserCompressSize = 2048;

    // Valueをメモリに保存する際に圧縮する場合の圧縮レベル
    //public volatile static int valueCompresserLevel = Deflater.BEST_COMPRESSION;
    public volatile static int valueCompresserLevel = Deflater.BEST_SPEED;
    //public volatile static int valueCompresserLevel = Deflater.FILTERED;
    //public volatile static int valueCompresserLevel = Deflater.DEFAULT_COMPRESSION;
    

    // データ永続化WALログへの書き込みタイミング(true:都度, false:一定間隔)
    public volatile static boolean dataTransactionFileFlushTiming = true;

    // 共有データファイルへの書き込み遅延の指定。遅延にした場合新規データは常に共有データファイルに書き込まれるが、既存の値の書き直しや
    // 過去の削除データ領域の再利用時などは一旦メモリに書き込んで順次共有データファイルに反映されていく。
    public volatile static boolean dataFileWriteDelayFlg = false;
    // 共有データファイルへの書き込み遅延の最大数(指定した値はdataFileWriteMaxSizeの倍数分のメモリ容量が必要になる)
    public volatile static int dataFileWriteDelayMaxSize = 4000;


    // 削除済みデータが共有データファイルのどこ存在していたかを保持する最大数(ここで保持できる数だけ削除した領域が再利用される。実際保持するのはIntegerの値)
    public volatile static int numberOfDeletedDataPoint = 600000;

    // WALログ書き出し用のBufferを何回利用するか
    public volatile static int maxTransactionLogBufferUseCount = 1000000;

    // 共有データファイル書き出し用のBufferを何回利用するか
    public volatile static int maxDataFileBufferUseCount = 500000;

    // DataNodeに保存するさいの遅延指定
    public volatile static boolean delayWriteFlg = false;

    // ネットワークI/O時に処理直後に同様のクライアントからの処理を一定時間ブロックにて待ち受けるかの設定(true:待ち受ける false:待ち受けない)
    public volatile static boolean retryClientReadFlg = true;

    public volatile static String characterDecodeSetBySearch = "UTF-8";


    // 検索Indexを1箇所のDataNodeに集中させないために同じIndex値であっても分散させる係数
    public volatile static int searchIndexDistributedCount = 2;

    // ---- 分散アルゴリズム系 ---------------------------------------------------
    // 分散アルゴリズムにConsistentHashを使用した場合の仮想ノードの数
    public volatile static int consistentHashVirtualNode = 50;


    // ---- KeyMapManager系 ------------------------------------------------------
    // Key値の数とファイルの行数の差がこの数値を超えるとvacuumを行う候補となる
    // 行数と1行のデータサイズをかけると不要なデータサイズとなる
    public static final int vacuumStartLimit = 100000;

    // Key値の数とファイルの行数の差がこの数値を超えると強制的にvacuumを行う
    // 行数と1行のデータサイズをかけると不要なデータサイズとなる
    // vacuumStartLimit × (ImdstDefine.dataFileWriteMaxSize * 1.38) = 不要サイズ
    public static final int vacuumStartCompulsionLimit = 1000000;

    // Vacuum実行時に事前に以下のミリ秒の間アクセスがないと実行許可となる
    public static final int vacuumExecAfterAccessTime = 200;

    // WALログをローテーションする際のサイズ(1.8GB)
    public static final long workFileChangeNewFileSize = 1610612736;


    // DelayWriteCoreFileBaseKeyMapがディスクに書きだすデータをどれだけメモリにキューイングするかのレコード数
    public volatile static int delayWriteMaxQueueingSize = 8000;

    // ファイルシステムへの同時アクセス係数
    public volatile static int parallelDiskAccess = 49;

    // WALログのファイルシステムへのfsync係数(0=OSでの自動sync制御、1=fsync回数低、2=fsync回数中、3=fsync回数高、4=常にfsync
    public volatile static int transactionLogFsyncType = 0;

    // 完全ファイルモード時に既に登録されているKeyの更新の場合にファイル上のValueの場所を再利用するかの設定。再利用しない場合は、
    // 新規、更新ともに処理速度を向上させることが可能。正し同一のKeyを書き換え続けた場合にValueを格納している
    // データファイルが肥大し続ける。ただし、定期的にVacuum処理で回収はされる。
    // true=再利用する、false=再利用しない
    public volatile static boolean reuseDataFileValuePositionFlg = true;

    // SerializeMapのBucketサイズのJVMへのメモリ割当1MB単位への格納係数　小さな値にすればBucket数は減る
    public volatile static long serializeMapBucketSizeMemoryFactor = 400;

    // 完全ファイルモード時に既に存在するデータを再利用する設定
    public volatile static boolean recycleExsistData = true;

    // okuyamaが利用するディスクの種類 1=HDD,2=SSD
    public volatile static int useDiskType = 1;


    // 操作記録ログ(WALログ)を読み込む設定
    public volatile static boolean workFileStartingReadFlg = true;

    // 保存データサイズの合計値演算設定
    // true:計算する
    // false:計算しない
    public volatile static boolean calcSizeFlg = true;

    // 有効期限切れデータバキューム実行指定
    public volatile static boolean vacuumInvalidDataFlg = true;
    // 有効期限切れデータバキューム実行強制指定
    public volatile static boolean vacuumInvalidDataCompulsion = false;


    // 有効期限切れデータ削除チェックサイクル(単位:分)
    public static int startVaccumInvalidCount = 29;

    // 有効期限切れのデータを実際に物理削除するまでの経過時間(ミリ秒)
    public static final long invalidDataDeleteTime = 6000;

    // データファイルをOSのPageCacheにのせる要否
    public volatile static boolean pageCacheMappendFlg = false;
    // データファイルをOSのPageCacheにのせる件数
    public volatile static int pageCacheMappendSize = 100000;

    // 高速なDiskを読み出しキャッシュに利用する場合の最大キャッシュ数(ここでの定義数 × dataFileWriteMaxSize=ディスク上に作成される最大サイズ(バイト/単位))
    public volatile static int maxDiskCacheSize = 10000;


    // データファイルへのシークアクセスをSequentialになるように調整する設定
    public volatile static boolean dataFileSequentialSchedulingFlg = false;


    public volatile static boolean fileBaseMapTimeDebug = false;
}