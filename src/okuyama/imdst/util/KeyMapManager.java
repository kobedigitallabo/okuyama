package okuyama.imdst.util;

import java.util.*;
import java.io.*;
import java.net.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import okuyama.base.util.ILogger;
import okuyama.base.util.LoggerFactory;
import okuyama.base.lang.BatchException;
import okuyama.imdst.util.StatusUtil;
import okuyama.imdst.util.io.*;
import okuyama.imdst.util.JavaSystemApi;

import com.sun.mail.util.BASE64DecoderStream;
import com.sun.mail.util.BASE64EncoderStream;

/**
 * DataNodeが使用するKey-Valueを管理するモジュール.<br>
 * データのファイルストア、登録ログの出力、同期化を行う.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class KeyMapManager extends Thread {

    /**
     * Logger.<br>
     */
    private static ILogger logger = LoggerFactory.createLogger(KeyMapManager.class);

    private KeyManagerValueMap keyMapObj = null;
    private int mapSize = 0;

    private static final String tagStartStr = ImdstDefine.imdstTagStartStr;
    private static final String tagEndStr   = ImdstDefine.imdstTagEndStr;
    private static final String tagKeySep   = ImdstDefine.imdstTagKeyAppendSep;

    private int putValueMaxSize = new Double(ImdstDefine.saveDataMaxSize * 1.38).intValue();

    // Recoverを強制的に停止するためのフラグ
    public boolean outputDataStopSignal = false;

    // Key系の書き込み、取得
    private Object poolKeyLock = new Object();
    private Object setKeyLock = new Object();
    private Object lockKeyLock = new Object();

    // set,remove系のシンクロオブジェクト
    private static final int parallelSize = 5000;
    private Integer[] parallelSyncObjs = new Integer[KeyMapManager.parallelSize];

    // tagsetのシンクロオブジェクト
    private static final int tagSetParallelSize = 300;
    private Integer[] tagSetParallelSyncObjs = new Integer[KeyMapManager.tagSetParallelSize];


    // Tag系の書き込み、取得
    private Object setTagLock = new Object();

    private String nodeKeyMapFilePath = null;

    private String workKeyFilePath = null;

    // Keyをファイル保存にした場合の保存ファイルディレクトリ群
    private String[] keyFileDirs = null;

    // トランザクションログ書き出し用ストリーム
    private BufferedWriter bw = null;
    private AtomicInteger tLogWriteCount = null;
    protected static int accessorTypeBw = 2;

    // 本クラスへのアクセスブロック状態
    private boolean blocking = false;
    // 本クラスの初期化状態
    private boolean initFlg  = false;

    // Mapファイルを書き込む必要有無
    private boolean writeMapFileFlg = false;

    // 起動時にトランザクションログから復旧
    // Mapファイル本体を更新する時間間隔(ミリ秒)(時間間隔の合計 = updateInterval × intervalCount)
    private static int updateInterval = 1000;
    private static int intervalCount =  60;

    // 起動時にとトランザクションログを読み込む設定
    private static boolean workFileStartingReadFlg = ImdstDefine.workFileStartingReadFlg;

    // workMap(トランザクションログ)ファイルのデータセパレータ文字列
    private static String workFileSeq = ImdstDefine.keyWorkFileSep;

    // workMap(トランザクションログ)ファイルの文字コード
    protected static String workMapFileEnc = ImdstDefine.keyWorkFileEncoding;

    // workMap(トランザクションログ)ファイルのデータセパレータ文字列
    private static String workFileEndPoint = ImdstDefine.keyWorkFileEndPointStr;

    // workMap(トランザクションログ)ファイルをメモリーモードにするかの指定
    private boolean workFileMemory = false;

    // トランザクションログを書き出す際に使用するロック
    private Object lockWorkFileSync = new Object();

    // トランザクションログflushタイミング(true:都度, false:一定間隔)
    private boolean workFileFlushTiming = ImdstDefine.dataTransactionFileFlushTiming;
    // トランザクションログ書き込みデーモン
    private DataTransactionFileFlushDaemon dataTransactionFileFlushDaemon = null;

    // トランザクションログをローテーションする際のサイズ
    private static final long workFileChangeNewFileSize = ImdstDefine.workFileChangeNewFileSize;

    // トランザクションログをローテーションチェック頻度
    private static final int workFileChangeCheckLimit = 2;

    // データのメモリーモードかファイルモードかの指定
    private boolean dataMemory = true;

    // データのKeyおよびValueの完全ファイルモードのフラグ
    private boolean allDataForFile = false;

    // Diskモード(ファイルモード)で稼動している場合のデータファイル名
    private String diskModeRestoreFile = null;

    // データへの最終アクセス時間
    private long lastAccess = 0L;

    // データファイルのバキューム実行指定
    // 現在Vacuumの必要はないのでOff
    private boolean vacuumExec = false;

    // Key値の数とファイルの行数の差がこの数値を超えるとvacuumを行う候補となる
    // 行数と1行のデータサイズをかけると不要なデータサイズとなる
    // vacuumStartLimit × (ImdstDefine.saveDataMaxSize * 1.38) = 不要サイズ
    private int vacuumStartLimit = ImdstDefine.vacuumStartLimit;


    // Key値の数とファイルの行数の差がこの数値を超えると強制的にvacuumを行う
    // 行数と1行のデータサイズをかけると不要なデータサイズとなる
    // vacuumStartLimit × (ImdstDefine.saveDataMaxSize * 1.38) = 不要サイズ
    private int vacuumStartCompulsionLimit = ImdstDefine.vacuumStartCompulsionLimit;


    // Vacuum実行時に事前に以下のミリ秒の間アクセスがないと実行許可となる
    private int vacuumExecAfterAccessTime = ImdstDefine.vacuumExecAfterAccessTime;


    // データを管理するか、Transaction情報を管理するかを決定
    private boolean dataManege = true;

    // Lockの開始時間の連結文字列
    private String lockKeyTimeSep = "_";

    // リスト構造体の最初の要素を表す値を取得する場合のList名に付加する値
    private static String listStructStartPrefix = new String(BASE64EncoderStream.encode(ImdstDefine.imdstListStructStartStr.getBytes()));

    // リスト構造体の最後の要素を表す値を取得する場合のList名に付加する値
    private static String listStructEndPrefix = new String(BASE64EncoderStream.encode(ImdstDefine.imdstListStructEndStr.getBytes()));

    // リスト構造体の現在の最大Pointerの表す値を取得する場合のList名に付加する値
    private static String listStructPointerPrefix = new String(BASE64EncoderStream.encode(ImdstDefine.imdstListStructPointerPrefixStr.getBytes()));

    // リスト構造体の現在のサイズの表す値を取得する場合のList名に付加する値
    private static String listStructSizePrefix = new String(BASE64EncoderStream.encode(ImdstDefine.imdstListStructSizePrefixStr.getBytes()));

    // リスト構造体のList内の値を格納するKeyのPrefix
    private static String listStructDataKeyPrefix = new String(BASE64EncoderStream.encode(ImdstDefine.imdstListDataPrefixStr.getBytes()));



    // ノード復旧中のデータを一時的に蓄積する設定
    private boolean diffDataPoolingFlg = false;
    private Object diffSync = new Object();
    private List diffDataPoolingListForFileBase = null;

    // 現在のKeyMapManagerの状態を"通常(1)" or "復旧中(2)" or "復旧データ取得元(3)" or "強制リカバリが必要(4)"の3種類で管理する
    public int myOperationStatus = 1;
    
    // 現在のKeyMapManagerのdiffデータモード状態を"通常(1)" or "diffモード(2)"の2種類で管理する
    public int myDiffModeOperationStatus = 1;

    // ノード間でのデータ移動時に削除として蓄積するMap
    private ConcurrentHashMap moveAdjustmentDataMap = null;
    private Object moveAdjustmentSync = new Object();

    // 仮想ストレージモード設定
    private int memoryLimitSize = -1;
    private String[] virtualStorageDirs = null;


    // Valueのファイルを新規時に削除するかの指定
    protected boolean initDataFile = true;

    protected boolean keyObjBkupMode = false;

    protected String keyObjBkupFilePath = null;

    private Object keyObjectExportSync = new Object();

    private int keyObjectStoreTiming = 1; // 25分に一度バックアップが作成される

    private String diskCacheFile = null;


    // 初期化メソッド
    // Transactionを管理する場合に呼び出す
    public KeyMapManager(String keyMapFilePath, String workKeyMapFilePath, boolean workFileMemory, int keySize, boolean dataMemory, boolean dataManage, String diskCacheFile) throws BatchException {
        if (ImdstDefine.recoverRequired == true) {
            myOperationStatus = 4;
        }
        this.keyObjBkupMode = true;
        this.diskCacheFile = diskCacheFile;
        this.bkupObjCheck(keyMapFilePath);
        this.init(keyMapFilePath, workKeyMapFilePath, workFileMemory, keySize, dataMemory, null);
        this.dataManege = dataManage;
    }


    // 初期化メソッド
    // Key値はメモリを使用する場合に使用
    public KeyMapManager(String keyMapFilePath, String workKeyMapFilePath, boolean workFileMemory, int keySize, boolean dataMemory, String diskCacheFile) throws BatchException {
        if (ImdstDefine.recoverRequired == true) {
            myOperationStatus = 4;
        }
        this.keyObjBkupMode = true;
        this.diskCacheFile = diskCacheFile;
        this.bkupObjCheck(keyMapFilePath);
        this.init(keyMapFilePath, workKeyMapFilePath, workFileMemory, keySize, dataMemory, null);
    }

    // 初期化メソッド
    // Key値はメモリを使用する場合に使用
    public KeyMapManager(String keyMapFilePath, String workKeyMapFilePath, boolean workFileMemory, int keySize, boolean dataMemory, int memoryLimitSize, String[] virtualStorageDirs, String diskCacheFile) throws BatchException {
        if (ImdstDefine.recoverRequired == true) {
            myOperationStatus = 4;
        }

        this.keyObjBkupMode = true;
        this.diskCacheFile = diskCacheFile;
        this.bkupObjCheck(keyMapFilePath);
        this.memoryLimitSize = memoryLimitSize;
        this.virtualStorageDirs = virtualStorageDirs;
        this.init(keyMapFilePath, workKeyMapFilePath, workFileMemory, keySize, dataMemory, null);
    }


    // 初期化メソッド
    // Keyもファイルの場合
    public KeyMapManager(String keyMapFilePath, String workKeyMapFilePath, boolean workFileMemory, int keySize, boolean dataMemory, String[] dirs, String diskCacheFile) throws BatchException {
        if (ImdstDefine.recoverRequired == true) {
            myOperationStatus = 4;
        }

        boolean renewFlg = false;
        this.diskCacheFile = diskCacheFile;
        for (int idx = 0; idx < dirs.length; idx++) {
            File path = new File(dirs[idx]);
            if (!path.exists()) {
                renewFlg = true;
            }
        }
        initDataFile = renewFlg;
        if (ImdstDefine.recycleExsistData == false) initDataFile = true;
        this.init(keyMapFilePath, workKeyMapFilePath, workFileMemory, keySize, dataMemory, dirs);
    }


    private void bkupObjCheck(String keyMapFilePath) {
    
        boolean renewFlg = false;
        this.keyObjBkupFilePath = keyMapFilePath + ".obj";
        File path = new File(this.keyObjBkupFilePath);
        if (!path.exists()) renewFlg = true;
        initDataFile = renewFlg;
        if (ImdstDefine.recycleExsistData == false) initDataFile = true;
    }

    /**
     * 初期化処理
     */
    private void init(String keyMapFilePath, String workKeyMapFilePath, boolean workFileMemory, int keySize, boolean dataMemory, String[] dirs) throws BatchException {
        logger.debug("init - start");

        if (!initFlg) {
            initFlg = true;
            this.nodeKeyMapFilePath = keyMapFilePath;
            this.workFileMemory = workFileMemory;
            this.dataMemory = dataMemory;
            this.mapSize = keySize;
            this.workKeyFilePath = workKeyMapFilePath;
            // Tagの1Valu当たりのサイズを決定
            if (ImdstDefine.bigValueFileStoreUse == true && 
                    ImdstDefine.tagValueAppendMaxSize > ImdstDefine.memoryStoreLimitSize) {
                    ImdstDefine.tagValueAppendMaxSize = ImdstDefine.memoryStoreLimitSize  - ImdstDefine.saveKeyMaxSize;
            }

            FileInputStream workKeyFilefis = null;
            InputStreamReader isr = null;

            FileReader fr = null;
            BufferedReader br = null;
            String line = null;
            String[] workSplitStrs = null;

            if (dirs != null) {
                this.dataMemory = false;
                this.allDataForFile = true;
                this.keyFileDirs = dirs;
            }

            // Diskモード時のファイルパス作成
            if (!this.dataMemory) {
                this.diskModeRestoreFile = keyMapFilePath + ".data";
            }

            // set,remove系のシンクロオブジェクト初期化
            for (int i = 0; i < KeyMapManager.parallelSize; i++) {
                this.parallelSyncObjs[i] = new Integer(i);
            }

            // tagsetのシンクロオブジェクト初期化
            for (int i = 0; i < KeyMapManager.tagSetParallelSize; i++) {
                this.tagSetParallelSyncObjs[i] = new Integer(i+KeyMapManager.tagSetParallelSize);
            }


            synchronized(this.poolKeyLock) {
                try {

                    // Mapファイルを読み込む必要の有無
                    boolean mapFileRead = true;

                    // KeyManagerValueMap作成
                    if (!this.allDataForFile) {
                        this.keyMapObj = new KeyManagerValueMap(this.mapSize, this.dataMemory, this.virtualStorageDirs, this.initDataFile, new File(this.keyObjBkupFilePath), this.diskCacheFile);
                    } else {
                        this.keyMapObj = new KeyManagerValueMap(this.keyFileDirs, this.mapSize, this.initDataFile, this.diskCacheFile);
                    }


                    // Valueをファイルに保持する場合は初期化
                    // ファイルが既に存在する場合は削除する
                    if (!dataMemory) {
                        File beforeDtaFile = new File(this.diskModeRestoreFile);

                        if (this.initDataFile == true) {
                            if (beforeDtaFile.exists()) beforeDtaFile.delete();
                        }
                        this.keyMapObj.initNoMemoryModeSetting(this.diskModeRestoreFile, this.initDataFile);
                    }


                    if (this.workFileStartingReadFlg == true) {
                        System.out.println(" Data log file read start - " + new Date().toString());
                        // WorkKeyMapファイルが存在する場合は読み込み
                        // トランザクションファイルはサイズでローテーションされているので、0からのインデックス番号順に読み込む
                        for (int i = 0; true; i++) {

                            boolean endFlg = false;
                            File workKeyFile = new File(this.workKeyFilePath + i);

                            if (!workKeyFile.exists()) {

                                workKeyFile = new File(this.workKeyFilePath);
                                endFlg = true;
                            }

                            if (workKeyFile.exists()) {
                                logger.info("workKeyMapFile - Read - start");
                                workKeyFilefis = new FileInputStream(workKeyFile);
                                isr = new InputStreamReader(workKeyFilefis , KeyMapManager.workMapFileEnc);
                                br = new BufferedReader(isr);
                                int counter = 1;

                                while((line=br.readLine())!=null){
                                    if ((counter % 100) == 0) {
                                        logger.info("workKeyMapFile - Read - Count =[" + counter + "]");
                                    }

                                    if (!line.equals("")) {
                                        workSplitStrs = line.split(KeyMapManager.workFileSeq);


                                        // データは必ず5つか6つに分解できる
                                        // 復元の際はCoreStorageをバックアップイメージから復元している場合があるので、
                                        // CoreStorageのバックアップデータ作成時間より新しいログデータのみ復元対象とする。
                                        // CoreStorageがバックアップデータから復元していない場合は、作成時間が0なので確実にログから復元される
                                        if (workSplitStrs.length == 5) {

                                            if (workSplitStrs[0].equals("+")) {
                                                // 登録データ
                                                // トランザクションファイルからデータ登録操作を復元する。その際に登録実行時間もファイルから復元
                                                // 復元時間もチェック
                                                if (keyMapObj.useStorageObjectTime <  Long.parseLong(workSplitStrs[3])) 
                                                    keyMapObjPutSetTime(workSplitStrs[1], workSplitStrs[2], new Long(workSplitStrs[3]).longValue());
                                            } else if (workSplitStrs[0].equals("-")) {

                                                // 削除データ
                                                // トランザクションファイルからデータ削除操作を復元する。その際に削除実行時間もファイルから復元
                                                // 復元時間もチェック
                                                if (keyMapObj.useStorageObjectTime <  Long.parseLong(workSplitStrs[3]))
                                                    keyMapObjRemoveSetTime(workSplitStrs[1], new Long(workSplitStrs[3]).longValue());
                                            }
                                        } else if (workSplitStrs.length == 6) {

                                            if (workSplitStrs[0].equals("+")) {

                                                // 登録データ
                                                // トランザクションファイルからデータ登録操作を復元する。その際に登録実行時間もファイルから復元
                                                // 復元時間もチェック
                                                if (keyMapObj.useStorageObjectTime <  Long.parseLong(workSplitStrs[4]))
                                                    keyMapObjPutSetTime(workSplitStrs[1], workSplitStrs[2] + KeyMapManager.workFileSeq + workSplitStrs[3], new Long(workSplitStrs[4]).longValue());
                                            } else if (workSplitStrs[0].equals("-")) {

                                                // 削除データ
                                                // トランザクションファイルからデータ削除操作を復元する。その際に削除実行時間もファイルから復元
                                                // 復元時間もチェック
                                                if (keyMapObj.useStorageObjectTime <  Long.parseLong(workSplitStrs[3]))
                                                    keyMapObjRemoveSetTime(workSplitStrs[1], new Long(workSplitStrs[3]).longValue());
                                            }
                                        } else {

                                            // 不正データ
                                            logger.error("workKeyMapFile - Read - Error " + counter + "Line Data = [" + workSplitStrs + "]");
                                        }
                                    } else {
                                        logger.info("workKeyMapFile - Read - Info " + counter + "Line Blank");
                                    }
                                    counter++;
                                }

                                br.close();
                                isr.close();
                                workKeyFilefis.close();
                                logger.info("workKeyMapFile - Read - end");

                            }
                            if (endFlg) break;
                        }
                        System.out.println(" Data log file read end - " + new Date().toString());
                    }

                    // トランザクションログ用のストリーム構築
                    this.tLogWriteCount = new AtomicInteger(0);
                    
                    FileOutputStream newFos = new FileOutputStream(new File(this.workKeyFilePath), true);
                    this.bw = new CustomBufferedWriter(new OutputStreamWriter(newFos , KeyMapManager.workMapFileEnc), 8192 * 24, newFos);
                    this.bw.newLine();
                    SystemUtil.diskAccessSync(this.bw);

                    if (this.workFileMemory == false && this.workFileFlushTiming == false) {
                        this.dataTransactionFileFlushDaemon = new DataTransactionFileFlushDaemon(); 
                        this.dataTransactionFileFlushDaemon.tFilePath = this.workKeyFilePath;
                        this.dataTransactionFileFlushDaemon.tBw = this.bw;
                        this.dataTransactionFileFlushDaemon.start();
                    }

                } catch (Exception e) {

                    logger.error("KeyMapManager - init - Error" + e);
                    blocking = true;
                    StatusUtil.setStatusAndMessage(1, "KeyMapManager - init - Error [" + e.getMessage() + "]");
                    throw new BatchException(e);
                }
            }
        }
        logger.debug("init - end");
    }


    /**
     * 定期的にトランザクションログファイルのローテーション及び、Vacuum処理を行う.<br>
     * システム停止要求を監視して停止依頼があった場合は自身を終了する.<br>
     *
     */ 
    public void run (){
        int sizeCheckCounter = 0;
        int vacuumInvalidDataCount = 0;
        int keyMapObjectStoreCheckCount = 0;

        while(true) {

            if (StatusUtil.getStatus() != 0) {
                logger.info ("KeyMapManager - run - System Shutdown [1] Msg=[" + StatusUtil.getStatusMessage() + "]");
                break;
            }

            try {

                // 1サイクル1秒の停止を規定回数行う(途中で停止要求があった場合は無条件で処理実行)
                for (int count = 0; count < KeyMapManager.intervalCount; count++) {

                    // システム停止要求を監視
                    if (StatusUtil.getStatus() != 0) {
                        logger.info ("KeyMapManager - run - System Shutdown 2");
                        break;
                    }

                    if (!this.dataManege) {
                        this.autoLockRelease(JavaSystemApi.currentTimeMillis);
                    }

                    // メモリの限界値をチェック
                    // ServerManagedJobに任せる
                    /*if(this.memoryLimitSize > 0) {
                        if (JavaSystemApi.getUseMemoryPercentCache() > this.memoryLimitSize) 
                            // 限界値を超えている
                            StatusUtil.useMemoryLimitOver();
                    }*/

                    Thread.sleep(KeyMapManager.updateInterval);


                    // TransactionDaemonの死亡確認を行う
                    // 死亡している場合は、依頼された書き込みQueueからデータを引き抜いて、再度生成したDaemonに渡して実行
                    if ((count % 4) == 0 && this.workFileMemory == false && this.workFileFlushTiming == false && this.dataTransactionFileFlushDaemon.getExecuteEnd() == true) {
                        try {
                            synchronized(this.poolKeyLock) {
                                synchronized(this.lockWorkFileSync) {
                                    if (this.dataTransactionFileFlushDaemon != null && this.dataTransactionFileFlushDaemon.getExecuteEnd() == true) {

                                        DataTransactionFileFlushDaemon dataTransactionFileFlushDaemonRe = new DataTransactionFileFlushDaemon();
                                        dataTransactionFileFlushDaemon.tFilePath = this.workKeyFilePath;
                                        dataTransactionFileFlushDaemonRe.tBw = this.bw;
                                        dataTransactionFileFlushDaemonRe.setDataTransactionFileQueue(this.dataTransactionFileFlushDaemon.getDataTransactionFileQueue());
                                        dataTransactionFileFlushDaemonRe.start();
                                        this.dataTransactionFileFlushDaemon = dataTransactionFileFlushDaemonRe;
                                    }
                                }
                            }
                        } catch(Exception reE) {
                            reE.printStackTrace();
                        }
                    }
                }


                // トランザクションログのサイズをチェック
                logger.debug("Transaction Log Size Check - Start");

                if (this.workFileMemory == false) {

                    sizeCheckCounter++;

                    // 規定回数に1度チェックする
                    if (sizeCheckCounter > KeyMapManager.workFileChangeCheckLimit) {

                        // サイズを調べる対象ファイル
                        File nowWorkFile = new File(this.workKeyFilePath);

                        // サイズが規定値を超えているか
                        if (nowWorkFile.length() > KeyMapManager.workFileChangeNewFileSize) {

                            // 規定サイズを超えている
                            synchronized(this.poolKeyLock) {
                                synchronized(this.lockWorkFileSync) {

                                    logger.debug("Transaction Log File Change - Start");
                                    if (this.workFileFlushTiming == false) {

                                        // 遅延書き込み時
                                        this.dataTransactionFileFlushDaemon.close();
                                    } else {

                                        // 都度書き込み
                                        SystemUtil.diskAccessSync(this.bw);
                                        this.bw.close();
                                        this.bw = null;
                                    }


                                    int nextWorkFileName = 0;
                                    File checkWorkKeyFile = null;
                                    for (nextWorkFileName = 0; true; nextWorkFileName++) {

                                        checkWorkKeyFile = new File(this.workKeyFilePath + nextWorkFileName);
                                        if (!checkWorkKeyFile.exists()) break;
                                    }

                                    if (!nowWorkFile.renameTo(checkWorkKeyFile)) throw new Exception("Work File Name Change Error");

                                    nowWorkFile = null;
                                    checkWorkKeyFile = null;

                                    this.tLogWriteCount = new AtomicInteger(0);
                                    FileOutputStream newFos = new FileOutputStream(new File(this.workKeyFilePath), true);
                                    this.bw = new CustomBufferedWriter(new OutputStreamWriter(newFos , KeyMapManager.workMapFileEnc), 8192 * 24, newFos);
                                    this.bw.newLine();
                                    SystemUtil.diskAccessSync(this.bw);

                                    // 遅延書き込み時
                                    if (this.workFileFlushTiming == false) {
                                        this.dataTransactionFileFlushDaemon.tFilePath = this.workKeyFilePath;
                                        this.dataTransactionFileFlushDaemon.tBw = this.bw;
                                    }

                                    logger.debug("Transaction Log File Change - End");
                                }
                            }
                        }
                        sizeCheckCounter = 0;
                    }

                }
                logger.debug("Transaction Log Size Check - End");


                //  Vacuum実行の確認
                logger.info("VacuumCheck - Start");

                // データがメモリーではなくかつ、vacuum実行指定がtrueの場合
                if (!dataMemory && vacuumExec == true && diffDataPoolingFlg == false) {
                    logger.debug("vacuumCheck - Start - 1");
                    synchronized(this.poolKeyLock) {
                        logger.debug("VacuumCheck - DifferenceCount = [" + (this.keyMapObj.getAllDataCount() - this.keyMapObj.getKeySize()) + "]");
                        if ((this.keyMapObj.getAllDataCount() - this.keyMapObj.getKeySize()) > this.vacuumStartLimit) {
                            logger.debug("VacuumCheck - Start - 2");

                            // 規定時間アクセスがない
                            if ((JavaSystemApi.currentTimeMillis - this.lastAccess) > this.vacuumExecAfterAccessTime ||
                                    (this.keyMapObj.getAllDataCount() - this.keyMapObj.getKeySize()) > this.vacuumStartCompulsionLimit) {

                                logger.info("Vacuum - Start Vacuum Data Count=[" + (this.keyMapObj.getAllDataCount() - this.keyMapObj.getKeySize()) + "]");

                                long vacuumStart = JavaSystemApi.currentTimeMillis;
                                this.keyMapObj.vacuumData();
                                this.keyObjectExport(null); // Vacuum完了後、メモリ上の情報もストアする

                                long vacuumEnd = JavaSystemApi.currentTimeMillis;
                                logger.info("Vacuum - End - VacuumTime [" + (vacuumEnd - vacuumStart) +"] Milli Second");
                            }
                        }
                    }
                }
                logger.info("VacuumCheck - End");


                // 有効期限切れデータの削除
                // 実行指定(ImdstDefine.vacuumInvalidDataFlg)がtrueの場合に実行される
                // このif文に到達するのが1分に1回なので、それを規定回数繰り返すと削除処理を実行する
                // 差分データ取集中は行わない
                if ((dataMemory == true || ImdstDefine.vacuumInvalidDataCompulsion == true) && ImdstDefine.vacuumInvalidDataFlg == true && vacuumInvalidDataCount > ImdstDefine.startVaccumInvalidCount && diffDataPoolingFlg == false) {
                    logger.info("VacuumInvalidData - Start - 1");

                    synchronized(this.poolKeyLock) {
                        Set entrySet = this.keyMapObj.entrySet();
                        Iterator entryIte = entrySet.iterator(); 
                        long removeTagetData =0L;

                        long counter = 0;
                        while(entryIte.hasNext()) {

                            counter++;
                            if ((counter % 2500) == 0) {
                                logger.info("VacuumInvalidData - Exec Count[" + counter + "]");
                                Thread.sleep(100);
                            }

                            Map.Entry obj = (Map.Entry)entryIte.next();
                            if (obj == null) continue;

                            Object key = null;

                            key = obj.getKey();

                            String valStr = (String)this.getKeyPair((String)key);
                            // 削除データの可能性があるので確認(FileBaseのMapを使っている場合)
                            if (valStr == null) continue;

                            String[] valStrSplit = valStr.split(ImdstDefine.setTimeParamSep);
                            valStr = valStrSplit[0];


                            // 有効期限チェックを行う(有効期限を1分過ぎているデータが対象)
                            String[] checkValueSplit = valStr.split(ImdstDefine.keyHelperClientParamSep);

                            if (checkValueSplit.length > 1) {

                                String[] metaColumns = checkValueSplit[1].split(ImdstDefine.valueMetaColumnSep);
                                logger.info("VacuumInvalidData - checkMetaColumns - " + Arrays.toString(metaColumns));
                                if (metaColumns.length > 1 && !SystemUtil.expireCheck(metaColumns[1], ImdstDefine.invalidDataDeleteTime)) {

                                    // 無効データは削除
                                    this.removeKeyPair((String)key, "0");
                                    removeTagetData++;
                                }
                            }
                        }

                        logger.info("RemoveInvalidData - Count [" + removeTagetData + "]");
                    }
                    logger.info("VacuumInvalidData - End - 1");
                    vacuumInvalidDataCount = 0;
                }

                vacuumInvalidDataCount++;

                // メモリ上の情報をファイルにストア
                // このループへは1分に1度訪れるので、規定分に一度実行する
                keyMapObjectStoreCheckCount++;
                if (keyMapObjectStoreCheckCount > this.keyObjectStoreTiming) {
                    this.keyObjectExport(null);
                    keyMapObjectStoreCheckCount = 0;
                }
            } catch (Exception e) {

                e.printStackTrace();
                logger.error("KeyMapManager - run - Error" + e);
                blocking = true;
                StatusUtil.setStatusAndMessage(1, "KeyMapManager - run - Error [" + e.getMessage() + "]");
                if (this.workFileMemory == false && this.workFileFlushTiming == false) {

                    this.dataTransactionFileFlushDaemon.close();
                }
            }
        }
    }


    /**
     * 指定のIsolation指定のuniqueKeyに関係するデータを全て削除する.<br>
     *
     * @param uniqueKey
     * @return long 削除件数
     */
    public long truncateData(String uniqueKey) {
        long truncateTagetData =0L;
        try {

            String tagUniqueKey = tagStartStr + uniqueKey;

            logger.info("truncateData - Start");
            synchronized(this.poolKeyLock) {
                Set entrySet = this.keyMapObj.entrySet();
                Iterator entryIte = entrySet.iterator(); 

                long counter = 0;
                while(entryIte.hasNext()) {
                    counter++;
                    if ((counter % 5000) == 0) logger.info("TruncateData - Exec Count[" + counter + "]");

                    Map.Entry obj = (Map.Entry)entryIte.next();
                    if (obj == null) continue;
                    String key = null;

                    key = (String)obj.getKey();

                    if (uniqueKey.equals("#all") || (key.indexOf(uniqueKey) == 0 || key.indexOf(tagUniqueKey) == 0)) {

                        if (!StatusUtil.configDataKeyMap.containsKey(key)) {
                            // 削除対象データ
                            this.removeKeyPair((String)key, "0");
                            truncateTagetData++;
                        } else {

                        }
                    }
                }
                logger.info("TruncateData - TotalTargetCount [" + counter + "]");
                logger.info("TruncateData - TotalRemoveCount [" + truncateTagetData + "]");
            }
            logger.info("truncateData - End");
        } catch (BatchException be) {
            logger.error("truncateData - InnerError", be);
            be.printStackTrace();
        } catch (Exception e) {
            logger.error("truncateData - Error" + e);
            e.printStackTrace();
        }
        return truncateTagetData;
    }


    /**
     * キーを指定することでノードをセットする.<br>
     *
     * @param key キー値
     * @param keyNode Value値
     * @param transactionCode
     * @param boolean 移行データ指定
     */
    public void setKeyPair(String key, String keyNode, String transactionCode) throws BatchException {
        if (!blocking) {
            try {
                //logger.debug("setKeyPair - synchronized - start");
                // このsynchroの方法は正しくないきがするが。。。
                synchronized(this.parallelSyncObjs[((key.hashCode() << 1) >>> 1) % KeyMapManager.parallelSize]) {

                    if (this.moveAdjustmentDataMap != null) {
                        synchronized (this.moveAdjustmentSync) {
                            if (this.moveAdjustmentDataMap != null && this.moveAdjustmentDataMap.containsKey(key))
                                this.moveAdjustmentDataMap.remove(key);
                        }
                    }

                    String data = null;
                    int timeSepPoint = keyNode.indexOf(ImdstDefine.setTimeParamSep);
                    //String[] keyNoddes = keyNode.split(ImdstDefine.setTimeParamSep);
                    if (timeSepPoint != -1) {

                        int lastTimeSepPoint = keyNode.lastIndexOf(ImdstDefine.setTimeParamSep);
                        if (timeSepPoint == lastTimeSepPoint) {
                            data = keyNode;
                        } else {
                            String[] keyNoddes = keyNode.split(ImdstDefine.setTimeParamSep);
                            data = keyNoddes[0] + ImdstDefine.setTimeParamSep + keyNoddes[1];
                        }
                    } else {

                        data = keyNode + ImdstDefine.setTimeParamSep + "0";
                    }

                    // 登録
                    //long start1 = System.nanoTime();
                    keyMapObjPut(key, data);
                    //long end1 = System.nanoTime();

                    //long start2 = System.nanoTime();
                    // データ操作履歴ファイルに追記
                    if (this.workFileMemory == false) {

                        synchronized(this.lockWorkFileSync) {

                            if (this.workFileFlushTiming) {

                                if (data.length() < 200) { 
                                    this.bw.write(new StringBuilder(ImdstDefine.stringBufferSmall_2Size).
                                                                    append("+").
                                                                    append(KeyMapManager.workFileSeq).
                                                                    append(key).
                                                                    append(KeyMapManager.workFileSeq).
                                                                    append(data).
                                                                    append(KeyMapManager.workFileSeq).
                                                                    append(JavaSystemApi.currentTimeMillis).
                                                                    append(KeyMapManager.workFileSeq).
                                                                    append(KeyMapManager.workFileEndPoint).
                                                                    append("\n").toString());
                                } else {
                                    this.bw.write("+");
                                    this.bw.write(KeyMapManager.workFileSeq);
                                    this.bw.write(key);
                                    this.bw.write(KeyMapManager.workFileSeq);
                                    this.bw.write(data);
                                    this.bw.write(KeyMapManager.workFileSeq);
                                    this.bw.write(new Long(JavaSystemApi.currentTimeMillis).toString());
                                    this.bw.write(KeyMapManager.workFileSeq);
                                    this.bw.write(KeyMapManager.workFileEndPoint);
                                    this.bw.write("\n");
                                }
                                SystemUtil.diskAccessSync(this.bw);

                                // 現在の利用回数をチェック
                                this.checkTransactionLogWriterLimit(this.tLogWriteCount.incrementAndGet());
                            } else {
                                this.dataTransactionFileFlushDaemon.addDataTransaction(new StringBuilder(ImdstDefine.stringBufferSmall_2Size).append("+").append(KeyMapManager.workFileSeq).append(key).append(KeyMapManager.workFileSeq).append(data).append(KeyMapManager.workFileSeq).append(JavaSystemApi.currentTimeMillis).append(KeyMapManager.workFileSeq).append(KeyMapManager.workFileEndPoint).append("\n").toString());
                            }
                        }
                    }
                    //long end2 = System.nanoTime();
                    //if (((end2 - start2) + (end1 - start1)) > 1 * 1000 * 1000 * 100) {
                    //    System.out.println("1=" + (end1 - start1) + " 2=" + (end2 - start2));
                    //}
                    // Diffモードでかつsync後は再度モードを確認後、addする
                    if (this.diffDataPoolingFlg) {

                        synchronized (diffSync) {

                            if (this.diffDataPoolingFlg) {

                                this.diffDataPoolingListForFileBase.add("+" + KeyMapManager.workFileSeq + key + KeyMapManager.workFileSeq +  data);
                            }
                        }
                    }
                }

                // データの書き込みを指示
                this.writeMapFileFlg = true;
                //logger.debug("setKeyPair - synchronized - end");
            } catch (BatchException be) {

                throw be;

            } catch (Exception e) {
                e.printStackTrace();
                logger.error("setKeyPair - Error");
                blocking = true;
                StatusUtil.setStatusAndMessage(1, "setKeyPair - Error [" + e.getMessage() + "]");
                throw new BatchException(e);
            }
        }
    }


    /**
     * キーを指定することでノードをセットする.<br>
     * 既に登録されている場合は、失敗する。
     *
     * @param key キー値
     * @param keyNode Value値
     * @param transactionCode 
     */
    public boolean setKeyPairOnlyOnce(String key, String keyNode, String transactionCode) throws BatchException {
        return setKeyPairOnlyOnce(key, keyNode, transactionCode, false);
    }


    /**
     * キーを指定することでノードをセットする.<br>
     * 既に登録されている場合は、失敗する。
     *
     * @param key キー値
     * @param keyNode Value値
     * @param transactionCode 
     */
    public boolean setKeyPairOnlyOnce(String key, String keyNode, String transactionCode, boolean moveData) throws BatchException {
        boolean ret = false;

        if (!blocking) {
            try {
                // このsynchroの方法は正しくないきがするが。。。
                synchronized(this.parallelSyncObjs[((key.hashCode() << 1) >>> 1) % KeyMapManager.parallelSize]) {

                    //logger.debug("setKeyPairOnlyOnce - synchronized - start");

                    if(this.containsKeyPair(key)) {

                        String tmp = keyMapObjGet(key);

                        if(!isExpireData(tmp)) return ret;
                    }

                    if (this.moveAdjustmentDataMap != null) {
                        synchronized (this.moveAdjustmentSync) {
                            if (this.moveAdjustmentDataMap != null && this.moveAdjustmentDataMap.containsKey(key) && moveData == false)
                                this.moveAdjustmentDataMap.remove(key);
                        }
                    }


                    String data = null;

                    if (keyNode.indexOf("-1") == -1) {

                        data = keyNode;
                    } else {

                        String[] keyNoddes = keyNode.split(ImdstDefine.setTimeParamSep);
                        data = keyNoddes[0] + ImdstDefine.setTimeParamSep + "0";
                    }

                    keyMapObjPut(key, data);
                    ret = true;

                    // データ操作履歴ファイルに追記
                    if (this.workFileMemory == false) {
                        synchronized(this.lockWorkFileSync) {
                            if (this.workFileFlushTiming) {

                                this.bw.write(new StringBuilder(ImdstDefine.stringBufferSmall_2Size).append("+").append(KeyMapManager.workFileSeq).append(key).append(KeyMapManager.workFileSeq).append(data).append(KeyMapManager.workFileSeq).append(JavaSystemApi.currentTimeMillis).append(KeyMapManager.workFileSeq).append(KeyMapManager.workFileEndPoint).append("\n").toString());
                                SystemUtil.diskAccessSync(this.bw);
                                this.checkTransactionLogWriterLimit(this.tLogWriteCount.incrementAndGet());
                            } else {

                                this.dataTransactionFileFlushDaemon.addDataTransaction(new StringBuilder(ImdstDefine.stringBufferSmall_2Size).append("+").append(KeyMapManager.workFileSeq).append(key).append(KeyMapManager.workFileSeq).append(data).append(KeyMapManager.workFileSeq).append(JavaSystemApi.currentTimeMillis).append(KeyMapManager.workFileSeq).append(KeyMapManager.workFileEndPoint).append("\n").toString());
                            }
                        }
                    }

                    if (this.diffDataPoolingFlg) {
                        synchronized (diffSync) {
                            if (this.diffDataPoolingFlg) {

                                this.diffDataPoolingListForFileBase.add("+" + KeyMapManager.workFileSeq + key + KeyMapManager.workFileSeq +  data);
                            }
                        }
                    }
                }

                // データの書き込みを指示
                this.writeMapFileFlg = true;
            } catch (BatchException be) {
                throw be;
            } catch (Exception e) {
                e.printStackTrace();
                logger.error("setKeyPairOnlyOnce - Error");
                blocking = true;
                StatusUtil.setStatusAndMessage(1, "setKeyPairOnlyOnce - Error [" + e.getMessage() + "]");
                throw new BatchException(e);
            }
        }
        return ret;
    }


    /**
     * キーを指定することでノードをセットする.<br>
     * 引数のVersionNoと登録されているVersionNoが異なる場合は失敗する.<br>
     *
     * @param key キー値
     * @param keyNode Value値
     * @param transactionCode 
     * @param updateVersionNo
     */
    public boolean setKeyPairVersionCheck(String key, String keyNode, String transactionCode, String updateVersionNo, boolean execCheck) throws BatchException {
        boolean ret = false;
        if (!blocking) {
            try {
                // このsynchroの方法は正しくないきがするが。。。
                synchronized(this.parallelSyncObjs[((key.hashCode() << 1) >>> 1) % KeyMapManager.parallelSize]) {

                    //logger.debug("setKeyPairVersionCheck - synchronized - start");


                    if(execCheck == true && this.containsKeyPair(key)) {

                        String checkValue = this.getKeyPair(key);
                        if(!((String[])checkValue.split(ImdstDefine.setTimeParamSep))[1].equals(updateVersionNo)) return ret;
                    }

                    if (this.moveAdjustmentDataMap != null) {

                        synchronized (this.moveAdjustmentSync) {
                            if (this.moveAdjustmentDataMap != null && this.moveAdjustmentDataMap.containsKey(key))
                                this.moveAdjustmentDataMap.remove(key);
                        }
                    }


                    String data = null;
                    String[] keyNoddes = keyNode.split("!");
                    // VersionNoを送られてきた値+1で更新
                    data = keyNoddes[0] + "!" + ((new Long(updateVersionNo).longValue()) + 1);

                    keyMapObjPut(key, data);
                    ret = true;

                    // データ操作履歴ファイルに追記
                    if (this.workFileMemory == false) {
                        synchronized(this.lockWorkFileSync) {
                            if (this.workFileFlushTiming) {

                                this.bw.write(new StringBuilder(ImdstDefine.stringBufferSmall_2Size).append("+").append(KeyMapManager.workFileSeq).append(key).append(KeyMapManager.workFileSeq).append(data).append(KeyMapManager.workFileSeq).append(JavaSystemApi.currentTimeMillis).append(KeyMapManager.workFileSeq).append(KeyMapManager.workFileEndPoint).append("\n").toString());
                                SystemUtil.diskAccessSync(this.bw);
                                this.checkTransactionLogWriterLimit(this.tLogWriteCount.incrementAndGet());
                            } else {

                                this.dataTransactionFileFlushDaemon.addDataTransaction(new StringBuilder(ImdstDefine.stringBufferSmall_2Size).append("+").append(KeyMapManager.workFileSeq).append(key).append(KeyMapManager.workFileSeq).append(data).append(KeyMapManager.workFileSeq).append(JavaSystemApi.currentTimeMillis).append(KeyMapManager.workFileSeq).append(KeyMapManager.workFileEndPoint).append("\n").toString());
                            }
                        }
                    }

                    if (this.diffDataPoolingFlg) {
                        synchronized (diffSync) {
                            if (this.diffDataPoolingFlg) {

                                this.diffDataPoolingListForFileBase.add("+" + KeyMapManager.workFileSeq + key + KeyMapManager.workFileSeq +  data);
                            }
                        }
                    }
                }

                // データの書き込みを指示
                this.writeMapFileFlg = true;
            } catch (BatchException be) {
                throw be;
            } catch (Exception e) {
                e.printStackTrace();
                logger.error("setKeyPairVersionCheck - Error", e);
                blocking = true;
                StatusUtil.setStatusAndMessage(1, "setKeyPairVersionCheck - Error [" + e.getMessage() + "]");
                throw new BatchException(e);
            }
        }
        return ret;
    }


    // キーを指定することでノードを返す
    public String getKeyPair(String key) {
        String ret = null;
        if (!blocking) {
            try {
                ret =  (String)keyMapObjGet(key);
            } catch (BatchException be) {
                ret = null;
            }
        }

        return ret;
    }


    /**
     * List構造を作成する.<br>
     * 既に作成済みの場合は失敗となる<br>
     *
     * @param listName List名
     * @param compulsiveFlg 強制作成フラグ
     * @param transactionCode 
     * @return 処理ステータス 0=成功 1=失敗 2=既に存在する
     * @throw BatchException
     */
    public int createListStruct(String listName, boolean compulsiveFlg, String transactionCode) throws BatchException {
        int ret = 1;
        if (!blocking) {
            try {
                // このsynchroの方法は正しくないきがするが。。。
                synchronized(this.parallelSyncObjs[((listName.hashCode() << 1) >>> 1) % KeyMapManager.parallelSize]) {
                
                    String pointerKey = listStructPointerPrefix + listName;
                    String sizeKey = listStructSizePrefix + listName;
                    String key = listStructStartPrefix + listName;
                    String[] keyList = new String[2];
                    keyList[0] = key;
                    keyList[1] = listStructEndPrefix + listName;;

                    if(compulsiveFlg == false && this.containsKeyPair(pointerKey)) {
                        // 既に作成済み
                        ret = 2;
                        return ret;
                    }

                    if (this.moveAdjustmentDataMap != null) {
                        synchronized (this.moveAdjustmentSync) {
                            if (this.moveAdjustmentDataMap != null && this.moveAdjustmentDataMap.containsKey(key))
                                this.moveAdjustmentDataMap.remove(key);

                            if (this.moveAdjustmentDataMap != null && this.moveAdjustmentDataMap.containsKey(keyList[1]))
                                this.moveAdjustmentDataMap.remove(keyList[1]);

                            if (this.moveAdjustmentDataMap != null && this.moveAdjustmentDataMap.containsKey(pointerKey))
                                this.moveAdjustmentDataMap.remove(pointerKey);

                            if (this.moveAdjustmentDataMap != null && this.moveAdjustmentDataMap.containsKey(sizeKey))
                                this.moveAdjustmentDataMap.remove(sizeKey);
                        }
                    }

                    // ここから構造体登録
                    // Listの先頭と最終を表すデータ
                    String data = ImdstDefine.imdstBlankStrData + ImdstDefine.setTimeParamSep + "0";
                    for (int i = 0; i < 2; i++) {
                        keyMapObjPut(keyList[i], data);
    
                        // データ操作履歴ファイルに追記
                        if (this.workFileMemory == false) {
                            synchronized(this.lockWorkFileSync) {
                                if (this.workFileFlushTiming) {
    
                                    this.bw.write(new StringBuilder(ImdstDefine.stringBufferSmall_2Size).append("+").append(KeyMapManager.workFileSeq).append(keyList[i]).append(KeyMapManager.workFileSeq).append(data).append(KeyMapManager.workFileSeq).append(JavaSystemApi.currentTimeMillis).append(KeyMapManager.workFileSeq).append(KeyMapManager.workFileEndPoint).append("\n").toString());
                                    SystemUtil.diskAccessSync(this.bw);
                                    this.checkTransactionLogWriterLimit(this.tLogWriteCount.incrementAndGet());
                                } else {
    
                                    this.dataTransactionFileFlushDaemon.addDataTransaction(new StringBuilder(ImdstDefine.stringBufferSmall_2Size).append("+").append(KeyMapManager.workFileSeq).append(keyList[i]).append(KeyMapManager.workFileSeq).append(data).append(KeyMapManager.workFileSeq).append(JavaSystemApi.currentTimeMillis).append(KeyMapManager.workFileSeq).append(KeyMapManager.workFileEndPoint).append("\n").toString());
                                }
                            }
                        }
    
                        if (this.diffDataPoolingFlg) {
                            synchronized (diffSync) {
                                if (this.diffDataPoolingFlg) {
                                    this.diffDataPoolingListForFileBase.add("+" + KeyMapManager.workFileSeq + keyList[i] + KeyMapManager.workFileSeq +  data);
                                }
                            }
                        }
                    }

                    // 最終位置のポインター情報を格納
                    data = "0" + ImdstDefine.setTimeParamSep + "0";

                    keyMapObjPut(pointerKey, data);

                    // データ操作履歴ファイルに追記
                    if (this.workFileMemory == false) {
                        synchronized(this.lockWorkFileSync) {
                            if (this.workFileFlushTiming) {

                                this.bw.write(new StringBuilder(ImdstDefine.stringBufferSmall_2Size).append("+").append(KeyMapManager.workFileSeq).append(pointerKey).append(KeyMapManager.workFileSeq).append(data).append(KeyMapManager.workFileSeq).append(JavaSystemApi.currentTimeMillis).append(KeyMapManager.workFileSeq).append(KeyMapManager.workFileEndPoint).append("\n").toString());
                                SystemUtil.diskAccessSync(this.bw);
                                this.checkTransactionLogWriterLimit(this.tLogWriteCount.incrementAndGet());
                            } else {

                                this.dataTransactionFileFlushDaemon.addDataTransaction(new StringBuilder(ImdstDefine.stringBufferSmall_2Size).append("+").append(KeyMapManager.workFileSeq).append(pointerKey).append(KeyMapManager.workFileSeq).append(data).append(KeyMapManager.workFileSeq).append(JavaSystemApi.currentTimeMillis).append(KeyMapManager.workFileSeq).append(KeyMapManager.workFileEndPoint).append("\n").toString());
                            }
                        }
                    }

                    if (this.diffDataPoolingFlg) {
                        synchronized (diffSync) {
                            if (this.diffDataPoolingFlg) {
                                this.diffDataPoolingListForFileBase.add("+" + KeyMapManager.workFileSeq + pointerKey + KeyMapManager.workFileSeq +  data);
                            }
                        }
                    }


                    // リストサイズ情報を格納
                    data = "0" + ImdstDefine.setTimeParamSep + "0";

                    keyMapObjPut(sizeKey, data);

                    // データ操作履歴ファイルに追記
                    if (this.workFileMemory == false) {
                        synchronized(this.lockWorkFileSync) {
                            if (this.workFileFlushTiming) {

                                this.bw.write(new StringBuilder(ImdstDefine.stringBufferSmall_2Size).append("+").append(KeyMapManager.workFileSeq).append(sizeKey).append(KeyMapManager.workFileSeq).append(data).append(KeyMapManager.workFileSeq).append(JavaSystemApi.currentTimeMillis).append(KeyMapManager.workFileSeq).append(KeyMapManager.workFileEndPoint).append("\n").toString());
                                SystemUtil.diskAccessSync(this.bw);
                                this.checkTransactionLogWriterLimit(this.tLogWriteCount.incrementAndGet());
                            } else {

                                this.dataTransactionFileFlushDaemon.addDataTransaction(new StringBuilder(ImdstDefine.stringBufferSmall_2Size).append("+").append(KeyMapManager.workFileSeq).append(sizeKey).append(KeyMapManager.workFileSeq).append(data).append(KeyMapManager.workFileSeq).append(JavaSystemApi.currentTimeMillis).append(KeyMapManager.workFileSeq).append(KeyMapManager.workFileEndPoint).append("\n").toString());
                            }
                        }
                    }

                    if (this.diffDataPoolingFlg) {
                        synchronized (diffSync) {
                            if (this.diffDataPoolingFlg) {
                                this.diffDataPoolingListForFileBase.add("+" + KeyMapManager.workFileSeq + sizeKey + KeyMapManager.workFileSeq +  data);
                            }
                        }
                    }

                    ret = 0;
                }
                // データの書き込みを指示
                this.writeMapFileFlg = true;
            } catch (BatchException be) {
                throw be;
            } catch (Exception e) {
                e.printStackTrace();
                logger.error("createListStruct - Error");
                blocking = true;
                StatusUtil.setStatusAndMessage(1, "createListStruct - Error [" + e.getMessage() + "]");
                throw new BatchException(e);
            }
        }
        return ret;
    }


    /**
     * List構造の先頭にデータの登録をおこなう.<br>
     * List構造体が登録されていない場合はエラー<br>
     *
     * @param listName List名
     * @param keyNode Value値
     * @param transactionCode 
     * @return 成否 0=成功,1=構造体なし,2=それ以外の失敗
     * @throw BatchException
     */
    public int listLPush(String listName, String keyNode, String transactionCode) throws BatchException {
        int ret = 2;

        if (!blocking) {
            try {
                // このsynchroの方法は正しくないきがするが。。。
                synchronized(this.parallelSyncObjs[((listName.hashCode() << 1) >>> 1) % KeyMapManager.parallelSize]) {
                    String pointerKey = listStructPointerPrefix + listName;
                    String sizeKey = listStructSizePrefix + listName;

                    String key = listStructStartPrefix + listName;
                    String[] keyList = new String[3];
                    boolean allDataDeleted = false;

                    keyList[0] = key;
                    keyList[1] = listStructEndPrefix + listName;;
                    keyList[2] = listStructDataKeyPrefix + listName;

                    // List構造体が存在しない場合はエラー
                    if(!this.containsKeyPair(pointerKey)) {
                        ret = 1;
                        return ret;
                    }


                    // Start構造体のデータを取得/最初の要素のPointerを取得
                    // ブランクの場合過去にデータは入れられたことはあるが、全て削除済みとなる
                    String chkListStartStruct = this.getKeyPair(keyList[0]);
                    if (chkListStartStruct.indexOf(ImdstDefine.imdstBlankStrData) == 0) {
                        // 全てのデータが削除済み
                        // 初期化状態と同じ
                        allDataDeleted = true;
                    }


                    // 1度もデータ入れらたことがないか、全てのデータ削除後の初投入か確認
                    List saveDataList = new ArrayList();
                    String listPointerStruct = this.getKeyPair(pointerKey);
                    if ((listPointerStruct != null && listPointerStruct.indexOf("0") == 0) || allDataDeleted == true) {

                        // 一度も入れたことがない
                        String pointerLongStr = "1";
                        String listStartStruct = pointerLongStr + ImdstDefine.setTimeParamSep + "0"; // 先頭を表す構造体情報のValue
                        String listEndStruct = pointerLongStr + ImdstDefine.setTimeParamSep + "0"; // 最後を表す構造体情報のValue
                        keyList[2] = keyList[2] + pointerLongStr;

                        // Listの最初と最後を表す構造体を登録
                        keyMapObjPut(keyList[0], listStartStruct);
                        keyMapObjPut(keyList[1], listEndStruct);
                        String[] saveDataTmp = {keyList[0], listStartStruct};
                        saveDataList.add(saveDataTmp);
                        saveDataTmp = new String[]{keyList[1], listEndStruct};
                        saveDataList.add(saveDataTmp);

                        // Data部分を格納
                        String dataValue = keyNode + ImdstDefine.setTimeParamSep + "0";
                        keyMapObjPut(keyList[2], dataValue);
                        saveDataTmp = new String[]{keyList[2], dataValue};
                        saveDataList.add(saveDataTmp);

                        // Dataの左隣のデータをPointerを格納
                        String leftPointerKey = keyList[2]+"_L";
                        String leftPointerValue = ImdstDefine.imdstBlankStrData + ImdstDefine.setTimeParamSep + "0";
                        keyMapObjPut(leftPointerKey, leftPointerValue);
                        saveDataTmp = new String[]{leftPointerKey, leftPointerValue};
                        saveDataList.add(saveDataTmp);

                        // Dataの右隣のデータをPointerを格納
                        String rightPointerKey = keyList[2]+"_R";
                        String rightPointerValue = ImdstDefine.imdstBlankStrData + ImdstDefine.setTimeParamSep + "0";
                        keyMapObjPut(rightPointerKey, rightPointerValue);
                        saveDataTmp = new String[]{rightPointerKey, rightPointerValue};
                        saveDataList.add(saveDataTmp);

                        // 進めたpointerを格納
                        keyMapObjPut(pointerKey, listStartStruct);
                        saveDataTmp = new String[]{pointerKey, listStartStruct};
                        saveDataList.add(saveDataTmp);
                    } else {

                        // 一度はデータを入れている
                        long pointerLong = Long.parseLong(((String[])(listPointerStruct.split(ImdstDefine.setTimeParamSep)))[0]);
                        pointerLong = pointerLong + 1;

                        // 一旦現在の先頭データ構造体データを取得
                        String listStartStruct = this.getKeyPair(keyList[0]);

                        // 現在の先頭データのPointerを使って先頭データのKeyを作成
                        String nowStartDataPointer = ((String[])listStartStruct.split(ImdstDefine.setTimeParamSep))[0];
                        String nowStartDataKey = keyList[2] + nowStartDataPointer;

                        // 登録されたデータを作成
                        String newStartDataKey = keyList[2] + pointerLong;
                        String dataValue = keyNode + ImdstDefine.setTimeParamSep + "0";
                        keyMapObjPut(newStartDataKey, dataValue);
                        String[] saveDataTmp = new String[]{newStartDataKey, dataValue};
                        saveDataList.add(saveDataTmp);


                        // 登録されたデータの左隣のデータをPointerを格納
                        String leftPointerKey = newStartDataKey+"_L";
                        String leftPointerValue = ImdstDefine.imdstBlankStrData + ImdstDefine.setTimeParamSep + "0";
                        keyMapObjPut(leftPointerKey, leftPointerValue);
                        saveDataTmp = new String[]{leftPointerKey, leftPointerValue};
                        saveDataList.add(saveDataTmp);

                        // 登録されたデータの右隣のデータをPointerを格納
                        String rightPointerKey = newStartDataKey+"_R";
                        String rightPointerValue = nowStartDataPointer + ImdstDefine.setTimeParamSep + "0";

                        keyMapObjPut(rightPointerKey, rightPointerValue);
                        saveDataTmp = new String[]{rightPointerKey, rightPointerValue};
                        saveDataList.add(saveDataTmp);

                        // 先頭データの構造体を更新
                        String startDataNewStructValue = pointerLong + ImdstDefine.setTimeParamSep + "0";
                        keyMapObjPut(keyList[0], startDataNewStructValue);
                        saveDataTmp = new String[]{keyList[0], startDataNewStructValue};
                        saveDataList.add(saveDataTmp);

                        // 今までの先頭データの左隣情報を更新する
                        String beforStartDataLeftStructKey = nowStartDataKey+"_L";
                        String beforStartDataLeftStructValue = pointerLong + ImdstDefine.setTimeParamSep + "0";
                        keyMapObjPut(beforStartDataLeftStructKey, beforStartDataLeftStructValue);
                        saveDataTmp = new String[]{beforStartDataLeftStructKey, beforStartDataLeftStructValue};
                        saveDataList.add(saveDataTmp);

                        // 進めたpointerを格納
                        keyMapObjPut(pointerKey, beforStartDataLeftStructValue);
                        saveDataTmp = new String[]{pointerKey, beforStartDataLeftStructValue};
                        saveDataList.add(saveDataTmp);
                    }

                    // Listサイズをインクリメント
                    String nowSizeRet = keyMapObjGet(sizeKey);
                    if (nowSizeRet == null) throw new BatchException("List size not found error");
                    String[] nowSizeRetSplit = nowSizeRet.split(ImdstDefine.setTimeParamSep); 
                    long listSizeLong = new Long(nowSizeRetSplit[0]).longValue();
                    listSizeLong++;
                    String nextSize = listSizeLong + ImdstDefine.setTimeParamSep + "0";
                    keyMapObjPut(sizeKey, nextSize);
                    String[] saveSizeDataTmp = new String[]{sizeKey, nextSize};
                    saveDataList.add(saveSizeDataTmp);


                    // 以降操作記録ログへの書き込み処理
                    for (int idx = 0; idx < saveDataList.size(); idx++) {

                        String[] saveData = (String[])saveDataList.get(idx);
                        if (this.moveAdjustmentDataMap != null) {
                            synchronized (this.moveAdjustmentSync) {
                                if (this.moveAdjustmentDataMap != null && this.moveAdjustmentDataMap.containsKey(saveData[0]))
                                    this.moveAdjustmentDataMap.remove(saveData[0]);
                            }
                        }
    
                        // データ操作履歴ファイルに追記
                        if (this.workFileMemory == false) {
                            synchronized(this.lockWorkFileSync) {
                                if (this.workFileFlushTiming) {
    
                                    this.bw.write(new StringBuilder(ImdstDefine.stringBufferSmall_2Size).append("+").append(KeyMapManager.workFileSeq).append(saveData[0]).append(KeyMapManager.workFileSeq).append(saveData[1]).append(KeyMapManager.workFileSeq).append(JavaSystemApi.currentTimeMillis).append(KeyMapManager.workFileSeq).append(KeyMapManager.workFileEndPoint).append("\n").toString());
                                    SystemUtil.diskAccessSync(this.bw);
                                    this.checkTransactionLogWriterLimit(this.tLogWriteCount.incrementAndGet());
                                } else {
    
                                    this.dataTransactionFileFlushDaemon.addDataTransaction(new StringBuilder(ImdstDefine.stringBufferSmall_2Size).append("+").append(KeyMapManager.workFileSeq).append(saveData[0]).append(KeyMapManager.workFileSeq).append(saveData[1]).append(KeyMapManager.workFileSeq).append(JavaSystemApi.currentTimeMillis).append(KeyMapManager.workFileSeq).append(KeyMapManager.workFileEndPoint).append("\n").toString());
                                }
                            }
                        }
    
                        if (this.diffDataPoolingFlg) {
                            synchronized (diffSync) {
                                if (this.diffDataPoolingFlg) {
    
                                    this.diffDataPoolingListForFileBase.add("+" + KeyMapManager.workFileSeq + saveData[0] + KeyMapManager.workFileSeq +  saveData[1]);
                                }
                            }
                        }
                    }
                    ret = 0;
                }

                // データの書き込みを指示
                this.writeMapFileFlg = true;
            } catch (BatchException be) {
                throw be;
            } catch (Exception e) {
                e.printStackTrace();
                logger.error("listRPush - Error");
                blocking = true;
                StatusUtil.setStatusAndMessage(1, "listRPush - Error [" + e.getMessage() + "]");
                throw new BatchException(e);
            }
        }
        return ret;
    }


    /**
     * List構造の最後尾にデータの登録をおこなう.<br>
     * List構造体が登録されていない場合はエラー<br>
     *
     * @param listName List名
     * @param keyNode Value値
     * @param transactionCode 
     * @return 成否 0=成功,1=構造体なし,2=それ以外の失敗
     * @throw BatchException
     */
    public int listRPush(String listName, String keyNode, String transactionCode) throws BatchException {
        int ret = 2;

        if (!blocking) {
            try {
                // このsynchroの方法は正しくないきがするが。。。
                synchronized(this.parallelSyncObjs[((listName.hashCode() << 1) >>> 1) % KeyMapManager.parallelSize]) {
                    String pointerKey = listStructPointerPrefix + listName;
                    String sizeKey = listStructSizePrefix + listName;
                    String key = listStructStartPrefix + listName;
                    String[] keyList = new String[3];
                    boolean allDataDeleted = false;

                    keyList[0] = key;
                    keyList[1] = listStructEndPrefix + listName;;
                    keyList[2] = listStructDataKeyPrefix + listName;
                    
                    // List構造体が存在しない場合はエラー
                    if(!this.containsKeyPair(pointerKey)) {
                        ret = 1;
                        return ret;
                    }

                    // Start構造体のデータを取得/最初の要素のPointerを取得
                    // ブランクの場合過去にデータは入れられたことはあるが、全て削除済みとなる
                    String chkListStartStruct = this.getKeyPair(keyList[0]);
                    if (chkListStartStruct.indexOf(ImdstDefine.imdstBlankStrData) == 0) {
                        // 全てのデータが削除済み
                        // 初期化状態と同じ
                        allDataDeleted = true;
                    }

                    // 1度もデータ入れらたことがないか、全て削除の初投入か確認
                    List saveDataList = new ArrayList();
                    String listPointerStruct = this.getKeyPair(pointerKey);
                    if ((listPointerStruct != null && listPointerStruct.indexOf("0") == 0) || allDataDeleted == true) {

                        // 一度も入れたことがない
                        String pointerLongStr = "1";
                        String listStartStruct = pointerLongStr + ImdstDefine.setTimeParamSep + "0"; // 先頭を表す構造体情報のValue
                        String listEndStruct = pointerLongStr + ImdstDefine.setTimeParamSep + "0"; // 最後を表す構造体情報のValue
                        keyList[2] = keyList[2] + pointerLongStr;

                        // Listの最初と最後を表す構造体を登録
                        keyMapObjPut(keyList[0], listStartStruct);
                        keyMapObjPut(keyList[1], listEndStruct);
                        String[] saveDataTmp = {keyList[0], listStartStruct};
                        saveDataList.add(saveDataTmp);
                        saveDataTmp = new String[]{keyList[1], listEndStruct};
                        saveDataList.add(saveDataTmp);

                        // Data部分を格納
                        String dataValue = keyNode + ImdstDefine.setTimeParamSep + "0";
                        keyMapObjPut(keyList[2], dataValue);
                        saveDataTmp = new String[]{keyList[2], dataValue};
                        saveDataList.add(saveDataTmp);

                        // Dataの左隣のデータをPointerを格納
                        String leftPointerKey = keyList[2]+"_L";
                        String leftPointerValue = ImdstDefine.imdstBlankStrData + ImdstDefine.setTimeParamSep + "0";
                        keyMapObjPut(leftPointerKey, leftPointerValue);
                        saveDataTmp = new String[]{leftPointerKey, leftPointerValue};
                        saveDataList.add(saveDataTmp);

                        // Dataの右隣のデータをPointerを格納
                        String rightPointerKey = keyList[2]+"_R";
                        String rightPointerValue = ImdstDefine.imdstBlankStrData + ImdstDefine.setTimeParamSep + "0";
                        keyMapObjPut(rightPointerKey, rightPointerValue);
                        saveDataTmp = new String[]{rightPointerKey, rightPointerValue};
                        saveDataList.add(saveDataTmp);

                        // 進めたpointerを格納
                        keyMapObjPut(pointerKey, listStartStruct);
                        saveDataTmp = new String[]{pointerKey, listStartStruct};
                        saveDataList.add(saveDataTmp);
                    } else {

                        // 一度はデータを入れている
                        long pointerLong = Long.parseLong(((String[])(listPointerStruct.split(ImdstDefine.setTimeParamSep)))[0]);
                        pointerLong = pointerLong + 1;

                        // 一旦現在の最後尾データ構造体データを取得
                        String listEndStruct = this.getKeyPair(keyList[1]);

                        // 現在の最後尾データのPointerを使って最後尾データのKeyを作成
                        String nowEndDataPointer = ((String[])listEndStruct.split(ImdstDefine.setTimeParamSep))[0];
                        String nowEndDataKey = keyList[2] + nowEndDataPointer;

                        // 登録されたデータを作成
                        String newEndDataKey = keyList[2] + pointerLong;
                        String dataValue = keyNode + ImdstDefine.setTimeParamSep + "0";
                        keyMapObjPut(newEndDataKey, dataValue);
                        String[] saveDataTmp = new String[]{newEndDataKey, dataValue};
                        saveDataList.add(saveDataTmp);


                        // 登録されたデータの左隣のデータをPointerを格納
                        String leftPointerKey = newEndDataKey+"_L";
                        String leftPointerValue = nowEndDataPointer + ImdstDefine.setTimeParamSep + "0";
                        keyMapObjPut(leftPointerKey, leftPointerValue);
                        saveDataTmp = new String[]{leftPointerKey, leftPointerValue};
                        saveDataList.add(saveDataTmp);

                        // 登録されたデータの右隣のデータをPointerを格納
                        String rightPointerKey = newEndDataKey+"_R";
                        String rightPointerValue = ImdstDefine.imdstBlankStrData + ImdstDefine.setTimeParamSep + "0";
                        keyMapObjPut(rightPointerKey, rightPointerValue);
                        saveDataTmp = new String[]{rightPointerKey, rightPointerValue};
                        saveDataList.add(saveDataTmp);

                        // 最後尾データの構造体を更新
                        String endDataNewStructValue = pointerLong + ImdstDefine.setTimeParamSep + "0";

                        keyMapObjPut(keyList[1], endDataNewStructValue);
                        saveDataTmp = new String[]{keyList[1], endDataNewStructValue};
                        saveDataList.add(saveDataTmp);

                        // 今までの最後尾データの右隣情報を更新する
                        String beforEndDataRightStructKey = nowEndDataKey+"_R";
                        String beforEndDataRightStructValue = pointerLong + ImdstDefine.setTimeParamSep + "0";
                        keyMapObjPut(beforEndDataRightStructKey, beforEndDataRightStructValue);
                        saveDataTmp = new String[]{beforEndDataRightStructKey, beforEndDataRightStructValue};
                        saveDataList.add(saveDataTmp);

                        // 進めたpointerを格納
                        keyMapObjPut(pointerKey, beforEndDataRightStructValue);
                        saveDataTmp = new String[]{pointerKey, beforEndDataRightStructValue};
                        saveDataList.add(saveDataTmp);
                    }

                    // Listサイズをインクリメント
                    String nowSizeRet = keyMapObjGet(sizeKey);
                    if (nowSizeRet == null) throw new BatchException("List size not found error");
                    String[] nowSizeRetSplit = nowSizeRet.split(ImdstDefine.setTimeParamSep); 
                    long listSizeLong = new Long(nowSizeRetSplit[0]).longValue();
                    listSizeLong++;
                    String nextSize = listSizeLong + ImdstDefine.setTimeParamSep + "0";

                    keyMapObjPut(sizeKey, nextSize);
                    String[] saveSizeDataTmp = new String[]{sizeKey, nextSize};
                    saveDataList.add(saveSizeDataTmp);


                    // 以降操作記録ログへの書き込み処理
                    for (int idx = 0; idx < saveDataList.size(); idx++) {

                        String[] saveData = (String[])saveDataList.get(idx);
                        if (this.moveAdjustmentDataMap != null) {
                            synchronized (this.moveAdjustmentSync) {
                                if (this.moveAdjustmentDataMap != null && this.moveAdjustmentDataMap.containsKey(saveData[0]))
                                    this.moveAdjustmentDataMap.remove(saveData[0]);
                            }
                        }
    
                        // データ操作履歴ファイルに追記
                        if (this.workFileMemory == false) {
                            synchronized(this.lockWorkFileSync) {
                                if (this.workFileFlushTiming) {
    
                                    this.bw.write(new StringBuilder(ImdstDefine.stringBufferSmall_2Size).append("+").append(KeyMapManager.workFileSeq).append(saveData[0]).append(KeyMapManager.workFileSeq).append(saveData[1]).append(KeyMapManager.workFileSeq).append(JavaSystemApi.currentTimeMillis).append(KeyMapManager.workFileSeq).append(KeyMapManager.workFileEndPoint).append("\n").toString());
                                    SystemUtil.diskAccessSync(this.bw);
                                    this.checkTransactionLogWriterLimit(this.tLogWriteCount.incrementAndGet());
                                } else {
    
                                    this.dataTransactionFileFlushDaemon.addDataTransaction(new StringBuilder(ImdstDefine.stringBufferSmall_2Size).append("+").append(KeyMapManager.workFileSeq).append(saveData[0]).append(KeyMapManager.workFileSeq).append(saveData[1]).append(KeyMapManager.workFileSeq).append(JavaSystemApi.currentTimeMillis).append(KeyMapManager.workFileSeq).append(KeyMapManager.workFileEndPoint).append("\n").toString());
                                }
                            }
                        }
    
                        if (this.diffDataPoolingFlg) {
                            synchronized (diffSync) {
                                if (this.diffDataPoolingFlg) {
    
                                    this.diffDataPoolingListForFileBase.add("+" + KeyMapManager.workFileSeq + saveData[0] + KeyMapManager.workFileSeq +  saveData[1]);
                                }
                            }
                        }
                    }
                    ret = 0;
                }

                // データの書き込みを指示
                this.writeMapFileFlg = true;
            } catch (BatchException be) {
                throw be;
            } catch (Exception e) {
                e.printStackTrace();
                logger.error("listLPush - Error");
                blocking = true;
                StatusUtil.setStatusAndMessage(1, "listLPush - Error [" + e.getMessage() + "]");
                throw new BatchException(e);
            }
        }
        return ret;
    }


    /**
     * List構造の指定した位置のデータを取得する.<br>
     * List構造体が登録されていない場合はエラー<br>
     *
     * @param listName List名
     * @param index 指定位置
     * @param leftPointer 指定位置データの左辺のポインター<br>null指定可能
     * @param transactionCode 
     * @return 指定要素<br>存在しない場合はnull
     * @throw BatchException
     */
    public String listGetData(String listName, long index, String leftPointer, String transactionCode) throws BatchException {
        String ret = null;

        if (!blocking) {
            try {

                String pointerKey = listStructPointerPrefix + listName;
                // List構造体が存在しない場合はエラー
                if(!this.containsKeyPair(pointerKey)) {
                    return ret;
                }

                String listDataKey = null;
                if (leftPointer == null) {

                    // 取得データの左辺のPointerが指定されていない場合はループして取得データのKeyを求める
                    String listStartStruct = this.getKeyPair(listStructStartPrefix + listName);
                    String keyPrefix = listStructDataKeyPrefix + listName;

                    listDataKey = keyPrefix + ((String[])(listStartStruct.split(ImdstDefine.setTimeParamSep)))[0];
                    for (int createKeyIdx = 1; createKeyIdx <= index; createKeyIdx++) {

                        String rightDataPointer = this.getKeyPair(listDataKey+"_R");
                        if (rightDataPointer == null) return null;
                        listDataKey = keyPrefix + ((String[])(rightDataPointer.split(ImdstDefine.setTimeParamSep)))[0];
                    }
                } else {

                    // 取得データの左辺のPointerが指定されている場合は指定されている左辺データのポインターを使いそのデータの右辺のポインターを取得する
                    String keyPrefix = listStructDataKeyPrefix + listName;
                    String rightDataPointer = this.getKeyPair(keyPrefix + leftPointer + "_R");
                    if (rightDataPointer == null) {

                        // 指定されて左辺のデータが取得できない
                        // 先頭からループ処理し指定位置まで進める
                        String listStartStruct = this.getKeyPair(listStructStartPrefix + listName);
                        listDataKey = keyPrefix + ((String[])(listStartStruct.split(ImdstDefine.setTimeParamSep)))[0];

                        for (int createKeyIdx = 1; createKeyIdx <= index; createKeyIdx++) {
        
                            rightDataPointer = this.getKeyPair(listDataKey+"_R");
                            if (rightDataPointer == null) return null;
                            listDataKey = keyPrefix + ((String[])(rightDataPointer.split(ImdstDefine.setTimeParamSep)))[0];
                        }
                    } else {
                        listDataKey = keyPrefix + ((String[])(rightDataPointer.split(ImdstDefine.setTimeParamSep)))[0];
                    }
                }

                return this.getKeyPair(listDataKey);
            } catch (Exception e) {
                e.printStackTrace();
                logger.error("listGetData - Error");
                blocking = true;
                StatusUtil.setStatusAndMessage(1, "listGetData - Error [" + e.getMessage() + "]");
                throw new BatchException(e);
            }
        }
        return ret;
    }


    /**
     * List構造の先頭にデータの取得後、削除をおこなう.<br>
     * List構造体が登録されていない場合はエラー<br>
     *
     * @param listName List名
     * @param transactionCode 
     * @return 指定要素<br>存在しない場合はnull
     * @throw BatchException
     */
    public String listLPop(String listName, String transactionCode) throws BatchException {
        String ret = null;

        if (!blocking) {
            try {
                // このsynchroの方法は正しくないきがするが。。。
                synchronized(this.parallelSyncObjs[((listName.hashCode() << 1) >>> 1) % KeyMapManager.parallelSize]) {
                    String pointerKey = listStructPointerPrefix + listName;
                    String sizeKey = listStructSizePrefix + listName;
                    String key = listStructStartPrefix + listName;
                    String[] keyList = new String[3];
                    
                    keyList[0] = key; // Start構造体のKey
                    keyList[1] = listStructEndPrefix + listName; // End構造体のKey
                    keyList[2] = listStructDataKeyPrefix + listName; // Data構造体のKeyのベース部分

                    // List構造体が存在しない場合はエラー
                    if(!this.containsKeyPair(pointerKey)) {
                        ret = null;
                        return ret;
                    }

                    // Start構造体のデータを取得/最初の要素のPointerを取得
                    String listStartStruct = this.getKeyPair(keyList[0]);
                    if (listStartStruct.indexOf(ImdstDefine.imdstBlankStrData) == 0) {
                        // 先頭要素なし
                        // リスト初期化直後など
                        return null;
                    }

                    // 最初のPointerを使ってデータを取得
                    String nowStartDataPointer = ((String[])listStartStruct.split(ImdstDefine.setTimeParamSep))[0];
                    String nowStartDataKey = keyList[2] + nowStartDataPointer;
                    ret = this.getKeyPair(nowStartDataKey);
                    if (ret == null) {
                        return null;
                    }

                    // 最初の要素の右辺のポインターを取得
                    String nowStartDataRightPointerKey = nowStartDataKey + "_R";
                    String nowStartDataRightPointer = this.getKeyPair(nowStartDataRightPointerKey);

                    String[] saveDataTmp = null;
                    List saveDataList = new ArrayList();
                    // 右辺があるか確認
                    if (nowStartDataRightPointer != null && nowStartDataRightPointer.indexOf(ImdstDefine.imdstBlankStrData) != 0) {
                        // 右辺がある
                        // Start構造体のデータに最初のデータの右辺のポインターを入れて更新
                        keyMapObjPut(keyList[0], nowStartDataRightPointer);
                        saveDataTmp = new String[]{keyList[0], nowStartDataRightPointer};
                        saveDataList.add(saveDataTmp);

                        // 最初のデータの右辺のデータが持つ左辺のポインターを(B)に更新
                        String[] newStartDataPointer = nowStartDataRightPointer.split(ImdstDefine.setTimeParamSep);
                        String newStartDataLeftPointerKey = keyList[2] + newStartDataPointer[0] + "_L";
                        String newStartDataLeftPointerValue = ImdstDefine.imdstBlankStrData + ImdstDefine.setTimeParamSep + "0";
                        keyMapObjPut(newStartDataLeftPointerKey, newStartDataLeftPointerValue);
                        saveDataTmp = new String[]{newStartDataLeftPointerKey, newStartDataLeftPointerValue};
                        saveDataList.add(saveDataTmp);
                    } else {
                    
                        // 右辺がない
                        // Start構造体のデータに最初のデータを(B)へ更新
                        String newStartStructValue = ImdstDefine.imdstBlankStrData + ImdstDefine.setTimeParamSep + "0";
                        keyMapObjPut(keyList[0], newStartStructValue);
                        saveDataTmp = new String[]{keyList[0], newStartStructValue};
                        saveDataList.add(saveDataTmp);

                        // End構造体のデータに最初のデータを(B)へ更新
                        String newEndStructValue = ImdstDefine.imdstBlankStrData + ImdstDefine.setTimeParamSep + "0";
                        keyMapObjPut(keyList[1], newEndStructValue);
                        saveDataTmp = new String[]{keyList[1], newEndStructValue};
                        saveDataList.add(saveDataTmp);
                    }

                    // Listサイズをデクリメント
                    String nowSizeRet = keyMapObjGet(sizeKey);
                    if (nowSizeRet == null) throw new BatchException("List size not found error");
                    String[] nowSizeRetSplit = nowSizeRet.split(ImdstDefine.setTimeParamSep); 
                    long listSizeLong = new Long(nowSizeRetSplit[0]).longValue();
                    listSizeLong--;
                    String nextSize = listSizeLong + ImdstDefine.setTimeParamSep + "0";
                    keyMapObjPut(sizeKey, nextSize);
                    saveDataTmp = new String[]{sizeKey, nextSize};
                    saveDataList.add(saveDataTmp);


                    // ここまでの登録、削除を記録
                    for (int idx = 0; idx < saveDataList.size(); idx++) {

                        String[] saveData = (String[])saveDataList.get(idx);
                        if (this.moveAdjustmentDataMap != null) {
                            synchronized (this.moveAdjustmentSync) {
                                if (this.moveAdjustmentDataMap != null && this.moveAdjustmentDataMap.containsKey(saveData[0]))
                                    this.moveAdjustmentDataMap.remove(saveData[0]);
                            }
                        }
    
                        // データ操作履歴ファイルに追記
                        if (this.workFileMemory == false) {
                            synchronized(this.lockWorkFileSync) {
                                if (this.workFileFlushTiming) {
                                    this.bw.write(new StringBuilder(ImdstDefine.stringBufferSmall_2Size).append("+").append(KeyMapManager.workFileSeq).append(saveData[0]).append(KeyMapManager.workFileSeq).append(saveData[1]).append(KeyMapManager.workFileSeq).append(JavaSystemApi.currentTimeMillis).append(KeyMapManager.workFileSeq).append(KeyMapManager.workFileEndPoint).append("\n").toString());
                                    SystemUtil.diskAccessSync(this.bw);
                                    this.checkTransactionLogWriterLimit(this.tLogWriteCount.incrementAndGet());
                                } else {
                                    this.dataTransactionFileFlushDaemon.addDataTransaction(new StringBuilder(ImdstDefine.stringBufferSmall_2Size).append("+").append(KeyMapManager.workFileSeq).append(saveData[0]).append(KeyMapManager.workFileSeq).append(saveData[1]).append(KeyMapManager.workFileSeq).append(JavaSystemApi.currentTimeMillis).append(KeyMapManager.workFileSeq).append(KeyMapManager.workFileEndPoint).append("\n").toString());
                                }
                            }
                        }
    
                        if (this.diffDataPoolingFlg) {
                            synchronized (diffSync) {
                                if (this.diffDataPoolingFlg) {
                                    this.diffDataPoolingListForFileBase.add("+" + KeyMapManager.workFileSeq + saveData[0] + KeyMapManager.workFileSeq +  saveData[1]);
                                }
                            }
                        }
                    }

                    // 最初のデータのValueと左辺、右辺を削除
                    this.removeKeyPair(nowStartDataKey, transactionCode);
                    this.removeKeyPair(nowStartDataRightPointerKey, transactionCode);
                    this.removeKeyPair(nowStartDataKey + "_L", transactionCode);
                }

                // データの書き込みを指示
                this.writeMapFileFlg = true;
            } catch (BatchException be) {
                throw be;
            } catch (Exception e) {
                e.printStackTrace();
                logger.error("listLPop - Error");
                blocking = true;
                StatusUtil.setStatusAndMessage(1, "listLPop - Error [" + e.getMessage() + "]");
                throw new BatchException(e);
            }
        }
        return ret;
    }


    /**
     * List構造の後端にデータの取得後、削除をおこなう.<br>
     * List構造体が登録されていない場合はエラー<br>
     *
     * @param listName List名
     * @param transactionCode 
     * @return 指定要素<br>存在しない場合はnull
     * @throw BatchException
     */
    public String listRPop(String listName, String transactionCode) throws BatchException {
        String ret = null;

        if (!blocking) {
            try {
                // このsynchroの方法は正しくないきがするが。。。
                synchronized(this.parallelSyncObjs[((listName.hashCode() << 1) >>> 1) % KeyMapManager.parallelSize]) {
                    String pointerKey = listStructPointerPrefix + listName;
                    String sizeKey = listStructSizePrefix + listName;
                    String[] keyList = new String[3];
                    
                    String key = listStructStartPrefix + listName;
                    keyList[0] = key; // Start構造体のKey
                    keyList[1] = listStructEndPrefix + listName; // End構造体のKey
                    keyList[2] = listStructDataKeyPrefix + listName; // Data構造体のKeyのベース部分

                    // List構造体が存在しない場合はエラー
                    if(!this.containsKeyPair(pointerKey)) {
                        ret = null;
                        return ret;
                    }

                    // Start構造体のデータを取得/最初の要素のPointerを取得
                    String listStartStruct = this.getKeyPair(keyList[0]);
                    if (listStartStruct.indexOf(ImdstDefine.imdstBlankStrData) == 0) {
                        // 先頭要素なし
                        // リスト初期化直後など
                        return null;
                    }

                    // 一旦現在の最後尾データ構造体データを取得
                    String listEndStruct = this.getKeyPair(keyList[1]);

                    // 現在の最後尾データのPointerを使って最後尾データのKeyを作成
                    String nowEndDataPointer = ((String[])listEndStruct.split(ImdstDefine.setTimeParamSep))[0];
                    String nowEndDataKey = keyList[2] + nowEndDataPointer;
                    ret = this.getKeyPair(nowEndDataKey);
                    if (ret == null) {
                        return null;
                    }

                    // 最後の要素の左辺のポインターを取得
                    String nowEndDataLeftPointerKey = nowEndDataKey + "_L";
                    String nowEndDataLeftPointer = this.getKeyPair(nowEndDataLeftPointerKey);

                    String[] saveDataTmp = null;
                    List saveDataList = new ArrayList();
                    // 左辺があるか確認
                    if (nowEndDataLeftPointer != null && nowEndDataLeftPointer.indexOf(ImdstDefine.imdstBlankStrData) != 0) {
                        // 左辺がある
                        // End構造体のデータに最後のデータの左辺のポインターを入れて更新
                        keyMapObjPut(keyList[1], nowEndDataLeftPointer);
                        saveDataTmp = new String[]{keyList[1], nowEndDataLeftPointer};
                        saveDataList.add(saveDataTmp);

                        // 最後のデータの左辺のデータが持つ右辺のポインターを(B)に更新
                        String[] newEndDataPointer = nowEndDataLeftPointer.split(ImdstDefine.setTimeParamSep);
                        String newEndDataRightPointerKey = keyList[2] + newEndDataPointer[0] + "_R";
                        String newEndDataRightPointerValue = ImdstDefine.imdstBlankStrData + ImdstDefine.setTimeParamSep + "0";
                        keyMapObjPut(newEndDataRightPointerKey, newEndDataRightPointerValue);
                        saveDataTmp = new String[]{newEndDataRightPointerKey, newEndDataRightPointerValue};
                        saveDataList.add(saveDataTmp);
                    } else {
                    
                        // 左辺がない
                        // Start構造体のデータに最初のデータを(B)へ更新
                        String newStartStructValue = ImdstDefine.imdstBlankStrData + ImdstDefine.setTimeParamSep + "0";
                        keyMapObjPut(keyList[0], newStartStructValue);
                        saveDataTmp = new String[]{keyList[0], newStartStructValue};
                        saveDataList.add(saveDataTmp);

                        // End構造体のデータに最初のデータを(B)へ更新
                        String newEndStructValue = ImdstDefine.imdstBlankStrData + ImdstDefine.setTimeParamSep + "0";
                        keyMapObjPut(keyList[1], newEndStructValue);
                        saveDataTmp = new String[]{keyList[1], newEndStructValue};
                        saveDataList.add(saveDataTmp);
                    }

                    // Listサイズをデクリメント
                    String nowSizeRet = keyMapObjGet(sizeKey);
                    if (nowSizeRet == null) throw new BatchException("List size not found error");
                    String[] nowSizeRetSplit = nowSizeRet.split(ImdstDefine.setTimeParamSep); 
                    long listSizeLong = new Long(nowSizeRetSplit[0]).longValue();
                    listSizeLong--;
                    String nextSize = listSizeLong + ImdstDefine.setTimeParamSep + "0";
                    keyMapObjPut(sizeKey, nextSize);
                    saveDataTmp = new String[]{sizeKey, nextSize};
                    saveDataList.add(saveDataTmp);

                    // ここまでの登録、削除を記録
                    for (int idx = 0; idx < saveDataList.size(); idx++) {

                        String[] saveData = (String[])saveDataList.get(idx);
                        if (this.moveAdjustmentDataMap != null) {
                            synchronized (this.moveAdjustmentSync) {
                                if (this.moveAdjustmentDataMap != null && this.moveAdjustmentDataMap.containsKey(saveData[0]))
                                    this.moveAdjustmentDataMap.remove(saveData[0]);
                            }
                        }
    
                        // データ操作履歴ファイルに追記
                        if (this.workFileMemory == false) {
                            synchronized(this.lockWorkFileSync) {
                                if (this.workFileFlushTiming) {
                                    this.bw.write(new StringBuilder(ImdstDefine.stringBufferSmall_2Size).append("+").append(KeyMapManager.workFileSeq).append(saveData[0]).append(KeyMapManager.workFileSeq).append(saveData[1]).append(KeyMapManager.workFileSeq).append(JavaSystemApi.currentTimeMillis).append(KeyMapManager.workFileSeq).append(KeyMapManager.workFileEndPoint).append("\n").toString());
                                    SystemUtil.diskAccessSync(this.bw);
                                    this.checkTransactionLogWriterLimit(this.tLogWriteCount.incrementAndGet());
                                } else {
                                    this.dataTransactionFileFlushDaemon.addDataTransaction(new StringBuilder(ImdstDefine.stringBufferSmall_2Size).append("+").append(KeyMapManager.workFileSeq).append(saveData[0]).append(KeyMapManager.workFileSeq).append(saveData[1]).append(KeyMapManager.workFileSeq).append(JavaSystemApi.currentTimeMillis).append(KeyMapManager.workFileSeq).append(KeyMapManager.workFileEndPoint).append("\n").toString());
                                }
                            }
                        }
    
                        if (this.diffDataPoolingFlg) {
                            synchronized (diffSync) {
                                if (this.diffDataPoolingFlg) {
                                    this.diffDataPoolingListForFileBase.add("+" + KeyMapManager.workFileSeq + saveData[0] + KeyMapManager.workFileSeq +  saveData[1]);
                                }
                            }
                        }
                    }

                    // 最初のデータのValueと左辺、右辺を削除
                    this.removeKeyPair(nowEndDataKey, transactionCode);
                    this.removeKeyPair(nowEndDataLeftPointerKey, transactionCode);
                    this.removeKeyPair(nowEndDataKey + "_R", transactionCode);
                }

                // データの書き込みを指示
                this.writeMapFileFlg = true;
            } catch (BatchException be) {
                throw be;
            } catch (Exception e) {
                e.printStackTrace();
                logger.error("listRPop - Error");
                blocking = true;
                StatusUtil.setStatusAndMessage(1, "listRPop - Error [" + e.getMessage() + "]");
                throw new BatchException(e);
            }
        }
        return ret;
    }


    /**
     * 現在のList構造体のサイズを返す.<br>
     *
     * @param listName リスト名
     * @param transactionCode
     * @return サイズ
     * @throws BatchException Size構造体が存在しない場合
     */
    public String listLen(String listName, String transactionCode) throws BatchException {
        String ret = null;

        if (!blocking) {
            try {
                // このsynchroの方法は正しくないきがするが。。。
                synchronized(this.parallelSyncObjs[((listName.hashCode() << 1) >>> 1) % KeyMapManager.parallelSize]) {
                    String pointerKey = listStructPointerPrefix + listName;
                    String sizeKey = listStructSizePrefix + listName;

                    // List構造体が存在しない場合はエラー
                    if(!this.containsKeyPair(pointerKey)) {
                        ret = null;
                        return ret;
                    }


                    // Listサイズをデクリメント
                    String nowSizeRet = keyMapObjGet(sizeKey);
                    if (nowSizeRet == null) throw new BatchException("List size not found error");
                    String[] nowSizeRetSplit = nowSizeRet.split(ImdstDefine.setTimeParamSep); 
                    ret = nowSizeRetSplit[0];
                }
            } catch (BatchException be) {
                throw be;
            } catch (Exception e) {
                e.printStackTrace();
                logger.error("listLen - Error");
                blocking = true;
                StatusUtil.setStatusAndMessage(1, "listLen - Error [" + e.getMessage() + "]");
                throw new BatchException(e);
            }
        }
        return ret;
    }

    // Listの指定位置のデータを消す


    // キーを指定することでノードを削除する
    public String removeKeyPair(String key, String transactionCode) throws BatchException {
        String ret = null;
        if (!blocking) {
            try {
                // このsynchroの方法は正しくないきがするが。。。
                synchronized(this.parallelSyncObjs[((key.hashCode() << 1) >>> 1) % KeyMapManager.parallelSize]) {

                    ret =  (String)keyMapObjGet(key);

                    // 削除を記録
                    if (this.moveAdjustmentDataMap != null) {
                        synchronized (this.moveAdjustmentSync) {
                            if (this.moveAdjustmentDataMap != null)
                                this.moveAdjustmentDataMap.put(key, "");
                        }
                    }

                    if (ret != null) {
                        keyMapObjRemove(key);
                    } else {
                        return null;
                    }


                    // データ操作履歴ファイルに追記
                    if (this.workFileMemory == false) {
                        synchronized(this.lockWorkFileSync) {
                            // データ操作履歴ファイル再保存(登録と合わせるために4つに分割できるようにする)
                            if (this.workFileFlushTiming) {

                                this.bw.write(new StringBuilder(ImdstDefine.stringBufferSmall_2Size).append("-").append(KeyMapManager.workFileSeq).append(key).append(KeyMapManager.workFileSeq).append(" ").append(KeyMapManager.workFileSeq).append(JavaSystemApi.currentTimeMillis).append(KeyMapManager.workFileSeq).append(KeyMapManager.workFileEndPoint).append("\n").toString());
                                SystemUtil.diskAccessSync(this.bw);
                                this.checkTransactionLogWriterLimit(this.tLogWriteCount.incrementAndGet());
                            } else {

                                this.dataTransactionFileFlushDaemon.addDataTransaction(new StringBuilder(ImdstDefine.stringBufferSmall_2Size).append("-").append(KeyMapManager.workFileSeq).append(key).append(KeyMapManager.workFileSeq).append(" ").append(KeyMapManager.workFileSeq).append(JavaSystemApi.currentTimeMillis).append(KeyMapManager.workFileSeq).append(KeyMapManager.workFileEndPoint).append("\n").toString());
                            }
                        }
                    }
                    if (this.diffDataPoolingFlg) {
                        synchronized (diffSync) {
                            if (this.diffDataPoolingFlg) {

                                this.diffDataPoolingListForFileBase.add("-" + KeyMapManager.workFileSeq + key);
                            }
                        }
                    }
                }

                // データの書き込みを指示
                this.writeMapFileFlg = true;
            } catch (BatchException be) {
                throw be;
            } catch (Exception e) {
                logger.error("removeKeyPair - Error");
                blocking = true;
                StatusUtil.setStatusAndMessage(1, "removeKeyPair - Error[" + e.getMessage() + "]");
                throw new BatchException(e);
            }
        }
        return ret;
    }


    // Tagとキーを指定することでTagとキーをセットする
    public void setTagPair(String tag, String key, String transactionCode) throws BatchException {

        if (!blocking) {

            try {
                String keyStrs = null;
                int counter = 0;
                boolean appendFlg = true;
                String tagCnv = null;
                String lastTagCnv = null;
                int dataPutCounter = 0;
                int dataRegistTarget = -1;
                String dataRegistTargetTagStr = null;
                boolean firsrtRegist = true;

                // Key値をValueのように扱うため、バージョン番号(ユニーク値)が付加されているので取り外す
                key = ((String[])key.split(ImdstDefine.setTimeParamSep))[0] + ImdstDefine.setTimeParamSep +"0";
                counter = (((key.hashCode() << 1) >>> 1) % 300) * 500000;

                dataPutCounter = counter;

                // このsynchroの方法は正しくないきがするが。。。
                synchronized(this.tagSetParallelSyncObjs[((tag.hashCode() << 1) >>> 1) % KeyMapManager.tagSetParallelSize]) {

                    while (true) {

                        tagCnv = KeyMapManager.tagStartStr + tag + "_" + new Integer(counter).toString() + KeyMapManager.tagEndStr;

                        if (this.containsKeyPair(tagCnv)) {

                            firsrtRegist = false;
                            keyStrs = this.getKeyPair(tagCnv);

                            String[] workStrs = keyStrs.split(ImdstDefine.setTimeParamSep);
                            
                            keyStrs = workStrs[0];

                            // 含まれてる可能性を検証
                            String checkStr = ((String[])key.split(ImdstDefine.setTimeParamSep))[0];
                            if(keyStrs.indexOf(checkStr + KeyMapManager.tagKeySep) == 0 || 
                                   keyStrs.indexOf(KeyMapManager.tagKeySep + checkStr + KeyMapManager.tagKeySep) != -1) {
                                // 既に登録済み
                                appendFlg = false;
                            } else if (keyStrs.indexOf(checkStr) != -1) {

                                String[] tagKeysList = keyStrs.split(KeyMapManager.tagKeySep);
                                for (int tagKeysListIdx = 0; tagKeysListIdx < tagKeysList.length; tagKeysListIdx++) {

                                    if (tagKeysList[tagKeysListIdx].equals(checkStr)) {

                                        // 既に登録済み
                                        appendFlg = false;
                                        break;
                                    }
                                }
                            } else if (dataRegistTarget == -1) {

                                // データを登録する隙間があるか調べる
                                if ((keyStrs.getBytes().length + KeyMapManager.tagKeySep.getBytes().length + key.getBytes().length + 16) < ImdstDefine.tagValueAppendMaxSize) {
                                    dataRegistTarget = counter;
                                    dataRegistTargetTagStr = keyStrs;
                                }
                            }

                            // 既に登録済み
                            if (!appendFlg) break;
                        } else {

                            // Tag値のデータそのものがないもしくは、登録連番の中にはデータがない
                            if (counter > ((((key.hashCode() << 1) >>> 1) % 300) * 500000)) {

                                dataPutCounter = counter - 1;
                            } else {

                                // 該当領域にデータを登録する
                                dataPutCounter = counter;
                            }
                            break;
                        }

                        counter++;
                    }


                    // 登録の必要有無で分岐
                    if (appendFlg) {

                        // 登録
                        if (firsrtRegist) {

                            // 初登録
                            this.setKeyPair(tagCnv, key, transactionCode);
                        } else {

                            // 登録候補領域があるかどうか確認
                            if (dataRegistTarget != -1) {

                                tagCnv = KeyMapManager.tagStartStr + tag + "_" + dataRegistTarget + KeyMapManager.tagEndStr;

                                if (dataRegistTargetTagStr.equals("*")) {
                                    dataRegistTargetTagStr = key;
                                } else {
                                    dataRegistTargetTagStr = dataRegistTargetTagStr + KeyMapManager.tagKeySep + key;
                                }
                                this.setKeyPair(tagCnv, dataRegistTargetTagStr, transactionCode);

                            // 既に別のKeyが登録済みなので、そのキーにアペンドしても良いかを確認
                            // 登録時間の長さ(16)もプラス
                            } else if ((keyStrs.getBytes().length + KeyMapManager.tagKeySep.getBytes().length + key.getBytes().length + 16) >= ImdstDefine.tagValueAppendMaxSize) {

                                // 既にキー値が最大のサイズに到達しているので別のキーを生み出す
                                counter++;

                                tagCnv = KeyMapManager.tagStartStr + tag + "_" + (dataPutCounter + 1) + KeyMapManager.tagEndStr;

                                this.setKeyPair(tagCnv, key, transactionCode);
                            } else {

                                // アペンド
                                tagCnv = KeyMapManager.tagStartStr + tag + "_" + dataPutCounter + KeyMapManager.tagEndStr;

                                if (keyStrs.equals("*")) {
                                    keyStrs = key;
                                } else {
                                    keyStrs = keyStrs + KeyMapManager.tagKeySep + key;
                                }
                                this.setKeyPair(tagCnv, keyStrs, transactionCode);
                            }
                        }
                    }
                }
            } catch (BatchException be) {
                logger.error("setTagPair - InnerError", be);
                throw be;
            } catch (Exception e) {
                e.printStackTrace();
                logger.error("setTagPair - Error");
                blocking = true;
                StatusUtil.setStatusAndMessage(1, "setTagPair - Error [" + e.getMessage() + "]");
                throw new BatchException(e);
            }
        }
    }


    // Tagとキーを指定することでTagとキーをセットする
    public boolean removeTargetTagInKey(String tag, String key, String transactionCode) throws BatchException {
        boolean ret = false;

        if (!blocking) {

            try {
                String keyStrs = null;
                int counter = 0;
                boolean appendFlg = true;
                String tagCnv = null;
                String lastTagCnv = null;
                int dataPutCounter = 0;
                boolean firsrtRegist = true;

                String targetKey = key;
                key = ((String[])key.split(ImdstDefine.setTimeParamSep))[0] + ImdstDefine.setTimeParamSep +"0";
                counter = (((key.hashCode() << 1) >>> 1) % 300) * 500000;


                // このsynchroの方法は正しくないきがするが。。。
                synchronized(this.tagSetParallelSyncObjs[((tag.hashCode() << 1) >>> 1) % KeyMapManager.tagSetParallelSize]) {

                    while (true) {

                        tagCnv = KeyMapManager.tagStartStr + tag + "_" + new Integer(counter).toString() + KeyMapManager.tagEndStr;

                        if (this.containsKeyPair(tagCnv)) {

                            // データあり
                            firsrtRegist = false;
                            keyStrs = this.getKeyPair(tagCnv);

                            String[] workStrs = keyStrs.split(ImdstDefine.setTimeParamSep);
                            keyStrs = workStrs[0];

                            if (keyStrs.indexOf(targetKey) != -1) {
                                // 含まれている可能性あり
                                String[] matchKeys = keyStrs.split(":");
                                for (int matchKeysIdx = 0; matchKeysIdx < matchKeys.length; matchKeysIdx++) {
                                    if (matchKeys[matchKeysIdx].equals(targetKey)) {

                                        StringBuilder setNewTagKeysBuf = new StringBuilder();
                                        String sep = "";
                                        //  Tagを探す
                                        for (int matchKeysIdx2 = 0; matchKeysIdx2 < matchKeys.length; matchKeysIdx2++) {
                                            if (matchKeysIdx2 != matchKeysIdx) {
                                                setNewTagKeysBuf.append(sep);
                                                setNewTagKeysBuf.append(matchKeys[matchKeysIdx2]);
                                                sep = ":";
                                            }
                                        }

                                        // このTagの組に他のKeyも含まれている場合と、含まれていない場合で処理分岐
                                        if (setNewTagKeysBuf.length() > 0) {
                                            setNewTagKeysBuf.append(ImdstDefine.setTimeParamSep);
                                            setNewTagKeysBuf.append("0");
                                        } else {
                                            setNewTagKeysBuf.append("*");
                                            setNewTagKeysBuf.append(ImdstDefine.setTimeParamSep);
                                            setNewTagKeysBuf.append("0");
                                        }

                                        // 削除済みデータをset
                                        this.setKeyPair(tagCnv, setNewTagKeysBuf.toString(), transactionCode);

                                        ret = true;
                                        break;
                                    }
                                }
                                // Matchsした場合
                                if (ret == true) break;
                            }
                        } else {

                            // データなし
                            break;
                        }

                        counter++;
                    }
                }
            /*} catch (BatchException be) {
                logger.error("removeTargetTagInKey - InnerError", be);
                throw be;*/
            } catch (Exception e) {
                e.printStackTrace();
                logger.error("removeTargetTagInKey - Error");
                blocking = true;
                StatusUtil.setStatusAndMessage(12, "removeTargetTagInKey - Error [" + e.getMessage() + "]");
                throw new BatchException(e);
            }
        }
        return ret;
    }


    public String getTagPair(String tag) {
        String keyStrs = "";
        String[] setTimeSplitWork = null;

        boolean isMatch = false;
        StringBuilder tmpBuf = new StringBuilder(ImdstDefine.stringBufferLarge_2Size);
        String tmpStr = null;
        String tmpSep = "";
        String lastSetTime = "";
        String counterSep = "";
        StringBuilder ret = new StringBuilder();

        if (!blocking) {

            int counter = 0;
            /*
            String testList = getTargetTagIndexList(tag);
            System.out.println("tag[" + tag + "]");
            System.out.println("testList[" + testList);
            String[] testIndexs = testList.split(KeyMapManager.tagKeySep);
            System.out.println("[0]=" + getTargetIndexTagPair(tag, new Integer(testIndexs[0]).intValue()));
            System.out.println("[1]=" + getTargetIndexTagPair(tag, new Integer(testIndexs[1]).intValue()));
            */
            // Tagのキー値を連結
            for (int idx = 0; idx < ImdstDefine.tagRegisterParallelBucket; idx=idx+ImdstDefine.tagBucketMaxLink) {

                keyStrs = null;
                setTimeSplitWork = null;
                isMatch = false;
                tmpBuf = new StringBuilder(ImdstDefine.stringBufferLarge_2Size);
                tmpStr = null;
                tmpSep = "";


                counter = idx;
                while(true) {

                    String tagCnv = KeyMapManager.tagStartStr + tag + "_" + counter + KeyMapManager.tagEndStr;

                    if (this.containsKeyPair(tagCnv)) {

                        tmpStr = (String)this.getKeyPair(tagCnv);

                        if (tmpStr != null && !((String[])tmpStr.split("!"))[0].equals("*")) {

                            isMatch = true;
                            tmpBuf.append(tmpSep);

                            setTimeSplitWork = tmpStr.split(ImdstDefine.setTimeParamSep);

                            if (setTimeSplitWork.length > 1) lastSetTime = setTimeSplitWork[1];

                            tmpBuf.append(setTimeSplitWork[0]);
                            tmpSep = KeyMapManager.tagKeySep;

                        } else if (tmpStr == null || ((String[])tmpStr.split("!"))[0].equals("*")){

                            if (!isMatch) {

                                keyStrs = null;
                            } else {

                                keyStrs = tmpBuf.toString();
                            }
                            break;
                        }
                    } else {

                        if (!isMatch) {
                            keyStrs = null;
                        } else {
                            keyStrs = tmpBuf.toString();
                        }
                        break;
                    }
                    counter++;
                }

                if (keyStrs != null) {
                    ret.append(counterSep);
                    ret.append(keyStrs);
                    counterSep = KeyMapManager.tagKeySep;
                }
            }
        }

        if (ret.toString().equals("")) {
            keyStrs = null;
        } else {
            ret.append(ImdstDefine.setTimeParamSep).append(lastSetTime);
            keyStrs = ret.toString();
        } 

        return keyStrs;
    }


    /**
     * 対象のTagのデータが存在する位置をKeyMapManager.tagKeySepで連結した文字列を返す<.br>
     * 本メソッドで取得したIndexをgetTargetIndexTagPairに渡すことで指定位置のTagとKeyのペアを取得可能.<br>
     *
     * @param tag タグ
     * @return 指定したTag値が分割保存されているIndexの中で実際に値が存在するIndexのKeyMapManager.tagKeySep区切り文字列。Tagが登録されていない場合はnullが返る
     */
    public String getTargetTagIndexList(String tag) {
        String keyIndexList = "";
        String tmpStr = null;
        String tmpSep = "";
        StringBuilder retBuf = new StringBuilder(1024);

        if (!blocking) {

            int counter = 0;
            tmpSep = "";

            // Tagのキー値を連結
            for (int idx = 0; idx < ImdstDefine.tagRegisterParallelBucket; idx=idx+ImdstDefine.tagBucketMaxLink) {

                counter = idx;

                while(true) {

                    String tagCnv = KeyMapManager.tagStartStr + tag + "_" + counter + KeyMapManager.tagEndStr;

                    if (this.containsKeyPair(tagCnv)) {

                        tmpStr = (String)this.getKeyPair(tagCnv);

                        if (tmpStr != null && !((String[])tmpStr.split("!"))[0].equals("*")) {


                            retBuf.append(tmpSep);
                            retBuf.append(counter);
                            tmpSep = KeyMapManager.tagKeySep;
                            break;
                        } else if (tmpStr == null || ((String[])tmpStr.split("!"))[0].equals("*")){

                            break;
                        }
                    } else {

                        break;
                    }
                    counter++;
                }
            }
        }


        keyIndexList = retBuf.toString();
        if (keyIndexList == null || keyIndexList.equals("")) {
            keyIndexList = null;
        } 
        return keyIndexList;
    }


    public String getTargetIndexTagPair(String tag, int index) {
        String keyStrs = "";
        String[] setTimeSplitWork = null;

        boolean isMatch = false;
        StringBuilder tmpBuf = new StringBuilder(ImdstDefine.stringBufferLarge_2Size);
        String tmpStr = null;
        String tmpSep = "";
        String lastSetTime = "";
        String counterSep = "";
        StringBuilder ret = new StringBuilder();

        if (!blocking) {

            int counter = 0;


            // Tagのキー値を連結
            keyStrs = null;
            setTimeSplitWork = null;
            isMatch = false;
            tmpBuf = new StringBuilder(ImdstDefine.stringBufferLarge_2Size);
            tmpStr = null;
            tmpSep = "";


            counter = index;
            while(true) {

                String tagCnv = KeyMapManager.tagStartStr + tag + "_" + counter + KeyMapManager.tagEndStr;

                if (this.containsKeyPair(tagCnv)) {

                    tmpStr = (String)this.getKeyPair(tagCnv);

                    if (tmpStr != null && !((String[])tmpStr.split("!"))[0].equals("*")) {

                        isMatch = true;
                        tmpBuf.append(tmpSep);

                        setTimeSplitWork = tmpStr.split(ImdstDefine.setTimeParamSep);

                        if (setTimeSplitWork.length > 1) lastSetTime = setTimeSplitWork[1];

                        tmpBuf.append(setTimeSplitWork[0]);
                        tmpSep = KeyMapManager.tagKeySep;

                    } else if (tmpStr == null || ((String[])tmpStr.split("!"))[0].equals("*")){

                        if (!isMatch) {

                            keyStrs = null;
                        } else {

                            keyStrs = tmpBuf.toString();
                        }
                        break;
                    }
                } else {

                    if (!isMatch) {
                        keyStrs = null;
                    } else {
                        keyStrs = tmpBuf.toString();
                    }
                    break;
                }
                counter++;
            }

            if (keyStrs != null) {

                ret.append(counterSep);
                ret.append(keyStrs);
                counterSep = KeyMapManager.tagKeySep;
            }
        }

        if (ret.toString().equals("")) {
            keyStrs = null;
        } else {
            keyStrs = ret.toString();
        } 

        return keyStrs;
    }

    // Tagを指定することでTagを消す
    public String removeTagRelation(String tag, String transactionCode) throws BatchException {
        String keyStrs = "";
        String[] setTimeSplitWork = null;

        boolean isMatch = false;
        StringBuilder tmpBuf = new StringBuilder(ImdstDefine.stringBufferLarge_2Size);
        String tmpStr = null;
        String tmpSep = "";
        String lastSetTime = "";
        String counterSep = "";
        StringBuilder ret = new StringBuilder();
        try {        

            if (!blocking) {
                int counter = 0;

                // Tagを消し込む
                // 返却値として関係するKey値群を返す
                synchronized(this.tagSetParallelSyncObjs[((tag.hashCode() << 1) >>> 1) % KeyMapManager.tagSetParallelSize]) {
                    for (int idx = 0; idx < ImdstDefine.tagRegisterParallelBucket; idx=idx+ImdstDefine.tagBucketMaxLink) {
                        keyStrs = "";
                        setTimeSplitWork = null;
                        isMatch = false;
                        tmpBuf = new StringBuilder(ImdstDefine.stringBufferLarge_2Size);
                        tmpStr = null;
                        tmpSep = "";
                        
                        
                        counter = idx;
                        while(true) {
                
                            String tagCnv = KeyMapManager.tagStartStr + tag + "_" + counter + KeyMapManager.tagEndStr;
                            
                            if (this.containsKeyPair(tagCnv)) {

                                tmpStr = (String)this.removeKeyPair(tagCnv, transactionCode);
                
                                if (tmpStr != null) {
                
                                    isMatch = true;
                                    tmpBuf.append(tmpSep);
                
                                    setTimeSplitWork = tmpStr.split(ImdstDefine.setTimeParamSep);
                
                                    if (setTimeSplitWork.length > 1) lastSetTime = setTimeSplitWork[1];
                
                                    tmpBuf.append(setTimeSplitWork[0]);
                                    tmpSep = KeyMapManager.tagKeySep;
                                } else {
                
                                    if (!isMatch) {
                
                                        keyStrs = null;
                                    } else {
                
                                        keyStrs = tmpBuf.toString();
                                    }
                                    break;
                                }
                            } else {
                
                                if (!isMatch) {
                                    keyStrs = null;
                                } else {
                                    keyStrs = tmpBuf.toString();
                                }
                                break;
                            }
                            counter++;
                        }
                        
                        if (keyStrs != null) {
                            
                            ret.append(counterSep);
                            ret.append(keyStrs);
                            counterSep = KeyMapManager.tagKeySep;
                        }
                    }
                }
            }
            
            if (ret.toString().equals("")) {
                keyStrs = null;
            } else {
                ret.append(ImdstDefine.setTimeParamSep).append(lastSetTime);
                keyStrs = ret.toString();
            } 
        } catch (Exception e) {
            e.printStackTrace();
            throw new BatchException(e);
        }

        return keyStrs;
    }


    /**
     * キーを指定することで紐付くValueに計算を行う.<br>
     *
     * @param key キー値
     * @param calcVal 計算値(数値)
     * @param transactionCode
     */
    public String calcValue(String key, int calcVal, String transactionCode, boolean initCalcValueFlg) throws BatchException {
        String ret = null;
        String data = null;
        
        if (!blocking) {
            try {

                //logger.debug("calcValue - synchronized - start");
                // このsynchroの方法は正しくないきがするが。。。
                synchronized(this.parallelSyncObjs[((key.hashCode() << 1) >>> 1) % KeyMapManager.parallelSize]) {
                    boolean containsKeyRet = containsKeyPair(key);
                    // 初期化指定がされていて現在値が存在しない場合は0で初期化をまず行う

                    if (initCalcValueFlg == true && containsKeyRet == false) {

                        boolean initRet = this.setKeyPairOnlyOnce(key, new String(BASE64EncoderStream.encode("0".getBytes())) + ImdstDefine.setTimeParamSep + "0", "0");
                        if (initRet) {
                            containsKeyRet = true;
                        } else {
                            containsKeyRet = containsKeyPair(key);
                        }
                    }

                    if (containsKeyRet) {

                        String tmp = keyMapObjGet(key);
                        String[] keyNoddes = tmp.split(ImdstDefine.setTimeParamSep);
                        String setDataStr = null;

                        if (tmp != null) {

                            String targetData = keyNoddes[0];

                            if (keyNoddes[0].indexOf(",") != -1) {
                                String[] workSplitData = keyNoddes[0].split(",");
                                targetData = workSplitData[0];
                            }
                            String nowData = new String(BASE64DecoderStream.decode(targetData.getBytes()));
                            long nowDataLong = 0;

                            try {

                                nowDataLong = Long.parseLong(nowData);
                                nowDataLong = nowDataLong + calcVal;
                                if (nowDataLong < 0) nowDataLong = 0; 
                                setDataStr = new Long(nowDataLong).toString();
                            } catch (Exception e){

                                if (calcVal > 0) {
                                    setDataStr = new Long(calcVal).toString();
                                } else {
                                    setDataStr = new Long(nowDataLong).toString();
                                }
                            }

                            if (keyNoddes.length > 1) {

                                data = new String(BASE64EncoderStream.encode(setDataStr.getBytes())) + ImdstDefine.setTimeParamSep + (Long.parseLong(keyNoddes[1]) + 1);
                            } else {

                                data = new String(BASE64EncoderStream.encode(setDataStr.getBytes())) + ImdstDefine.setTimeParamSep + "0";
                            }

                        }
                    } 

                    if (data != null) {
                        // 登録
                        keyMapObjPut(key, data);

                        // データ操作履歴ファイルに追記
                        if (this.workFileMemory == false) {
                            synchronized(this.lockWorkFileSync) {

                                if (this.workFileFlushTiming) {

                                    this.bw.write(new StringBuilder(ImdstDefine.stringBufferSmall_2Size).append("+").append(KeyMapManager.workFileSeq).append(key).append(KeyMapManager.workFileSeq).append(data).append(KeyMapManager.workFileSeq).append(JavaSystemApi.currentTimeMillis).append(KeyMapManager.workFileSeq).append(KeyMapManager.workFileEndPoint).append("\n").toString());
                                    SystemUtil.diskAccessSync(this.bw);
                                    this.checkTransactionLogWriterLimit(this.tLogWriteCount.incrementAndGet());
                                } else {

                                    this.dataTransactionFileFlushDaemon.addDataTransaction(new StringBuilder(ImdstDefine.stringBufferSmall_2Size).append("+").append(KeyMapManager.workFileSeq).append(key).append(KeyMapManager.workFileSeq).append(data).append(KeyMapManager.workFileSeq).append(JavaSystemApi.currentTimeMillis).append(KeyMapManager.workFileSeq).append(KeyMapManager.workFileEndPoint).append("\n").toString());
                                }
                            }
                        }
                        
                        // Diffモードでかつsync後は再度モードを確認後、addする
                        if (this.diffDataPoolingFlg) {
                            synchronized (diffSync) {
                                if (this.diffDataPoolingFlg) {
    
                                    this.diffDataPoolingListForFileBase.add("+" + KeyMapManager.workFileSeq + key + KeyMapManager.workFileSeq +  data);
                                }
                            }
                        }
                        // データの書き込みを指示
                        this.writeMapFileFlg = true;
                        ret = ((String[])data.split(ImdstDefine.setTimeParamSep))[0];
                    }
                    
                }

                //logger.debug("setKeyPair - synchronized - end");

            } catch (BatchException be) {
                logger.error("calcValue - InnerError", be);
                throw be;
            } catch (Exception e) {
                e.printStackTrace();
                logger.error("setKeyPair - Error");
                blocking = true;
                StatusUtil.setStatusAndMessage(1, "setKeyPair - Error [" + e.getMessage() + "]");
                throw new BatchException(e);
            }
        }
        return ret;
    }


    /**
     * キーを指定することで紐付くValueの後ろに渡されたValueを付加する.<br>
     * 値が存在しない場合は新規の値としてただ登録される.<br>
     *
     * @param key キー値
     * @param appendValue 付加する値(base64でエンコード済み)
     * @param appendSep 値が既に存在する場合にセパレータとして付加する値(ブランク指定(B)の場合は何も付加されない)
     * @param transactionCode
     * @return boolean 成否 0=成功、1=サイズオーバによりエラー
     */
    public int appendValue(String key, String appendValue, String appendSep, String transactionCode) throws BatchException {
        int ret = -1;
        String data = null;
        
        if (!blocking) {
            try {

                //logger.debug("appendValue - synchronized - start");
                // このsynchroの方法は正しくないきがするが。。。
                synchronized(this.parallelSyncObjs[((key.hashCode() << 1) >>> 1) % KeyMapManager.parallelSize]) {
                    boolean containsKeyRet = containsKeyPair(key);
                    if (containsKeyRet) {

                        String tmp = keyMapObjGet(key);
                        String[] keyNoddes = tmp.split(ImdstDefine.setTimeParamSep);
                        String setDataStr = null;

                        if (tmp != null) {

                            String targetData = keyNoddes[0];

                            if (keyNoddes[0].indexOf(",") != -1) {
                                String[] workSplitData = keyNoddes[0].split(",");
                                targetData = workSplitData[0];
                            }

                            if (targetData.equals(ImdstDefine.imdstBlankStrData)) {
                                if (appendValue.equals(ImdstDefine.imdstBlankStrData)) {
                                    setDataStr = appendValue;
                                } else {
                                    setDataStr = new String(BASE64DecoderStream.decode(appendValue.getBytes()));
                                }
                            } else {

                                if (appendSep.equals(ImdstDefine.imdstBlankStrData)) {
                                    appendSep = "";
                                } else {
                                    appendSep = new String(BASE64DecoderStream.decode(appendSep.getBytes()));
                                }
                                String nowData = new String(BASE64DecoderStream.decode(targetData.getBytes()));
                                setDataStr = nowData + appendSep + appendValue;
                            }

                            // 登録前に長さをチェック
                            if (ImdstDefine.saveDataMaxSize < ((byte[])setDataStr.getBytes()).length) {
                                return 1;
                            }
            
                            if (keyNoddes.length > 1) {

                                data = new String(BASE64EncoderStream.encode(setDataStr.getBytes())) + ImdstDefine.setTimeParamSep + (Long.parseLong(keyNoddes[1]) + 1);
                            } else {

                                data = new String(BASE64EncoderStream.encode(setDataStr.getBytes())) + ImdstDefine.setTimeParamSep + "0";
                            }

                        }
                    } 

                    if (data != null) {
                        // 登録
                        keyMapObjPut(key, data);

                        // データ操作履歴ファイルに追記
                        if (this.workFileMemory == false) {
                            synchronized(this.lockWorkFileSync) {

                                if (this.workFileFlushTiming) {

                                    this.bw.write(new StringBuilder(ImdstDefine.stringBufferSmall_2Size).append("+").append(KeyMapManager.workFileSeq).append(key).append(KeyMapManager.workFileSeq).append(data).append(KeyMapManager.workFileSeq).append(JavaSystemApi.currentTimeMillis).append(KeyMapManager.workFileSeq).append(KeyMapManager.workFileEndPoint).append("\n").toString());
                                    SystemUtil.diskAccessSync(this.bw);
                                    this.checkTransactionLogWriterLimit(this.tLogWriteCount.incrementAndGet());
                                } else {

                                    this.dataTransactionFileFlushDaemon.addDataTransaction(new StringBuilder(ImdstDefine.stringBufferSmall_2Size).append("+").append(KeyMapManager.workFileSeq).append(key).append(KeyMapManager.workFileSeq).append(data).append(KeyMapManager.workFileSeq).append(JavaSystemApi.currentTimeMillis).append(KeyMapManager.workFileSeq).append(KeyMapManager.workFileEndPoint).append("\n").toString());
                                }
                            }
                        }
                        
                        // Diffモードでかつsync後は再度モードを確認後、addする
                        if (this.diffDataPoolingFlg) {
                            synchronized (diffSync) {
                                if (this.diffDataPoolingFlg) {
    
                                    this.diffDataPoolingListForFileBase.add("+" + KeyMapManager.workFileSeq + key + KeyMapManager.workFileSeq +  data);
                                }
                            }
                        }

                        // データの書き込みを指示
                        this.writeMapFileFlg = true;
                        ret = 0;
                    }
                }

                //logger.debug("appendValue - synchronized - end");
            } catch (BatchException be) {
                logger.error("appendValue - InnerError", be);
                throw be;
            } catch (Exception e) {
                e.printStackTrace();
                logger.error("appendValue - Error");
                blocking = true;
                StatusUtil.setStatusAndMessage(1, "appendValue - Error [" + e.getMessage() + "]");
                throw new BatchException(e);
            }
        }
        return ret;
    }


    // キーを指定することでキーが存在するかを返す
    public boolean containsKeyPair(String key) {
        boolean ret = false;
        if (!blocking) {
            if (key != null) {
                if(this.keyMapObj != null) ret =  this.keyMapObj.containsKey(key);
            }
        }
        return ret;
    }

    // キーを指定することでキーが存在するかを返す
    public boolean containsTagPair(String tag) {
        boolean ret = false;
        if (!blocking) {

            for (int idx = 0; idx < ImdstDefine.tagRegisterParallelBucket; idx=idx+ImdstDefine.tagBucketMaxLink) {

                String tagCnv = KeyMapManager.tagStartStr + tag + "_" + idx + KeyMapManager.tagEndStr;

                ret =  this.containsKeyPair(tagCnv);
                if (ret) break;
            }
        }
        return ret;
    }

    /**
     * Lockの取得を行う.<br>
     * 
     * @param key Key値
     * @param transactionCode 取得時に使用するTransactionCode
     * @param lockingTime Lock継続時間
     * @return String 成功時はtransactionCode、失敗時はnull
     */
    public String locking (String key, String transactionCode, int lockingTime) throws BatchException {
        if (!blocking) {
            try {
                String saveTransactionStr =  null;
                synchronized(this.lockKeyLock) {

                    if (this.containsKeyPair(key)) return null;
                    if (lockingTime == 0) {
                        saveTransactionStr = transactionCode + this.lockKeyTimeSep + new Long(Long.MAX_VALUE).toString();
                    } else {
                        saveTransactionStr = transactionCode + this.lockKeyTimeSep + new Long(JavaSystemApi.currentTimeMillis + (lockingTime * 1000)).toString();
                    }
                    keyMapObjPut(key, saveTransactionStr);
                }
                this.writeMapFileFlg = true;

                if (workFileMemory == false) {
                    synchronized(this.lockWorkFileSync) {
                        // データ格納場所記述ファイル再保存
                        if (this.workFileFlushTiming) {

                            this.bw.write(new StringBuilder("+").append(KeyMapManager.workFileSeq).append(key).append(KeyMapManager.workFileSeq).append(saveTransactionStr).append(KeyMapManager.workFileSeq).append(JavaSystemApi.currentTimeMillis).append(KeyMapManager.workFileSeq).append(KeyMapManager.workFileEndPoint).append("\n").toString());
                            SystemUtil.diskAccessSync(this.bw);
                            this.checkTransactionLogWriterLimit(this.tLogWriteCount.incrementAndGet());
                        } else {

                            this.dataTransactionFileFlushDaemon.addDataTransaction(new StringBuilder("+").append(KeyMapManager.workFileSeq).append(key).append(KeyMapManager.workFileSeq).append(saveTransactionStr).append(KeyMapManager.workFileSeq).append(JavaSystemApi.currentTimeMillis).append(KeyMapManager.workFileSeq).append(KeyMapManager.workFileEndPoint).append("\n").toString());
                        }
                    }
                }

                if (this.diffDataPoolingFlg) {
                    synchronized (diffSync) {
                        if (this.diffDataPoolingFlg) {

                            this.diffDataPoolingListForFileBase.add("+" + KeyMapManager.workFileSeq + key + KeyMapManager.workFileSeq +  saveTransactionStr);
                        }
                    }
                }

            } catch (Exception e) {
                e.printStackTrace();
                logger.error("locking - Error");
                blocking = true;
                StatusUtil.setStatusAndMessage(1, "locking - Error [" + e.getMessage() + "]");

                throw new BatchException(e);
            }
        }
        return transactionCode;
    }


    /**
     * Lockの開放を行う.<br>
     * 
     * @param key Key値
     * @param transactionCode 取得時に使用するTransactionCode
     * @return String 成功時はtransactionCode、失敗時はnull
     */
    public String removeLock (String key, String transactionCode) throws BatchException {

        String ret = null;
        if (!blocking) {
            try {
                synchronized(this.lockKeyLock) {
                    if (!this.containsKeyPair(key)) return transactionCode;
                    if (!(((String[])((String)this.keyMapObjGet(key)).split(this.lockKeyTimeSep))[0]).equals(transactionCode)) return null;
                    ret = ((String[])((String)this.keyMapObj.remove(key)).split(this.lockKeyTimeSep))[0];
                    this.keyMapObj.setKLastDataChangeTime(JavaSystemApi.currentTimeMillis);

                    this.lastAccess = JavaSystemApi.currentTimeMillis;
                }

                // データの書き込みを指示
                this.writeMapFileFlg = true;

                if (workFileMemory == false) {
                    synchronized(this.lockWorkFileSync) {
                        // データ格納場所記述ファイル再保存(登録と合わせるために4つに分割できるようにする)
                        if (this.workFileFlushTiming) {

                            this.bw.write(new StringBuilder("-").append(KeyMapManager.workFileSeq).append(key).append(KeyMapManager.workFileSeq).append(" ").append(KeyMapManager.workFileSeq).append(JavaSystemApi.currentTimeMillis).append(KeyMapManager.workFileSeq).append(KeyMapManager.workFileEndPoint).append("\n").toString());
                            SystemUtil.diskAccessSync(this.bw);
                            this.checkTransactionLogWriterLimit(this.tLogWriteCount.incrementAndGet());
                        } else {

                            this.dataTransactionFileFlushDaemon.addDataTransaction(new StringBuilder("-").append(KeyMapManager.workFileSeq).append(key).append(KeyMapManager.workFileSeq).append(" ").append(KeyMapManager.workFileSeq).append(JavaSystemApi.currentTimeMillis).append(KeyMapManager.workFileSeq).append(KeyMapManager.workFileEndPoint).append("\n").toString());
                        }
                    }
                }

                if (this.diffDataPoolingFlg) {
                    synchronized (diffSync) {
                        if (this.diffDataPoolingFlg) {

                            this.diffDataPoolingListForFileBase.add("-" + KeyMapManager.workFileSeq + key);
                        }
                    }
                }

            } catch (Exception e) {
                logger.error("removeLock - Error");
                blocking = true;
                StatusUtil.setStatusAndMessage(1, "removeLock - Error[" + e.getMessage() + "]");
                throw new BatchException(e);
            }
        }

        return ret;
    }


    /**
     * Lockの状況を確認する.<br>
     * 
     * @param key Key値
     * @return boolean true:ロックされている false:ロックされていない
     */
    public boolean isLock (String key) {
        return this.containsKeyPair(key);
    }


    /**
     * Lockの状況を確認する.<br>
     * 
     * @param key Key値
     * @return String TransactionCode
     */
    public String getLockedTransactionCode (String key) {
        synchronized(this.lockKeyLock) {
            try {
                if (!this.containsKeyPair(key)) return null;
                return ((String[])((String)this.keyMapObjGet(key)).split(this.lockKeyTimeSep))[0];
            } catch (Exception e) {
                return null;
            }
        }
    }


    /**
     * Lockの自動開放メソッド.<br>
     * 引数の時間だけ経過しているLockは強制的に開放される<br>
     *
     * @param time 現在ミリ秒
     */
    public void autoLockRelease(long time) throws BatchException {
        synchronized(this.setKeyLock) {
            synchronized(this.lockKeyLock) {
                try {
                    Object key = null;

                    Set set = this.keyMapObj.entrySet();
                    Iterator iterator = set.iterator();

                    String[] keyList = new String[this.keyMapObj.size()];
                    for (int idx = 0; idx < keyList.length; idx++) {
                        Map.Entry map = (Map.Entry)iterator.next();
                        keyList[idx] = (String)map.getKey();
                    }

                    for (int idx = 0; idx < keyList.length; idx++) {
                        String transactionLine = (String)this.keyMapObj.get(keyList[idx]);
                        String[] codeList = transactionLine.split(this.lockKeyTimeSep);
                        if(Long.parseLong(codeList[1]) < time) {

                            this.keyMapObj.remove(keyList[idx]);
                            // データの書き込みを指示
                            this.writeMapFileFlg = true;

                            if (workFileMemory == false) {
                                synchronized(this.lockWorkFileSync) {
                                    // データ格納場所記述ファイル再保存(登録と合わせるために4つに分割できるようにする)
                                    if (this.workFileFlushTiming) {

                                        this.bw.write(new StringBuilder("-").append(KeyMapManager.workFileSeq).append(keyList[idx]).append(KeyMapManager.workFileSeq).append(" ").append(KeyMapManager.workFileSeq).append(JavaSystemApi.currentTimeMillis).append(KeyMapManager.workFileSeq).append(KeyMapManager.workFileEndPoint).append("\n").toString());
                                        SystemUtil.diskAccessSync(this.bw);
                                        this.checkTransactionLogWriterLimit(this.tLogWriteCount.incrementAndGet());
                                    } else {

                                        this.dataTransactionFileFlushDaemon.addDataTransaction(new StringBuilder("-").append(KeyMapManager.workFileSeq).append(keyList[idx]).append(KeyMapManager.workFileSeq).append(" ").append(KeyMapManager.workFileSeq).append(JavaSystemApi.currentTimeMillis).append(KeyMapManager.workFileSeq).append(KeyMapManager.workFileEndPoint).append("\n").toString());
                                    }
                                }
                            }

                            if (this.diffDataPoolingFlg) {
                                synchronized (diffSync) {
                                    if (this.diffDataPoolingFlg) {

                                        this.diffDataPoolingListForFileBase.add("-" + KeyMapManager.workFileSeq + keyList[idx]);
                                    }
                                }
                            }
                        }
                    }
                } catch(Exception e) {
                    logger.error("autoLockRelease - Error");
                    blocking = true;
                    StatusUtil.setStatusAndMessage(1, "autoLockRelease - Error[" + e.getMessage() + "]");
                    throw new BatchException(e);
                }

            }
        }
    }


    /**
     * keyMapObjに対するアクセスメソッド.<br>
     * TODO:登録のValueのサイズが最大サイズを超えている場合は無条件で登録しない.<br>
     */
    private void keyMapObjPut(String key, String val) throws BatchException {
        try {
            if (val.length() < putValueMaxSize) {
                if (key != null && val != null) {
                    this.keyMapObj.put(key, val);
                    this.keyMapObj.setKLastDataChangeTime(JavaSystemApi.currentTimeMillis);
                }
            }
            this.lastAccess = JavaSystemApi.currentTimeMillis;
        } catch (Exception e) {
            throw new BatchException(e);
        }
    }

    private void setLastDataChangeTime() {
        this.keyMapObj.setKLastDataChangeTime(JavaSystemApi.currentTimeMillis);
    }
    /**
     * keyMapObjに対するアクセスメソッド.<br>
     * 更新時間を登録しない.<br>
     * TODO:登録のValueのサイズが最大サイズを超えている場合は無条件で登録しない.<br>
     */
    private void keyMapObjPutNoChange(String key, String val) {
        if (val.length() < putValueMaxSize) {
            if (key != null && val != null) {
                this.keyMapObj.put(key, val);
            }
        }
        this.lastAccess = JavaSystemApi.currentTimeMillis;
    }

    /**
     * keyMapObjに対するアクセスメソッド.<br>
     * 任意の更新時間をセットする.<br>
     * 登録のValueのサイズが最大サイズを超えている場合は無条件で登録しない.<br>
     */
    private void keyMapObjPutSetTime(String key, String val, long execTime) {
        if (val.length() < putValueMaxSize) {
            if (key != null && val != null) {
                this.keyMapObj.put(key, val);
                this.keyMapObj.setKLastDataChangeTime(execTime);
            }
        }
        this.lastAccess = JavaSystemApi.currentTimeMillis;
    }


    /**
     * keyMapObjに対するアクセスメソッド.<br>
     * get<br>
     */
    private String keyMapObjGet(String key) throws BatchException {
        try {
            this.lastAccess = JavaSystemApi.currentTimeMillis;
            if (key != null) {
                return (String)this.keyMapObj.get(key);
            } else {
                return null;
            }
        } catch (Exception e) {
            throw new BatchException(e);
        }
    }

    /**
     * keyMapObjに対するアクセスメソッド.<br>
     * Valueをファイルにしている場合にファイル上の位置を特定する
     * get<br>
     */
    private long keyMapObjDataPointGet(String key) throws BatchException {
        try {
            this.lastAccess = JavaSystemApi.currentTimeMillis;
            if (key != null) {
                return this.keyMapObj.dataPointGet(key);
            } else {
                return -1;
            }
        } catch (Exception e) {
            throw new BatchException(e);
        }
    }



    /**
     * keyMapObjに対するアクセスメソッド.<br>
     * 
     */
    private void keyMapObjRemove(String key) throws BatchException {
        try {

            if (key != null) {
                this.keyMapObj.remove(key);
                this.keyMapObj.setKLastDataChangeTime(JavaSystemApi.currentTimeMillis);
                this.lastAccess = JavaSystemApi.currentTimeMillis;
            }
        } catch (Exception e) {
            throw new BatchException(e);
        }
    }

    /**
     * keyMapObjに対するアクセスメソッド.<br>
     * 更新時間をセットしない
     */
    private void keyMapObjRemoveNoChange(String key) {
        if (key != null) {
            this.keyMapObj.remove(key);
            this.lastAccess = JavaSystemApi.currentTimeMillis;
        }
    }


    /**
     * keyMapObjに対するアクセスメソッド.<br>
     *  任意の更新時間をセットする.<br>
     */
    private void keyMapObjRemoveSetTime(String key, long execTime) {
        if (key != null) {
            this.keyMapObj.remove(key);
            this.keyMapObj.setKLastDataChangeTime(execTime);
            this.lastAccess = JavaSystemApi.currentTimeMillis;
        }
    }


    public void diffDataMode(boolean flg, PrintWriter pw) {
        synchronized (diffSync) {

            if (flg) {
                this.myDiffModeOperationStatus = 2;
                this.diffDataPoolingListForFileBase = new FileBaseDataList(this.nodeKeyMapFilePath + ".difftmplist");
            } else {

                if (this.diffDataPoolingListForFileBase != null) {
                    this.diffDataPoolingListForFileBase.clear();
                    this.diffDataPoolingListForFileBase = null;
                }
                this.myDiffModeOperationStatus = 1;
            }
            this.diffDataPoolingFlg = flg;
            try {
                pw.println("1");
                pw.flush();
            } catch (Exception e) {
            }
        }
    }


    public void diffDataMode(boolean flg) {
        synchronized (diffSync) {

            if (flg) {
                this.myDiffModeOperationStatus = 2;
                this.diffDataPoolingListForFileBase = new FileBaseDataList(this.nodeKeyMapFilePath + ".difftmplist");
            } else {

                if (this.diffDataPoolingListForFileBase != null) {
                    this.diffDataPoolingListForFileBase.clear();
                    this.diffDataPoolingListForFileBase = null;
                }
                this.myDiffModeOperationStatus = 1;
            }
            this.diffDataPoolingFlg = flg;
        }
    }


    // 強制的に差分モードをOffにする
    public void diffDataModeOff() {
        synchronized (diffSync) {
            if (this.diffDataPoolingListForFileBase != null) {
                this.diffDataPoolingListForFileBase.clear();
                this.diffDataPoolingListForFileBase = null;
            }
            this.myDiffModeOperationStatus = 1;

            this.diffDataPoolingFlg = false;
        }
    }


    // 引数で渡されてストリームに対し全Keyを書き出す
    public void outputKeyData2Stream(PrintWriter pw) throws BatchException {
        if (!blocking) {
            try {

                synchronized(poolKeyLock) {

                    Set entrySet = this.keyMapObj.entrySet();

                    Iterator entryIte = entrySet.iterator(); 

                    long counter = 0;
                    int sendCounter = 0;

                    while(entryIte.hasNext()) {
                        Map.Entry obj = (Map.Entry)entryIte.next();
                        if (obj == null) continue;
                        String key = null;
                        key = (String)obj.getKey();
                        try {
                            String decodeKey = new String(BASE64DecoderStream.decode(key.getBytes()));
                            if (decodeKey.indexOf("{imdst_") != 0) { // TagとListは除外
                                pw.println(key);
                                pw.flush();
                            }
                        } catch(Exception ee){}
                    }

                    pw.println("-1");
                    pw.flush();
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    // 引数で渡されてストリームに対しkeyMapObjを書き出す
    public void outputKeyMapObj2Stream(PrintWriter pw) throws BatchException {
        if (!blocking) {
            try {

                synchronized(poolKeyLock) {
                    this.myOperationStatus = 3;
                    logger.info("outputKeyMapObj2Stream - synchronized - start");
                    String allDataSep = "";
                    StringBuilder allDataBuf = new StringBuilder(ImdstDefine.stringBufferLarge_3Size);

                    // keyMapObjの全内容を1行文字列として書き出し
                    Set entrySet = this.keyMapObj.entrySet();

                    int printLineCount = 0;
                    // 一度に送信するデータ量を算出。空きメモリの30%を使用する
                    int maxLineCount = new Double((JavaSystemApi.getRuntimeFreeMem("") * 0.3) / (ImdstDefine.saveDataMaxSize / 50)).intValue();
                    // 最大でも10万件以上を一度に送信はしない
                    if (maxLineCount > 100000) maxLineCount = 100000;

                    if (ImdstDefine.lowSpecDataNode) {
                        if (maxLineCount > ImdstDefine.lowSpecDataNodeSendDataCount) maxLineCount = ImdstDefine.lowSpecDataNodeSendDataCount;
                    }

                    if (entrySet.size() > 0) {
                        if(maxLineCount == 0) maxLineCount = 1;
                        printLineCount = new Double(entrySet.size() / maxLineCount).intValue();
                        if (entrySet.size() % maxLineCount > 0) {
                            printLineCount = printLineCount + 1;
                        }
                    }

                    // 送信データ行数を送信
                    pw.println(printLineCount);
                    pw.flush();

                    Iterator entryIte = entrySet.iterator(); 

                    long counter = 0;
                    int sendCounter = 0;
                    if (this.keyMapObj.memoryMode) {
                        while(entryIte.hasNext()) {

                            if (this.outputDataStopSignal == true) {
                                logger.info("outputKeyMapObj2Stream - Stop Signal");
                                break;
                            }
                            if ((counter % 1000) == 0) logger.info("outputKeyMapObj2Stream - output count[" + counter + "]");

                            Map.Entry obj = (Map.Entry)entryIte.next();
                            if (obj == null) continue;
                            String key = null;

                            key = (String)obj.getKey();


                            // 全てのデータを送る
                            allDataBuf.append(allDataSep);
                            allDataBuf.append(key);

                            allDataBuf.append(KeyMapManager.workFileSeq);
                            allDataBuf.append(this.keyMapObjGet(key));
                            allDataSep = ImdstDefine.imdstConnectAllDataSendDataSep;

                            counter++;
                            if (counter > (maxLineCount - 1)) {
                                sendCounter++;
                                pw.println(allDataBuf.toString());
                                pw.flush();
                                allDataBuf = new StringBuilder(ImdstDefine.stringBufferLarge_3Size);
                                if (ImdstDefine.lowSpecDataNode) Thread.sleep(50);
                                counter = 0;
                                if ((sendCounter % 10) == 0) Thread.sleep(3000);
                            }
                        }

                        String lastSendStr = allDataBuf.toString();
                        if (!lastSendStr.equals("")) {
                            pw.println(lastSendStr);
                            pw.flush();
                        }
                        allDataBuf = null;

                        pw.println("-1");
                        pw.flush();
                    } else {
                        // Key-Valueどちらかがメモリではない
                        List keyList = new ArrayList();
                        Map pointToKeyMap = new HashMap();
                        while(entryIte.hasNext()) {

                            if (this.outputDataStopSignal == true) {
                                logger.info("outputKeyMapObj2Stream - Stop Signal");
                                break;
                            }
                            if ((counter % 1000) == 0) logger.info("outputKeyMapObj2Stream - output count[" + counter + "]");

                            Map.Entry obj = (Map.Entry)entryIte.next();
                            if (obj == null) continue;
                            String key = null;

                            
                            key = (String)obj.getKey();
                            long point = this.keyMapObjDataPointGet(key);
                            if (point != -1) {
                                pointToKeyMap.put(new Long(point), key);
                                keyList.add(point);
                            }
                            counter++;
                            if (counter > (maxLineCount - 1)) {

                                int keyListSize = keyList.size();
                                long[] keyListInt = new long[keyListSize];
                                for (int idx = 0; idx < keyListSize; idx++) {
                                    keyListInt[idx] = ((Long)keyList.get(idx)).longValue();
                                }
                                Arrays.sort(keyListInt);
                                keyList = null;
                                keyList = new ArrayList();
                                for (int idx = 0; idx < keyListInt.length; idx++) {

                                    // 全てのデータを送る
                                    String sendKey = (String)pointToKeyMap.get(new Long(keyListInt[idx]));
                                    allDataBuf.append(allDataSep);
                                    allDataBuf.append(sendKey);
                                    
                                    allDataBuf.append(KeyMapManager.workFileSeq);
                                    allDataBuf.append(this.keyMapObjGet(sendKey));
                                    allDataSep = ImdstDefine.imdstConnectAllDataSendDataSep;
                                }
                            
                                sendCounter++;
                                pw.println(allDataBuf.toString());
                                pw.flush();
                                if (ImdstDefine.lowSpecDataNode) Thread.sleep(50);
                                allDataBuf = new StringBuilder(ImdstDefine.stringBufferLarge_3Size);
                                counter = 0;
                                if ((sendCounter % 10) == 0) Thread.sleep(3000);

                                pointToKeyMap = new HashMap();
                            }
                        }

                        if (keyList.size() > 0) {

                            int keyListSize = keyList.size();
                            long[] keyListInt = new long[keyListSize];
                            for (int idx = 0; idx < keyListInt.length; idx++) {
                                keyListInt[idx] = ((Long)keyList.get(idx)).longValue();
                            }
                            Arrays.sort(keyListInt);
                            keyList = null;
                            keyList = new ArrayList();
                            for (int idx = 0; idx < keyListInt.length; idx++) {

                                // 全てのデータを送る
                                String sendKey = (String)pointToKeyMap.get(new Long(keyListInt[idx]));
                                allDataBuf.append(allDataSep);
                                allDataBuf.append(sendKey);
                                
                                allDataBuf.append(KeyMapManager.workFileSeq);
                                allDataBuf.append(this.keyMapObjGet(sendKey));
                                allDataSep = ImdstDefine.imdstConnectAllDataSendDataSep ;
                            }
                            String lastSendStr = allDataBuf.toString();
                            if (!lastSendStr.equals("")) {
                                pw.println(lastSendStr);
                                pw.flush();
                            }
                        }
                        allDataBuf = null;

                        pw.println("-1");
                        pw.flush();
                    }
                }

            } catch (Exception e) {
                e.printStackTrace();
                logger.error("outputKeyMapObj2Stream - Error =[" + e.getMessage() + "]");
            } finally {
                this.myOperationStatus = 1;
            }
        }
    }


    // 引数で渡されてストリームに対し復旧中の差分データを書き出す
    // 
    public void outputDiffKeyMapObj2Stream(PrintWriter pw, BufferedReader br) throws BatchException {
        if (!blocking) {
            try {

                synchronized(poolKeyLock) {
                    String nextWrite = null;
                    this.myOperationStatus = 3;
                    logger.info("outputDiffKeyMapObj2Stream - synchronized - start");
                    String allDataSep = "";
                    StringBuilder allDataBuf = new StringBuilder(ImdstDefine.stringBufferLarge_3Size);

                    // 差分データの全内容を1行文字列として書き出し
                    int i = 0;
                    ((FileBaseDataList)this.diffDataPoolingListForFileBase).waitTime = 300;
                    for (; i < this.diffDataPoolingListForFileBase.size() - 10; i++) {

                        allDataBuf.append(allDataSep);
                        allDataBuf.append(this.diffDataPoolingListForFileBase.get(i));
                        allDataSep = ImdstDefine.imdstConnectAllDataSendDataSep;

                        if (i > 0 && (i % 30) == 0) {
                            logger.info("outputDiffKeyMapObj2Stream - Diff Data Normal Send Count[" + i + "]");
                            pw.println(allDataBuf.toString());
                            pw.flush();
                            allDataBuf = null;
                            allDataBuf = new StringBuilder(ImdstDefine.stringBufferLarge_3Size);
                            allDataSep = "";

                            // 送信後結果を待つ
                            nextWrite = br.readLine();
                            if (nextWrite == null || (!nextWrite.equals("-1") && !nextWrite.equals("2"))) throw new Exception("NextWriteMessage= [" + nextWrite + "]");
                            nextWrite = null;
                        }
                    }

                    String lastSendData = allDataBuf.toString();
                    if (!lastSendData.equals("")) {
                        logger.info("outputDiffKeyMapObj2Stream - Diff Data Normal Send2 Count[" + i + "]");
                        pw.println(lastSendData);
                        pw.flush();
                        // 送信後結果を待つ
                        nextWrite = br.readLine();
                        if (nextWrite == null || (!nextWrite.equals("-1") && !nextWrite.equals("2"))) throw new Exception("NextWriteMessage= [" + nextWrite + "]");
                        nextWrite = null;
                    }


                    synchronized(diffSync) {
                        allDataBuf = null;
                        allDataBuf = new StringBuilder(ImdstDefine.stringBufferLarge_3Size);
                        allDataSep = "";

                        for (; i < this.diffDataPoolingListForFileBase.size(); i++) {

                            allDataBuf.append(allDataSep);
                            allDataBuf.append(this.diffDataPoolingListForFileBase.get(i));
                            allDataSep = ImdstDefine.imdstConnectAllDataSendDataSep;

                            if (i > 0 && (i % 20) == 0) {
                                logger.info("outputDiffKeyMapObj2Stream - Diff Data Last Send Count[" + i + "]");
                                pw.println(allDataBuf.toString());
                                pw.flush();
                                allDataBuf = null;
                                allDataBuf = new StringBuilder(ImdstDefine.stringBufferLarge_3Size);
                                allDataSep = "";

                                // 送信後結果を待つ
                                nextWrite = br.readLine();
                                if (nextWrite == null || (!nextWrite.equals("-1") && !nextWrite.equals("2"))) throw new Exception("Erro Target Read End Message= [" + nextWrite + "]");
                                nextWrite = null;
                            }
                        }

                        lastSendData = allDataBuf.toString();
                        if (!lastSendData.equals("")) {
                            pw.println(lastSendData);
                            pw.flush();
                            // 送信後結果を待つ
                            nextWrite = br.readLine();
                            if (nextWrite == null || (!nextWrite.equals("-1") && !nextWrite.equals("2"))) throw new Exception("Erro Target Read End Message= [" + nextWrite + "]");
                            nextWrite = null;
                        }

                        pw.println("-1");
                        pw.flush();


                        allDataBuf = null;
                        // 取り込み完了をまつ
                        String outputRet = br.readLine();

                        if (outputRet == null || !outputRet.equals("1")) {
                            throw new Exception("outputDiffKeyMapObj2Stream - Error Ret=[" + outputRet + "]");
                        }

                        // 終了受信を送信
                        pw.println("1");
                        pw.flush();
                        this.diffDataMode(false);
                    }
                }
                logger.info("outputDiffKeyMapObj2Stream - synchronized - end");
            } catch (Exception e) {
                logger.error("outputDiffKeyMapObj2Stream - Error [" + e.getMessage() + "]", e);
                this.diffDataMode(false);
                //blocking = true;
                //StatusUtil.setStatusAndMessage(1, "outputDiffKeyMapObj2Stream - Error [" + e.getMessage() + "]");
                //throw new BatchException(e);
            } finally {
                this.myOperationStatus = 1;
            }
        }
    }


    // 引数で渡されてストリームからの値でデータを作成する
    public void inputKeyMapObj2Stream(BufferedReader br, PrintWriter pw, int dataLineCount) throws BatchException {
        if (!blocking) {
            try {
                this.myOperationStatus = 2;
                int i = 0;
                String[] oneDatas = null;
                boolean setDataExec = false;
                logger.info("inputKeyMapObj2Stream - synchronized - start");

                synchronized(this.poolKeyLock) {
                    synchronized(this.lockWorkFileSync) {
                        // 事前に不要なファイルを削除

                        // WorkKeyMapファイルを消しこみ
                        // トランザクションログファイル
                        File workKeyMapObjFile = new File(this.workKeyFilePath);
                        if (workKeyMapObjFile.exists()) {
                            if (this.bw != null) {
                                this.bw.flush();
                                this.bw.close();
                            }

                            workKeyMapObjFile.delete();
                        }

                        // Disk時はデータファイルを削除
                        this.keyMapObj.clear();
                        if (this.dataMemory) {
                            if(this.keyMapObj != null) this.keyMapObj.close();
                        } else {
                            this.keyMapObj.deleteMapDataFile();
                        }

                        // KeyManagerValueMapのインスタンスを再作成
                        this.keyMapObj = null;

                        // 一旦フルGC
                        JavaSystemApi.manualGc();
                        Thread.sleep(3000); 

                        // 新たにKeyManagerValueMapを作成
                        if (!this.allDataForFile) {
                            this.keyMapObj = new KeyManagerValueMap(this.mapSize, this.dataMemory, this.virtualStorageDirs, true, null, this.diskCacheFile);
                        } else {
                            this.keyMapObj = new KeyManagerValueMap(this.keyFileDirs, this.mapSize, true, this.diskCacheFile);
                        }

                        if (!dataMemory) 
                            this.keyMapObj.initNoMemoryModeSetting(this.diskModeRestoreFile);


                        // WorkKeyMapファイル用のストリームを作成
                        FileOutputStream newFos = new FileOutputStream(new File(this.workKeyFilePath));
                        this.bw = new CustomBufferedWriter(new OutputStreamWriter(newFos , KeyMapManager.workMapFileEnc), 8192 * 24, newFos);
                        this.tLogWriteCount = new AtomicInteger(0);

                        if (this.workFileMemory == false && this.workFileFlushTiming == false) {
                            this.dataTransactionFileFlushDaemon.close();
                            this.dataTransactionFileFlushDaemon.tBw = this.bw;
                            this.dataTransactionFileFlushDaemon.tFilePath = this.workKeyFilePath;
                        }

                        // 開始時間を記録
                        long inputStartTime = JavaSystemApi.currentTimeMillis;

                        // 取り込み開始
                        long counter = 0;
                        for (int idx = 0; idx < Integer.MAX_VALUE; idx++) {

                            // 最終更新日付変えずに全てのデータを登録する
                            // ストリームからKeyMapの1ラインを読み込み、パース後1件づつ登録
                            String allDataStr = br.readLine();

                            if (allDataStr == null || allDataStr.trim().equals("-1")) {
                                logger.info("inputKeyMapObj2Stream ReadLine End");
                                break;
                            }

                            String[] allDataLines = allDataStr.split(ImdstDefine.imdstConnectAllDataSendDataSep);
                            allDataStr = null;

                            for (i = 0; i < allDataLines.length; i++) {
                                counter++;
                                if ((counter % 500) == 0) logger.info("inputKeyMapObj2Stream Input Count[" + counter + "]");

                                if (!allDataLines[i].trim().equals("")) {
                                    oneDatas = allDataLines[i].split(KeyMapManager.workFileSeq);
                                    if (oneDatas.length == 2) {

                                        setDataExec = true;
                                        this.keyMapObjPutNoChange(oneDatas[0], oneDatas[1]);

                                        if (this.workFileMemory == false) {

                                            this.bw.write("+" + KeyMapManager.workFileSeq + oneDatas[0] + KeyMapManager.workFileSeq + oneDatas[1] + KeyMapManager.workFileSeq + inputStartTime + KeyMapManager.workFileSeq + KeyMapManager.workFileEndPoint + "\n");
                                            this.checkTransactionLogWriterLimit(this.tLogWriteCount.incrementAndGet());
                                        }
                                    } else if (oneDatas.length == 3) {

                                        setDataExec = true;
                                        this.keyMapObjPutNoChange(oneDatas[0], oneDatas[1] + KeyMapManager.workFileSeq + oneDatas[2]);

                                        if (this.workFileMemory == false) {
                                            this.bw.write("+" + KeyMapManager.workFileSeq + oneDatas[0] + KeyMapManager.workFileSeq + oneDatas[1] + KeyMapManager.workFileSeq + oneDatas[2] + KeyMapManager.workFileSeq + inputStartTime + KeyMapManager.workFileSeq + KeyMapManager.workFileEndPoint + "\n");
                                            this.checkTransactionLogWriterLimit(this.tLogWriteCount.incrementAndGet());
                                        }
                                    } else {

                                    }
                                }
                            }

                            allDataLines = null;
                        }

                        // 全てのデータを取り込んだタイミングで最終更新時間を変更
                        // 1件でも取り込んでいる場合のみ
                        if (setDataExec == true) this.setLastDataChangeTime();
                        pw.println("1");
                        pw.flush();
                    }
                }
                logger.info("inputKeyMapObj2Stream - synchronized - end");

            } catch (Exception e) {
                try {
                    pw.println("-1");
                    pw.flush();
                } catch (Exception e2) {
                }
                logger.error("inputKeyMapObj2Stream - Error");
                blocking = true;
                StatusUtil.setStatusAndMessage(1, "inputKeyMapObj2Stream - Error [" + e.getMessage() + "]");
                throw new BatchException(e);
            }
        }
    }


    // 引数で渡されてストリームからの値でデータを作成する
    // 差分データの登録なので、データファイルの消しこみなどはせずに、追加で登録、削除していく
    public void inputDiffKeyMapObj2Stream(BufferedReader br, PrintWriter pw) throws BatchException {
        if (!blocking) {
            try {
                int i = 0;
                String[] oneDatas = null;
                logger.info("inputDiffKeyMapObj2Stream - synchronized - start");
                synchronized(this.poolKeyLock) {
                    synchronized(this.lockWorkFileSync) {
                        long writeCurrentTime = this.lastAccess;
                        int counter = 0;

                        // 最終更新日付変えずに全てのデータを登録する
                        // ストリームからKeyMapの1ラインを読み込み、パース後1件づつ登録
                        String allDataStr = null;
                        while((allDataStr = br.readLine()) != null) {

                            if (allDataStr.trim().equals("-1")) break;
                            if (allDataStr != null && !allDataStr.trim().equals("")) {

                                String[] allDataLines = allDataStr.split(ImdstDefine.imdstConnectAllDataSendDataSep);
                                allDataStr = null;

                                for (i = 0; i < allDataLines.length; i++) {
                                    if (!allDataLines[i].trim().equals("")) {
                                        oneDatas = allDataLines[i].split(KeyMapManager.workFileSeq);

                                        // 最後のデータのみ更新日を変更
                                        if (allDataLines.length == (i + 1)) {

                                            if (oneDatas[0].equals("+")) {

                                                if (oneDatas.length == 3) {
                                                    this.keyMapObjPut(oneDatas[1], oneDatas[2]);
                                                } else if (oneDatas.length == 4) {
                                                    this.keyMapObjPut(oneDatas[1], oneDatas[2] + KeyMapManager.workFileSeq + oneDatas[3]);
                                                }
                                            } else if (oneDatas[0].equals("-")) {

                                                this.keyMapObjRemove(oneDatas[1]);
                                            }
                                        } else {
                                            if (oneDatas[0].equals("+")) {

                                                if (oneDatas.length == 3) {
                                                    this.keyMapObjPutNoChange(oneDatas[1], oneDatas[2]);
                                                } else if (oneDatas.length == 4) {
                                                    this.keyMapObjPutNoChange(oneDatas[1], oneDatas[2] + KeyMapManager.workFileSeq + oneDatas[3]);
                                                }
                                            } else if (oneDatas[0].equals("-")) {

                                                this.keyMapObjRemoveNoChange(oneDatas[1]);
                                            }
                                        }

                                        // 差分データをトランザクションログモードがONの場合のみ書き出し
                                        // ファイルストリームは既にinputKeyMapObj2Streamメソッド内で作成されている想定
                                        if (this.workFileMemory == false) {

                                            counter++;
                                            String writeValueStr = " ";
                                            String dataType = "+";

                                            if (oneDatas[0].equals("-")) {

                                                dataType = "-";
                                            } else {

                                                if (oneDatas.length == 3) {
                                                    writeValueStr = oneDatas[2];
                                                } else if (oneDatas.length == 4) {
                                                    writeValueStr = oneDatas[2] + KeyMapManager.workFileSeq + oneDatas[3];
                                                }
                                            }

                                            this.bw.write(new StringBuilder(ImdstDefine.stringBufferMiddleSize).
                                                append(dataType).
                                                append(KeyMapManager.workFileSeq).
                                                append(oneDatas[1]).
                                                append(KeyMapManager.workFileSeq).
                                                append(writeValueStr).
                                                append(KeyMapManager.workFileSeq).
                                                append(writeCurrentTime).
                                                append(KeyMapManager.workFileSeq).
                                                append(KeyMapManager.workFileEndPoint).
                                                append("\n").
                                                toString());

                                            if((counter % 100) == 0) {
                                                SystemUtil.diskAccessSync(this.bw);
                                                this.checkTransactionLogWriterLimit(this.tLogWriteCount.incrementAndGet());
                                            }
                                        }
                                    }
                                }
                                if (this.workFileMemory == false) {
                                    SystemUtil.diskAccessSync(this.bw);
                                    this.checkTransactionLogWriterLimit(this.tLogWriteCount.incrementAndGet());
                                }
                                allDataLines = null;
                            }
                            pw.println("2");
                            pw.flush();
                        }

                        // トランザクションログモードがONの場合のみflush
                        if (this.workFileMemory == false) {
                            SystemUtil.diskAccessSync(this.bw);
                            this.checkTransactionLogWriterLimit(this.tLogWriteCount.incrementAndGet());
                        }

                        // 取り込み終了を送信
                        pw.println("1");
                        pw.flush();
                    }
                }
                logger.info("inputDiffKeyMapObj2Stream - synchronized - end");
                this.myOperationStatus = 1;
                
                // 起動時にリカバリが必要な指定が入っている場合もそれをoffとする
                if (ImdstDefine.recoverRequired == true) ImdstDefine.recoverRequired = false;
            } catch (Exception e) {
                this.myOperationStatus = 2;
                logger.error("inputDiffKeyMapObj2Stream - Error");
                blocking = true;
                StatusUtil.setStatusAndMessage(1, "inputDiffKeyMapObj2Stream - Error [" + e.getMessage() + "]");
                throw new BatchException(e);
            }
        }
    }


    // 引数で渡されてストリームに対しKey値を書き出す
    // 書き出すKey値は引数のrulesStrを使用して割り出した値がmatchNoとマッチしないデータ
    // 終了時は-1が返る
    public void outputNoMatchKeyMapKey2Stream(PrintWriter pw, int matchNo, String rulesStr) throws BatchException {
        if (!blocking) {
            try {
                String[] rules = null;
                int[] rulesInt = null;
                rules = rulesStr.split(",");
                rulesInt = new int[rules.length];

                for (int i = 0; i < rules.length; i++) {
                    rulesInt[i] = Integer.parseInt(rules[i]);
                }


                String allDataSep = "";
                StringBuilder allDataBuf = new StringBuilder(ImdstDefine.stringBufferLarge_3Size);

                // keyMapObjの全内容を1行文字列として書き出し
                Set entrySet = this.keyMapObj.entrySet();

                int printLineCount = 0;
                // 一度に送信するデータ量を算出。空きメモリの10%を使用する
                int maxLineCount = new Double((JavaSystemApi.getRuntimeFreeMem("") * 0.1) / ((ImdstDefine.saveKeyMaxSize * 1.38 + ImdstDefine.saveDataMaxSize * 1.38) / 50)).intValue();
                // 最大でも5万件以上を一度に送信はしない
                if (maxLineCount > 50000) maxLineCount = 50000;

                if (ImdstDefine.lowSpecDataNode) {
                    if (maxLineCount > ImdstDefine.lowSpecDataNodeSendDataCount) maxLineCount = ImdstDefine.lowSpecDataNodeSendDataCount;
                }

                //int maxLineCount = 500;
                if (entrySet.size() > 0) {
                    if(maxLineCount == 0) maxLineCount = 1;
                    printLineCount = new Double(entrySet.size() / maxLineCount).intValue();
                    if (entrySet.size() % maxLineCount > 0) {
                        printLineCount = printLineCount + 1;
                    }
                }


                Iterator entryIte = entrySet.iterator(); 

                int counter = 0;
                while(entryIte.hasNext()) {
                    Map.Entry obj = (Map.Entry)entryIte.next();
                    if (obj == null) continue;
                    String key = null;
                    String sendTagKey = null;
                    boolean sendFlg = true;
                    boolean tagFlg = false;

                    key = (String)obj.getKey();
                    if (key.indexOf(ImdstDefine.imdstTagStartStr) == 0) {
                        // タグの場合は分解して
                        tagFlg = true;
                        int startIdx = 15;
                        int endIdx = key.lastIndexOf(ImdstDefine.imdstTagEndStr);
         
                        String checkKey = key.substring(startIdx, endIdx);
                        sendTagKey = key.substring(startIdx, endIdx);

                        // プレフィックスを外すために位置確認
                        int lastIdx = checkKey.lastIndexOf("_");

                        // マッチするか確認
                        // タグの対象データ判定はタグ値に連結されているインデックス文字列や、左右のプレフィックス文字列をはずして判定する
                        for (int idx = 0; idx < rulesInt.length; idx++) {
                            if (DataDispatcher.isRuleMatchKey(checkKey.substring(0, lastIdx), rulesInt[idx], matchNo)) {
                                sendFlg = false;
                                break;
                            }
                        }

                    } else {

                        // マッチするか確認
                        for (int idx = 0; idx < rulesInt.length; idx++) {

                            if (DataDispatcher.isRuleMatchKey(key, rulesInt[idx], matchNo)) {
                                sendFlg = false;
                                break;
                            }
                        }
                    }

                    // 送信すべきデータのみ送る
                    if (sendFlg) {
                        SystemUtil.debugLine("outputNoMatchKeyMapKey2Stream - MoveTargetKey[" + key + "]");
                        String data = this.keyMapObjGet(key);

                        if (data != null) {
                            if (tagFlg) {

                                // タグ
                                // タグの場合はValue部分をレコードとしてばらして送る

                                String[] tagDatas = data.split(ImdstDefine.imdstTagKeyAppendSep);
                                for (int idx = 0; idx < tagDatas.length; idx++) {

                                    // タグの対象データのキーを送る場合はデータ転送後消しこむ際にインデックス番号が必要なので、
                                    // 左右のプレフィックス文字列は外すが、インデックス番号はつけたまま送る
                                    allDataBuf.append(allDataSep);
                                    allDataBuf.append("2");
                                    allDataBuf.append(KeyMapManager.workFileSeq);
                                    allDataBuf.append(sendTagKey);
                                    allDataBuf.append(KeyMapManager.workFileSeq);
                                    allDataBuf.append(tagDatas[idx]);
                                    allDataBuf.append(ImdstDefine.setTimeParamSep);
                                    allDataBuf.append("0");                         // 保存バージョンNoは0固定

                                    allDataSep = ImdstDefine.imdstConnectAllDataSendDataSep;
                                    counter++;
                                }
                            } else {

                                // 通常データ
                                allDataBuf.append(allDataSep);
                                allDataBuf.append("1");
                                allDataBuf.append(KeyMapManager.workFileSeq);
                                allDataBuf.append(key);
                                allDataBuf.append(KeyMapManager.workFileSeq);
                                allDataBuf.append(data);
                                allDataSep = ImdstDefine.imdstConnectAllDataSendDataSep;
                            }
                        }
                    }

                    counter++;

                    if (counter > (maxLineCount - 1)) {
                        pw.println(allDataBuf.toString());
                        allDataBuf = new StringBuilder(ImdstDefine.stringBufferLarge_3Size);
                        counter = 0;
                        allDataSep = "";
                    }
                }
                pw.println(allDataBuf.toString());
                pw.println("-1");
                pw.flush();
            } catch (Exception e) {
                e.printStackTrace();
                logger.error("outputNoMatchKeyMapKey2Stream - Error =[" + e.getMessage() + "]");
            }
        }
    }


    // 引数で渡されてストリームに対しKey値を書き出す
    // 書き出すKey値は引数のrulesStrを使用して割り出した値がmatchNoとマッチしないデータ
    // 終了時は-1が返る
    public void outputConsistentHashMoveData2Stream(PrintWriter pw, String targetRangStr) throws BatchException {
        if (!blocking) {
            try {

                String allDataSep = "";
                StringBuilder allDataBuf = new StringBuilder(ImdstDefine.stringBufferLarge_3Size);
                int counter = 0;

                // レンジデータ作成
                int[][] rangs = this.convertRangeData(targetRangStr);

                // keyMapObjの内容を1行文字列として書き出し
                Set entrySet = this.keyMapObj.entrySet();
                int printLineCount = 0;

                // 一度に送信するデータ量を算出。空きメモリの10%を使用する
                int maxLineCount = new Double((JavaSystemApi.getRuntimeFreeMem("") * 0.1) / ((ImdstDefine.saveKeyMaxSize * 1.38 + ImdstDefine.saveDataMaxSize * 1.38) / 50)).intValue();
                // 最大でも5万件以上を一度に送信はしない
                if (maxLineCount > 50000) maxLineCount = 50000;

                if (ImdstDefine.lowSpecDataNode) {
                    if (maxLineCount > ImdstDefine.lowSpecDataNodeSendDataCount) maxLineCount = ImdstDefine.lowSpecDataNodeSendDataCount;
                }

                //int maxLineCount = 500;
                if (entrySet.size() > 0) {
                    if (maxLineCount == 0) maxLineCount = 1;
                    printLineCount = new Double(entrySet.size() / maxLineCount).intValue();
                    if (entrySet.size() % maxLineCount > 0) {
                        printLineCount = printLineCount + 1;
                    }
                }


                // KeyMapObject内のデータを1件づつ対象になるか確認
                Iterator entryIte = entrySet.iterator(); 

                // キー値を1件づつレンジに含まれているか確認
                while(entryIte.hasNext()) {
                    if (this.outputDataStopSignal == true) {
                        logger.info("outputConsistentHashMoveData2Stream - Stop Signal");
                        break;
                    }

                    Map.Entry obj = (Map.Entry)entryIte.next();
                    if (obj == null) continue;
                    String key = null;
                    String sendTagKey = null;
                    boolean sendFlg = false;
                    boolean tagFlg = false;

                    // キー値を取り出し
                    key = (String)obj.getKey();

                    if (key.indexOf(ImdstDefine.imdstTagStartStr) == 0) {
                        // タグの場合は分解して
                        tagFlg = true;
                        int startIdx = 15;
                        int endIdx = key.lastIndexOf(ImdstDefine.imdstTagEndStr);

                        String checkKey = key.substring(startIdx, endIdx);
                        sendTagKey = key.substring(startIdx, endIdx);

                        // プレフィックスを外すために位置確認
                        int lastIdx = checkKey.lastIndexOf("_");

                        // 対象データ判定
                        // タグの対象データ判定はタグ値に連結されているインデックス文字列や、左右のプレフィックス文字列をはずして判定する
                        sendFlg = DataDispatcher.isRangeData(checkKey.substring(0, lastIdx), rangs);
                    } else if (key.indexOf(ImdstDefine.imdstListStructCommonPrefixStr) == 0) {
                        sendFlg = false;
                    } else {
                        // 対象データ判定
                        sendFlg = DataDispatcher.isRangeData(key, rangs);
                    }


                    // 送信すべきデータのみ送る
                    if (sendFlg) {
                        SystemUtil.debugLine("outputConsistentHashMoveData2Stream - MoveTargetKey[" + key + "]");

                        String data = this.keyMapObjGet(key);
                        if (data != null) {

                            if (tagFlg) {

                                // タグ
                                // タグの場合はValue部分をレコードとしてばらして送る

                                String[] tagDatas = data.split(ImdstDefine.imdstTagKeyAppendSep);
                                for (int idx = 0; idx < tagDatas.length; idx++) {

                                    if (tagDatas[idx].indexOf(ImdstDefine.setTimeParamSep) == -1) tagDatas[idx] = tagDatas[idx] + "!0";
                                    // 送信するTag値とTagのValue(実際のKey値群)
                                    //System.out.println(sendTagKey);
                                    //System.out.println(tagDatas[idx]);

                                    // タグの対象データのキーを送る場合はデータ転送後消しこむ際にインデックス番号が必要なので、
                                    // 左右のプレフィックス文字列は外すが、インデックス番号(TagKey_??)はつけたまま送る
                                    allDataBuf.append(allDataSep);
                                    allDataBuf.append("2");
                                    allDataBuf.append(KeyMapManager.workFileSeq);
                                    allDataBuf.append(sendTagKey);
                                    allDataBuf.append(KeyMapManager.workFileSeq);
                                    allDataBuf.append(tagDatas[idx]);
                                    allDataSep = ImdstDefine.imdstConnectAllDataSendDataSep;
                                    counter++;
                                }
                            } else {

                                // 通常データ
                                allDataBuf.append(allDataSep);
                                allDataBuf.append("1");
                                allDataBuf.append(KeyMapManager.workFileSeq);
                                allDataBuf.append(key);
                                allDataBuf.append(KeyMapManager.workFileSeq);
                                allDataBuf.append(data);
                                allDataSep = ImdstDefine.imdstConnectAllDataSendDataSep;
                            }
                        }
                    }

                    counter++;

                    if (counter > (maxLineCount - 1)) {

                        pw.println(allDataBuf.toString());
                        pw.flush();
                        if (ImdstDefine.lowSpecDataNode) Thread.sleep(50);
                        allDataBuf = new StringBuilder(ImdstDefine.stringBufferLarge_3Size);
                        counter = 0;
                        allDataSep = "";
                    }
                }

                pw.println(allDataBuf.toString());
                pw.println("-1");
                pw.flush();
            } catch (Exception e) {
                logger.error("outputConsistentHashMoveData2Stream - Error =[" + e.getMessage() + "]");
            }
        }
    }


    // 引数で渡されてストリームからの値をデータ登録する
    // この際、既に登録されているデータは登録しない
    public void inputNoMatchKeyMapKey2Stream(PrintWriter pw, BufferedReader br) throws BatchException {

        this.inputConsistentHashMoveData2Stream(pw, br);
    }


    // 引数で渡されてストリームからの値をデータ登録する
    // この際、既に登録されているデータは登録しない
    public void inputConsistentHashMoveData2Stream(PrintWriter pw, BufferedReader br) throws BatchException {

        if (!blocking) {
            try {

                this.moveAdjustmentDataMap = new ConcurrentHashMap(1024, 1000, 512);

                int i = 0;
                String[] oneDatas = null;


                // 最終更新日付変えずに全てのデータを登録する
                // ストリームからKeyMapの1ラインを読み込み、パース後1件づつ登録
                // 既に同一のキー値で登録データが存在する場合はそちらのデータを優先
                String dataStr = null;

                while(true) {
                    logger.info("inputConsistentHashMoveData2Stream - synchronized - start");
                    synchronized(this.poolKeyLock) {

                        dataStr = br.readLine();
                        if (dataStr == null || dataStr.equals("-1")) break;
                        String[] dataLines = dataStr.split(ImdstDefine.imdstConnectAllDataSendDataSep);

                        for (i = 0; i < dataLines.length; i++) {

                            if (!dataLines[i].trim().equals("")) {

                                // TODO:ここで移行データから有効期限よりも後ろを削除してしまっている。

                                oneDatas = dataLines[i].split(KeyMapManager.workFileSeq);

                                // データの種類に合わせて処理分岐
                                if (oneDatas[0].equals("1")) {

                                    // TODO:有効期限データはValueの後に、カンマ区切りで情報が結合されているため、
                                    // ここでoneDatasはレングスが4以上になる可能性がある
                                    
                                    // 通常データ
                                    // 成功、失敗関係なく全て登録処理
                                    if (oneDatas.length > 3) {
                                        oneDatas[2] = oneDatas[2] + "," + oneDatas[3];
                                    }
                                    this.setKeyPairOnlyOnce(oneDatas[1], oneDatas[2], "0", true);
                                } else if (oneDatas[0].equals("2")) {


                                    // Tagデータ
                                    // 通常通りタグとして保存
                                    // Tagデータはキー値にインデックス付きで送信されるので、インデックスを取り外す

                                    int lastIdx = oneDatas[1].lastIndexOf("_");
                                    oneDatas[1] = oneDatas[1].substring(0, lastIdx);

                                    this.setTagPair(oneDatas[1], oneDatas[2], "0");
                                }
                            }
                        }
                        pw.println("next");
                        pw.flush();
                    }
                    logger.info("inputConsistentHashMoveData2Stream - synchronized - end");
                }
                pw.println("end");
                pw.flush();
            } catch(SocketException se) {
                // 切断とみなす
                logger.error("", se);
            } catch (Exception e) {
                if (pw != null) {
                    try {
                        pw.println("error");
                        pw.flush();
                    } catch (Exception ee) {
                    }
                }
                logger.error("inputConsistentHashMoveData2Stream - Error");
                blocking = true;
                StatusUtil.setStatusAndMessage(1, "inputConsistentHashMoveData2Stream - Error [" + e.getMessage() + "]");
                throw new BatchException(e);
            } finally {
                synchronized (this.moveAdjustmentSync) {

                    // keyMapObjの内容を1行文字列として書き出し
                    Set entrySet = this.moveAdjustmentDataMap.entrySet();

                    // KeyMapObject内のデータを1件づつ対象になるか確認
                    Iterator entryIte = entrySet.iterator(); 

                    // キー値を1件づつレンジに含まれているか確認
                    while(entryIte.hasNext()) {
                        Map.Entry obj = (Map.Entry)entryIte.next();
                        if (obj == null) continue;
                        String key = null;

                        // キー値を取り出し
                        key = (String)obj.getKey();
                        // 削除
                        keyMapObjRemove(key);
                        
                    }
                    this.moveAdjustmentDataMap = null;
                }
            }
        }
    }


    // 移動対象のデータが移動完了した後に削除するために呼び出す
    // 終了時は-1が返る
    public void removeConsistentHashMoveData2Stream(PrintWriter pw, String targetRangStr) throws BatchException {
        if (!blocking) {
            try {
                // レンジデータ作成
                int[][] rangs = this.convertRangeData(targetRangStr);

                // keyMapObjの内容を1行文字列として書き出し
                Set entrySet = this.keyMapObj.entrySet();

                // KeyMapObject内のデータを1件づつ対象になるか確認
                Iterator entryIte = entrySet.iterator(); 

                // キー値を1件づつレンジに含まれているか確認
                while(entryIte.hasNext()) {
                    Map.Entry obj = (Map.Entry)entryIte.next();
                    if (obj == null) continue;
                    String key = null;

                    // キー値を取り出し
                    key = (String)obj.getKey();

                    if (key.indexOf(ImdstDefine.imdstTagStartStr) == 0) {
                        // タグの場合は分解して
                        int startIdx = 15;
                        int endIdx = key.lastIndexOf(ImdstDefine.imdstTagEndStr);

                        String checkKey = key.substring(startIdx, endIdx);

                        // プレフィックスを外すために位置確認
                        int lastIdx = checkKey.lastIndexOf("_");

                        // 対象データ判定
                        // タグの対象データ判定はタグ値に連結されているインデックス文字列や、左右のプレフィックス文字列をはずして判定する
                        if(DataDispatcher.isRangeData(checkKey.substring(0, lastIdx), rangs)) this.removeKeyPair(key, "0");
                    } else {
                        // 対象データ判定
                        if(DataDispatcher.isRangeData(key, rangs)) this.removeKeyPair(key, "0");
                    }
                }

                pw.println("-1");
                pw.flush();
            } catch (Exception e) {
                if (pw != null) {
                    try {
                        pw.println("error");
                        pw.flush();
                    } catch (Exception ee) {
                    }
                }
                logger.error("removeConsistentHashMoveData2Stream - Error =[" + e.getMessage() + "]");
            }
        }
    }


    public void removeModMoveData2Stream(PrintWriter pw, BufferedReader br) throws BatchException {
        if (!blocking) {
            try {
                int i = 0;
                String[] oneDatas = null;


                // 最終更新日付変えずに全てのデータを登録する
                // ストリームからKeyMapの1ラインを読み込み、パース後1件づつ登録
                // 既に同一のキー値で登録データが存在する場合はそちらのデータを優先
                String dataStr = null;

                while(true) {
                    logger.info("removeModMoveData2Stream - synchronized - start");

                    dataStr = br.readLine();
                    if (dataStr == null || dataStr.equals("-1")) break;

                    oneDatas = dataStr.split(KeyMapManager.workFileSeq);

                    if (oneDatas[0].equals("1")) {
                        // 通常データ
                        removeKeyPair(oneDatas[1], "0");
                    } else if (oneDatas[0].equals("2")) {
                        // タグ
                        removeKeyPair(KeyMapManager.tagStartStr + oneDatas[1] + KeyMapManager.tagEndStr, "0");
                    }
                    pw.println("next");
                    pw.flush();

                    logger.info("inputConsistentHashMoveData2Stream - synchronized - end");
                }

            } catch(SocketException se) {
                // 切断とみなす
                logger.error("", se);
            } catch (Exception e) {
                if (pw != null) {
                    try {
                        pw.println("error");
                        pw.flush();
                    } catch (Exception ee) {
                    }
                }
                logger.error("inputConsistentHashMoveData2Stream - Error");
                blocking = true;
                StatusUtil.setStatusAndMessage(1, "inputConsistentHashMoveData2Stream - Error [" + e.getMessage() + "]");
                throw new BatchException(e);
            }
        }
    }


    private void checkTransactionLogWriterLimit(int nowCount) {
        if (nowCount > ImdstDefine.maxTransactionLogBufferUseCount) {
            try {
                this.bw.flush();
                this.bw.close();
                this.bw = null;
            } catch (Exception e) {
                this.bw = null;
            } finally {
                try {

                    FileOutputStream newFos = new FileOutputStream(new File(this.workKeyFilePath), true);
                    this.bw = new CustomBufferedWriter(new OutputStreamWriter(newFos , KeyMapManager.workMapFileEnc), 8192 * 24, newFos);
                    this.tLogWriteCount = new AtomicInteger(0);
                } catch (Exception e) {
                    this.bw = null;
                }
            }
        }
    }

    // ConsistentHash時のデータ移動用レンジ用配列作成
    private int[][] convertRangeData(String rangsStr) {
        String[] targetRangs = rangsStr.split("_");
        int[][] rangs = new int[targetRangs.length][2];

        // レンジのstartとendをセット単位でintの配列に落とす
        for (int ii = 0; ii < targetRangs.length; ii++) {

            String[] workRangs = targetRangs[ii].split("-");
            rangs[ii][0] = Integer.parseInt(workRangs[0]);
            rangs[ii][1] = Integer.parseInt(workRangs[1]);
        }
        return rangs;
    }


    // データの最終更新時間を返す
    public long getLastDataChangeTime() {
        return this.keyMapObj.getKLastDataChangeTime();
    }

    // 格納データ数を返す
    public int getSaveDataCount() {
        return this.keyMapObj.size();
    }

    // 格納データのサイズを返す
    // ユニークキー指定あり
    public long getSaveDataSize(String uniqueKey) {
        return this.keyMapObj.getDataUseSize(uniqueKey);
    }

    // 格納データのサイズのユニークキー別の全てのサイズを返す
    public String[] getAllSaveDataSize() {
        return this.keyMapObj.getAllDataUseSize();
    }

    // 自身のステータスがエラーでないかを返す
    public boolean checkError() {
        return this.blocking;
    }


    // 有効期限切れデータの場合はtrueが返る
    private static boolean isExpireData(String valStr) {
        String[] checkValueSplit = null;

        if (valStr != null && valStr.length() <= 1000) {

            String[] valStrSplit = valStr.split(ImdstDefine.setTimeParamSep);
            valStr = valStrSplit[0];
            checkValueSplit = valStr.split(ImdstDefine.keyHelperClientParamSep);
        } else if (valStr != null && valStr.length() >= 1001){

            if (valStr.indexOf(ImdstDefine.keyHelperClientParamSep, (valStr.length() - 100)) != -1) {
                String[] valStrSplit = valStr.split(ImdstDefine.setTimeParamSep);
                valStr = valStrSplit[0];
                checkValueSplit = valStr.split(ImdstDefine.keyHelperClientParamSep);
            }
        } else {
            return false;
        }

        // 有効期限チェックを行う
        if (checkValueSplit != null && checkValueSplit.length > 1) {

            String[] metaColumns = checkValueSplit[1].split(ImdstDefine.valueMetaColumnSep);
            if (!SystemUtil.expireCheck(metaColumns[1], 1)) {

                return true;
            }
        }
        return false;
    }


    public void dump() {
        try {
            System.out.println("-------------------------------------- Dump Start ------------------------------------");
            System.out.println("ALL Data Count = [" + this.getSaveDataCount() + "]");
            System.out.println("======================================================================================");
            // keyMapObjの内容を1行文字列として書き出し
            Set entrySet = this.keyMapObj.entrySet();

            // KeyMapObject内のデータを1件づつ対象になるか確認
            Iterator entryIte = entrySet.iterator(); 

            // キー値を1件づつレンジに含まれているか確認
            while(entryIte.hasNext()) {
                Map.Entry obj = (Map.Entry)entryIte.next();
                if (obj == null) continue;
                String key = null;

                // キー値を取り出し

                key = (String)obj.getKey();


                if (key.indexOf(ImdstDefine.imdstTagStartStr) == 0) {

                    String tag = key;
                    int startIdx = 15;
                    int endIdx = key.lastIndexOf(ImdstDefine.imdstTagEndStr);
     
                    key = key.substring(startIdx, endIdx);

                    // プレフィックスを外すために位置確認
                    int lastIdx = key.lastIndexOf("_");

                    key = key.substring(0, lastIdx);

                    System.out.println("Tag=[" + new String(BASE64DecoderStream.decode(key.getBytes())) + "], Value=[" + this.keyMapObjGet(tag) + "]");

                } else {
                    System.out.println("Key=[" + new String(BASE64DecoderStream.decode(key.getBytes())) + "], Value=[" + this.keyMapObjGet(key) + "]");
                }
            }
            System.out.println("-------------------------------------- Dump End --------------------------------------");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * メモリ上のデータのみバックアップファイルにストア
     */
    public int keyObjectExport(String filePath) {

        if (this.keyObjBkupMode || filePath != null) {
            synchronized(this.keyObjectExportSync) {
                try {
                    File storeFile = null;
                    if (filePath != null) {
                        storeFile = new File(filePath);
                    } else {
                        storeFile = new File(keyObjBkupFilePath);
                    }
                    this.keyMapObj.fileStoreMapObject(storeFile);
                    return 1;
                } catch (Exception e) {
                    e.printStackTrace();
                    return -9;
                }
            }
        } else {
            return -1;
        }
    }

    /**
     * データバックアップ用
     */
    public void dataExport(PrintWriter pw, BufferedReader br, Socket soc) {
        try {
            int checkTime = 1;
            long waitTime = 0;

            long networkCheckCount = 1000;
            long count = 0;
            int maxOutPutCount = 5;

            // クライアントから設定値を取り出し
            try {
                soc.setSoTimeout(1000);
                String confStr = br.readLine();
                if (confStr != null && !confStr.trim().equals("")) {
                    String[] confStrList = confStr.split(",");
                    checkTime = Integer.parseInt(confStrList[0]);
                    waitTime = Long.parseLong(confStrList[1]);
                }
            } catch(Exception ee) {}

            // 全データをトランザクションログ形式でネットワークに書き出す
            Set entrySet = this.keyMapObj.entrySet();

            // KeyMapObject内のデータを1件づつ対象になるか確認
            Iterator entryIte = entrySet.iterator(); 

            // キー値を1件づつレンジに含まれているか確認
            while(entryIte.hasNext()) {
                count++;
                Map.Entry obj = (Map.Entry)entryIte.next();
                if (obj == null) continue;
                String key = null;

                // キー値を取り出し

                key = (String)obj.getKey();
                String value = this.keyMapObjGet(key);
                pw.println("+" + KeyMapManager.workFileSeq + key + KeyMapManager.workFileSeq + value + KeyMapManager.workFileSeq + JavaSystemApi.currentTimeMillis + KeyMapManager.workFileSeq + KeyMapManager.workFileEndPoint);
                if ((count % maxOutPutCount) == 0) pw.flush();

                if ((count % networkCheckCount) == 0) {
                    try {
                        soc.setSoTimeout(checkTime);
                        br.read();
                        br.reset(); 
                    } catch (SocketTimeoutException se) {
                    }
                }
                if (waitTime != 0) Thread.sleep(waitTime);
            }
            pw.flush();

        } catch (Exception e) {
            //e.printStackTrace();
        } finally {
            try {

                pw.println("-1");
                pw.flush();
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
    }

    public void testSearch(String val){
        try {
            Set entrySet = this.keyMapObj.entrySet();

            // KeyMapObject内のデータを1件づつ対象になるか確認
            Iterator entryIte = entrySet.iterator(); 

            // キー値を1件づつレンジに含まれているか確認
            List retList = new ArrayList();

            while(entryIte.hasNext()) {

                Map.Entry obj = (Map.Entry)entryIte.next();
                if (obj == null) continue;

                // キー値を取り出し
                String key = (String)obj.getKey();
                if (key != null && key.indexOf("imdst_tag#9641") == -1) {

                    String value = this.keyMapObjGet(key);
                    if (value != null) {
                        String[] valwork = value.split("!");
                        String decodeVal = new String(BASE64DecoderStream.decode(valwork[0].getBytes()));
//                        String decodeVal = valwork[0];

                        if (decodeVal.indexOf(val) != -1) {
                            retList.add(key);
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

class DataTransactionFileFlushDaemon extends Thread {

    private volatile ArrayBlockingQueue delayWriteQueue = new ArrayBlockingQueue(4096);

    public volatile boolean execFlg = true;

    public volatile String tFilePath = null;
    
    public volatile BufferedWriter tBw= null;

    public Object daemonSyncObj = new Object();

    public volatile boolean executeEnd = false;

    public void run() {
        int writeCount = 0;
        String writeStr = null;
        int bufferUseCount = 0;
        int maxBufferUseCount = 1000000;

        while (this.execFlg) {

            try {
                if (writeStr == null) {
                    StringBuilder strBuf = new StringBuilder();
                    for (int i = 0; i < 10; i++) {
                        String tmp = (String)this.delayWriteQueue.poll(200L, TimeUnit.MILLISECONDS);
                        if (tmp != null) strBuf.append(tmp);
                    }
                    
                    writeStr = strBuf.toString();
                    if (writeStr == null || writeStr.equals("")) writeStr = null;
                }

                synchronized (daemonSyncObj) {
                    if (this.tBw != null && writeStr != null) {
                        this.tBw.write(writeStr);
                        SystemUtil.diskAccessSync(this.tBw);
                        writeStr = null;
                        /*bufferUseCount++;
                        if (bufferUseCount > maxBufferUseCount) {
                            this.tBw.flush();
                            this.tBw.close();
                            this.tBw = null;
                            this.tBw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(this.tFilePath), true) , KeyMapManager.workMapFileEnc), 8192 * 24);
                            bufferUseCount = 0;
                        }*/
                    }
                }
            } catch (Throwable te) {
                te.printStackTrace();
            } 
        }
        this.executeEnd = true;
    }


    public ArrayBlockingQueue getDataTransactionFileQueue() {
        return this.delayWriteQueue;
    }

    public void setDataTransactionFileQueue(ArrayBlockingQueue queue) {
        this.delayWriteQueue = queue;
    }

    public boolean getExecuteEnd() {
        return this.executeEnd;
    }

    public void addDataTransaction(String str) {
        while (true) {
            try {
                this.delayWriteQueue.put(str);
                break;
            } catch (Throwable te) {
            }
        }
    }


    public void close() {
        synchronized (daemonSyncObj) {
            if (this.tBw != null) {
                try {
                    SystemUtil.diskAccessSync(this.tBw);
                } catch (Throwable te) {
                    te.printStackTrace();
                } finally {
                    try {
                        this.tBw.close();
                    } catch (Throwable te2) {
                        te2.printStackTrace();
                    }
                    this.tBw = null;
                    this.tFilePath = null;
                }
            }
        }
    }
}