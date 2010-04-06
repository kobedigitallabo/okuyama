package org.imdst.util;

import java.util.*;
import java.io.*;

import org.batch.util.ILogger;
import org.batch.util.LoggerFactory;
import org.batch.lang.BatchException;
import org.imdst.util.StatusUtil;

/**
 * DataNodeが使用するKey-Valueを管理するモジュール.<br>
 * データの定期的なファイルストア、登録ログの出力、同期化を行う.<br>
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
    private static final String tagEndStr = ImdstDefine.imdstTagEndStr;
    private static final String tagKeySep = ImdstDefine.imdstTagKeyAppendSep;


    // Key系の書き込み、取得
    private Object poolKeyLock = new Object();
    private Object getKeyLock = new Object();

    // Tag系の書き込み、取得
    private Object setTagLock = new Object();
    private Object getTagLock = new Object();

    private String keyFilePath = null;
    private String keyFileTmpPath = null;
    private String workKeyFilePath = null;

    FileOutputStream fos = null;
    OutputStreamWriter osw = null;
    BufferedWriter bw = null;

    // 本クラスへのアクセスブロック状態
    private boolean blocking = false;
    // 本クラスの初期化状態
    private boolean initFlg  = false;

    // Mapファイルを書き込む必要有無
    private boolean writeMapFileFlg = false;

    // TODO:Mapファイル本体を保存しないように一時的に変更updateInterval=30秒
    // 起動時にトランザクションログから復旧
    // Mapファイル本体を更新する時間間隔(ミリ秒)(時間間隔の合計 = updateInterval × intervalCount)
    private static int updateInterval = 3000;
    private static int intervalCount = 10;

    // workMap(トランザクションログ)ファイルのデータセパレータ文字列
    private static String workFileSeq = ImdstDefine.keyWorkFileSep;

    // workMap(トランザクションログ)ファイルの文字コード
    private static String workMapFileEnc = ImdstDefine.keyWorkFileEncoding;

    // workMap(トランザクションログ)ファイルのデータセパレータ文字列
    private static String workFileEndPoint = ImdstDefine.keyWorkFileEndPointStr;

    // workMap(トランザクションログ)ファイルをメモリーモードにするかの指定
    private boolean workFileMemory = false;

    // データのメモリーモードかファイルモードかの指定
    private boolean dataMemory = true;

    // Diskモード(ファイルモード)で稼動している場合のデータファイル名
    private String diskModeRestoreFile = null;

    // データへの最終アクセス時間
    private long lastAccess = 0L;

    // データファイルのバキューム実行指定
    private boolean vacuumExec = true;

    // Key値の数とファイルの行数の差がこの数値を超えるとvacuumを行う
    // 行数と1行のデータサイズをかけると不要なデータサイズとなる
    // vacuumStartLimit × (ImdstDefine.saveDataMaxSize * 1.38) = 不要サイズ
    private int vacuumStartLimit = 20000;

    // Vacuum実行時に事前に以下のミリ秒の間アクセスがないと実行許可となる
    private int vacuumExecAfterAccessTime = 30000;

	// データを管理するか、Transaction情報を管理するかを決定
	private boolean dataManege = true;

	// Lockの開始時間の連結文字列
    private String lockKeyTimeSep = "_";


    // 初期化メソッド
	// Transactionを管理する場合に呼び出す
    public KeyMapManager(String keyMapFilePath, String workKeyMapFilePath, boolean workFileMemory, int keySize, boolean dataMemory, boolean dataManage) throws BatchException {
		this(keyMapFilePath, workKeyMapFilePath, workFileMemory, keySize, dataMemory);
		this.dataManege = dataManage;
	}

    // 初期化メソッド
    public KeyMapManager(String keyMapFilePath, String workKeyMapFilePath, boolean workFileMemory, int keySize, boolean dataMemory) throws BatchException {
        logger.debug("init - start");

        if (!initFlg) {
            initFlg = true;
            this.workFileMemory = workFileMemory;
            this.dataMemory = dataMemory;
            this.mapSize = keySize;
            this.keyFilePath = keyMapFilePath;
            this.keyFileTmpPath = keyMapFilePath + ".tmp";
            this.workKeyFilePath = workKeyMapFilePath;

            FileInputStream keyFilefis = null;
            ObjectInputStream keyFileois = null;

            FileInputStream workKeyFilefis = null;
            InputStreamReader isr = null;
            ObjectInputStream workKeyFileois = null;

            FileReader fr = null;
            BufferedReader br = null;
            String line = null;
            String[] workSplitStrs = null;

            // Diskモード時のファイルパス作成
            if (!this.dataMemory) {
                this.diskModeRestoreFile = keyMapFilePath + ".data";
            }

            synchronized(poolKeyLock) {
                try {
                    File keyFile = new File(this.keyFilePath);
                    File tmpKeyFile = new File(this.keyFileTmpPath);

                    File workKeyFile = new File(this.workKeyFilePath);

                    // Mapファイルを読み込む必要の有無
                    boolean mapFileRead = true;

                    // tmpMapファイルがある場合はMapファイルへの変更中に前回エラーの可能性があるので読み込む
                    if (tmpKeyFile.exists()) {
                        logger.info("tmpKeyMapFile - Read - start");
                        mapFileRead = false;
                        keyFilefis = new FileInputStream(tmpKeyFile);
                        keyFileois = new ObjectInputStream(keyFilefis);
                        try {
                            if (this.keyMapObj != null) this.keyMapObj.close();
                            this.keyMapObj = (KeyManagerValueMap)keyFileois.readObject();
                            if (!dataMemory) {
                                this.keyMapObj.initNoMemoryModeSetting(this.diskModeRestoreFile);
                            }
                            keyFileois.close();
                            keyFilefis.close();

                            // 古いKeyMapファイルを消しこみ
                            File keyMapObjFile = new File(this.keyFilePath);
                            if (keyMapObjFile.exists()) {
                                keyMapObjFile.delete();
                                keyMapObjFile = null;
                            }
                        // 一時KeyMapファイルをKeyMapファイル名に変更
                        tmpKeyFile.renameTo(new File(this.keyFilePath));
                        logger.info("tmpKeyMapFile - Read - end");
                        } catch (Exception we) {
                            logger.error("tmpKeyMapFile - Read - Error", we);
                            // workKeyファイル読み込み失敗
                            mapFileRead = true;
                            keyFileois = null;
                            keyFilefis = null;

                            if (this.keyMapObj != null) this.keyMapObj.close();
                            this.keyMapObj = null;
                            // tmpKeyMapファイルを消しこみ
                            tmpKeyFile.delete();
                            logger.info("tmpKeyMapFile - Delete - End");
                        }
                    }

                    // KeyMapファイルが存在する場合は読み込み
                    if (mapFileRead = true && keyFile.exists()) {
                        logger.info("KeyMapFile - Read - start");

                        keyFilefis = new FileInputStream(keyFile);
                        keyFileois = new ObjectInputStream(keyFilefis);

                        if (this.keyMapObj != null) this.keyMapObj.close();
                        this.keyMapObj = (KeyManagerValueMap)keyFileois.readObject();
                        if (!dataMemory) {
                            this.keyMapObj.initNoMemoryModeSetting(this.diskModeRestoreFile);
                        }

                        keyFileois.close();
                        keyFilefis.close();
                        logger.info("KeyMapFile - Read - end");
                    } else {
                        logger.info("KeyMapFile - No Exists");
                        // 存在しない場合はMapを作成

                        if (this.keyMapObj != null) this.keyMapObj.close();
                        this.keyMapObj = new KeyManagerValueMap(this.mapSize);
                        if (!dataMemory) {
                            this.keyMapObj.initNoMemoryModeSetting(this.diskModeRestoreFile);
                        }

                    }


                    // WorkKeyMapファイルが存在する場合は読み込み
                    if (workKeyFile.exists()) {
                        logger.info("workKeyMapFile - Read - start");
                        workKeyFilefis = new FileInputStream(workKeyFile);
                        isr = new InputStreamReader(workKeyFilefis , workMapFileEnc);
                        br = new BufferedReader(isr);
                        int counter = 1;
                        while((line=br.readLine())!=null){
                            if (!line.equals("")) {
                                workSplitStrs = line.split(workFileSeq);

                                // データは必ず5つに分解できる
                                if (workSplitStrs.length == 5) {
                                    // 登録データ
                                    if (workSplitStrs[0].equals("+")) {

                                        // トランザクションファイルからデータ登録操作を復元する。その際に登録実行時間もファイルから復元
                                        keyMapObjPutSetTime(new Integer(workSplitStrs[1]), workSplitStrs[2], new Long(workSplitStrs[3]).longValue());
                                    } else if (workSplitStrs[0].equals("-")) {

                                        // トランザクションファイルからデータ削除操作を復元する。その際に削除実行時間もファイルから復元
                                        keyMapObjRemoveSetTime(new Integer(workSplitStrs[1]), new Long(workSplitStrs[3]).longValue());
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

                    this.fos = new FileOutputStream(new File(this.workKeyFilePath), true);
                    this.osw = new OutputStreamWriter(fos , workMapFileEnc);
                    this.bw = new BufferedWriter(osw);
                    this.bw.newLine();
                    this.bw.flush();
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
     * 定期的にKeyMapを再保存する.<br>
     * システム停止要求を監視して停止依頼があった場合は自身を終了する.<br>
     *
     */ 
    public void run (){
        while(true) {

            if (StatusUtil.getStatus() != 0) {
                logger.info ("KeyMapManager - run - System Shutdown 1");
                break;
            }

            try {
                // 1サイクル30秒の停止を規定回数行う(途中で停止要求があった場合は無条件で処理実行)
                for (int count = 0; count < intervalCount; count++) {

                    // システム停止要求を監視
                    if (StatusUtil.getStatus() != 0) {
                        logger.info ("KeyMapManager - run - System Shutdown 2");
                        break;
                    }

					if (!this.dataManege) {
	                    this.autoLockRelease(System.currentTimeMillis());
					}
                    Thread.sleep(updateInterval);
                }

                logger.info("VacuumCheck - Start");
                //  Vacuum実行の確認
                // データがメモリーではなくかつ、vacuum実行指定がtrueの場合
                if (!dataMemory && vacuumExec == true) {
                    logger.info("vacuumCheck - Start - 1");
                    if ((this.keyMapObj.getAllDataCount() - this.keyMapObj.getKeySize()) > this.vacuumStartLimit) {
                        logger.info("VacuumCheck - Start - 2");

                        // 規定時間アクセスがない
                        if ((System.currentTimeMillis() - this.lastAccess) > this.vacuumExecAfterAccessTime) {
                            logger.info("Vacuum - Start");
                            long vacuumStart = System.currentTimeMillis();

                            synchronized(poolKeyLock) {
                                synchronized(this.getKeyLock) {
                                    this.keyMapObj.vacuumData();
                                    this.keyMapObj.initNoMemoryModeSetting(this.diskModeRestoreFile);
                                }
                            }

                            long vacuumEnd = System.currentTimeMillis();
                            logger.info("Vacuum - End - VacuumTime [" + (vacuumEnd - vacuumStart) +"] Milli Second");
                        }
                    }
                }
                logger.info("VacuumCheck - End");

                /*
                if (!blocking) {
                    if (this.writeMapFileFlg) {
                        synchronized(poolKeyLock) {
                            
                            logger.debug("run - synchronized - start");
                            // データ格納場所記述ファイル再保存

                            // 一時KeyMapファイルに書き込み
                            File tmpKeyFile = new File(this.keyFileTmpPath);
                            FileOutputStream oF = new FileOutputStream(tmpKeyFile);
                            ObjectOutputStream oO = new ObjectOutputStream(oF);
                            // キャッシュ情報を保持したDataSetをファイル書き込み
                            oO.writeObject(this.keyMapObj);
                            oO.close();
                            oF.close();

                            // 古いKeyMapファイルを消しこみ
                            File keyMapObjFile = new File(this.keyFilePath);
                            if (keyMapObjFile.exists()) {
                                keyMapObjFile.delete();
                                keyMapObjFile = null;
                            }

                            // 一時KeyMapファイルをKeyMapファイル名に変更
                            tmpKeyFile.renameTo(new File(this.keyFilePath));

                            // WorkKeyMapファイルを消しこみ
                            File workKeyMapObjFile = new File(this.workKeyFilePath);
                            if (workKeyMapObjFile.exists()) {

                                this.bw.close();
                                this.osw.close();
                                this.fos.close();
                                workKeyMapObjFile.delete();
                            }

                            // WorkKeyMapファイル用のストリームを作成
                            this.fos = new FileOutputStream(new File(this.workKeyFilePath));
                            this.osw = new OutputStreamWriter(fos , workMapFileEnc);
                            this.bw = new BufferedWriter(osw);

                            this.writeMapFileFlg = false;
                            //logger.debug("run - synchronized - end");
                        }
                    }
                }*/
            } catch (Exception e) {
                logger.error("KeyMapManager - run - Error" + e);
                blocking = true;
                StatusUtil.setStatusAndMessage(1, "KeyMapManager - run - Error [" + e.getMessage() + "]");
                e.printStackTrace();
            }   
        }
    }

    /**
     * キーを指定することでノードをセットする.<br>
     *
     * @param key キー値
     * @param keyNode Value値
     * @param transactionCode 
     */
    public void setKeyPair(Integer key, String keyNode, String transactionCode) throws BatchException {
        if (!blocking) {
            try {
                synchronized(poolKeyLock) {

                    //logger.debug("setKeyPair - synchronized - start");
                    synchronized(this.getKeyLock) {

                        keyMapObjPut(key, keyNode);
                    }

                    // データの書き込みを指示
                    this.writeMapFileFlg = true;

                    if (workFileMemory == false) {

                        // データ格納場所記述ファイル再保存
                        this.bw.write(new StringBuffer("+").append(workFileSeq).append(key.toString()).append(workFileSeq).append(keyNode).append(workFileSeq).append(new Date().getTime()).append(workFileSeq).append(workFileEndPoint).toString());
                        this.bw.newLine();
                        this.bw.flush();
                    }
                    //logger.debug("setKeyPair - synchronized - end");
                }
            } catch (Exception e) {
                e.printStackTrace();
                logger.error("setKeyPair - Error");
                blocking = true;
                StatusUtil.setStatusAndMessage(1, "setKeyPair - Error [" + e.getMessage() + "]");
                throw new BatchException(e);
            }
        }
    }

    // キーを指定することでノードを返す
    public String getKeyPair(Integer key) {
        String ret = null;
        if (!blocking) {
            synchronized(this.getKeyLock) {
                ret =  (String)keyMapObjGet(key);
            }
        }
        return ret;
    }

    // キーを指定することでノードを削除する
    public String removeKeyPair(Integer key, String transactionCode) throws BatchException {
        String ret = null;
        if (!blocking) {
            try {
                synchronized(this.getKeyLock) {
                    ret =  (String)keyMapObjGet(key);
                    keyMapObjRemove(key);

                    // データの書き込みを指示
                    this.writeMapFileFlg = true;

                    if (workFileMemory == false) {

                        // データ格納場所記述ファイル再保存(登録と合わせるために4つに分割できるようにする)
                        this.bw.write(new StringBuffer("-").append(workFileSeq).append(key.toString()).append(workFileSeq).append(" ").append(workFileSeq).append(new Date().getTime()).append(workFileSeq).append(workFileEndPoint).toString());
                        this.bw.newLine();
                        this.bw.flush();
                    }
                }
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
    public void setTagPair(Integer tag, String key, String transactionCode) throws BatchException {

        if (!blocking) {

            try {
                synchronized(this.setTagLock) {

                    String keyStrs = null;
                    int counter = 0;
                    boolean appendFlg = true;
                    Integer tagCnv = null;
                    Integer lastTagCnv = null;
                    int dataPutCounter = 0;
                    boolean firsrtRegist = true;

                    while (true) {

                        tagCnv = new Integer((tagStartStr + tag + "_" + new Integer(counter).toString() + tagEndStr).hashCode());

                        if (this.containsKeyPair(tagCnv)) {
                            firsrtRegist = false;
                            keyStrs = this.getKeyPair(tagCnv);

                            if (keyStrs.indexOf(key) != -1) {

                                // 既に登録済み
                                appendFlg = false;
                                break;
                            }
                        } else {

                            // Tag値のデータそのものがないもしくは、登録連番の中にはデータがない
                            if (counter > 0) {
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

                            // 既に別のKeyが登録済みなので、そのキーにアペンドしても良いかを確認
                            if ((keyStrs.getBytes().length + tagKeySep.getBytes().length + key.getBytes().length) >= ImdstDefine.saveDataMaxSize) {

                                // 既にキー値が最大のサイズに到達しているので別のキーを生み出す
                                counter++;

                                tagCnv = new Integer((tagStartStr + tag + "_" + (dataPutCounter + 1) + tagEndStr).hashCode());
                                this.setKeyPair(tagCnv, key, transactionCode);
                            } else{

                                // アペンド
                                tagCnv = new Integer((tagStartStr + tag + "_" + dataPutCounter + tagEndStr).hashCode());

                                keyStrs = keyStrs + tagKeySep + key;
                                this.setKeyPair(tagCnv, keyStrs, transactionCode);
                            }
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("setTagPair - Error");
                blocking = true;
                StatusUtil.setStatusAndMessage(1, "setTagPair - Error [" + e.getMessage() + "]");
                throw new BatchException(e);
            }
        }
    }

    // Tagを指定することでKeyリストを返す
    public String getTagPair(Integer tag) {
        String keyStrs = "";
        boolean isMatch = false;
        StringBuffer tmpBuf = new StringBuffer();
        String tmpStr = null;
        String tmpSep = "";

        if (!blocking) {

            int counter = 0;
            // Tagのキー値を連結
            while(true) {

                Integer tagCnv = new Integer((tagStartStr + tag + "_" + counter + tagEndStr).hashCode());

                if (this.containsKeyPair(tagCnv)) {

                    tmpStr = (String)this.getKeyPair(tagCnv);

                    if (tmpStr != null) {

                        isMatch = true;
                        tmpBuf.append(tmpSep);
                        tmpBuf.append(tmpStr);
                        tmpSep = tagKeySep;
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
        }
        return keyStrs;
    }


    // キーを指定することでキーが存在するかを返す
    public boolean containsKeyPair(Integer key) {
        boolean ret = false;
        if (!blocking) {
            synchronized(this.getKeyLock) {
                ret =  this.keyMapObj.containsKey(key);
            }
        }
        return ret;
    }

    // キーを指定することでキーが存在するかを返す
    public boolean containsTagPair(Integer tag) {
        boolean ret = false;
        if (!blocking) {
            Integer tagCnv = new Integer((tagStartStr + tag + "_0" + tagEndStr).hashCode());

            ret =  this.containsKeyPair(tagCnv);
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
    public String locking (Integer key, String transactionCode, int lockingTime) throws BatchException {
        if (!blocking) {
            try {
		        synchronized(this.poolKeyLock) {
		            if (this.containsKeyPair(key)) return null;
					String saveTransactionStr =  null;

					if (lockingTime == 0) {
						saveTransactionStr = transactionCode + this.lockKeyTimeSep + new Long(Long.MAX_VALUE).toString();
					} else {
						saveTransactionStr = transactionCode + this.lockKeyTimeSep + new Long(System.currentTimeMillis() + (lockingTime * 1000)).toString();
					}

			        keyMapObjPut(key, saveTransactionStr);

		            this.writeMapFileFlg = true;

		            if (workFileMemory == false) {

					    // データ格納場所記述ファイル再保存
					    this.bw.write(new StringBuffer("+").append(workFileSeq).append(key.toString()).append(workFileSeq).append(saveTransactionStr).append(workFileSeq).append(new Date().getTime()).append(workFileSeq).append(workFileEndPoint).toString());
					    this.bw.newLine();
					    this.bw.flush();
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
    public String removeLock (Integer key, String transactionCode) throws BatchException {

        String ret = null;
        if (!blocking) {
            try {
                synchronized(this.poolKeyLock) {
		            if (!this.containsKeyPair(key)) return transactionCode;
		            if (!(((String[])((String)this.keyMapObjGet(key)).split(this.lockKeyTimeSep))[0]).equals(transactionCode)) return null;
					ret = ((String[])((String)this.keyMapObjGet(key)).split(this.lockKeyTimeSep))[0];

                    // データの書き込みを指示
                    this.writeMapFileFlg = true;

                    if (workFileMemory == false) {

                        // データ格納場所記述ファイル再保存(登録と合わせるために4つに分割できるようにする)
                        this.bw.write(new StringBuffer("-").append(workFileSeq).append(key.toString()).append(workFileSeq).append(" ").append(workFileSeq).append(new Date().getTime()).append(workFileSeq).append(workFileEndPoint).toString());
                        this.bw.newLine();
                        this.bw.flush();
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
    public boolean isLock (Integer key) {
        return this.containsKeyPair(key);
    }


    /**
     * Lockの自動開放メソッド.<br>
     * 引数の時間だけ経過しているLockは強制的に開放される<br>
     *
     * @param time 現在ミリ秒
     */
    public void autoLockRelease(long time) {
        synchronized(poolKeyLock) {
            Object key = null;

            Set set = this.keyMapObj.keySet();
            Iterator iterator = set.iterator();

            while(iterator.hasNext()){
                key = iterator.next();
                String transactionLine = (String)this.keyMapObj.get(key);
                String[] codeList = transactionLine.split(this.lockKeyTimeSep);
                if(Long.parseLong(codeList[1]) > time) this.keyMapObj.remove(key);
            }
        }
    }


    /**
     * keyMapObjに対するアクセスメソッド.<br>
     *
     */
    private void keyMapObjPut(Integer key, String val) {
        this.keyMapObj.put(key, val);
        this.keyMapObj.setKLastDataChangeTime(new Date().getTime());
        this.lastAccess = System.currentTimeMillis();
    }

    /**
     * keyMapObjに対するアクセスメソッド.<br>
     * 更新時間を登録しない.<br>
     *
     */
    private void keyMapObjPutNoChange(Integer key, String val) {
        this.keyMapObj.put(key, val);
        this.lastAccess = System.currentTimeMillis();
    }

    /**
     * keyMapObjに対するアクセスメソッド.<br>
     * 任意の更新時間をセットする.<br>
     *
     */
    private void keyMapObjPutSetTime(Integer key, String val, long execTime) {
        this.keyMapObj.put(key, val);
        this.keyMapObj.setKLastDataChangeTime(execTime);
        this.lastAccess = System.currentTimeMillis();
    }


    /**
     * keyMapObjに対するアクセスメソッド.<br>
     * get<br>
     */
    private String keyMapObjGet(Integer key) {
        this.lastAccess = System.currentTimeMillis();
        return (String)this.keyMapObj.get(key);

    }

    /**
     * keyMapObjに対するアクセスメソッド.<br>
     * 
     */
    private void keyMapObjRemove(Integer key) {
        this.keyMapObj.remove(key);
        this.keyMapObj.setKLastDataChangeTime(new Date().getTime());
        this.lastAccess = System.currentTimeMillis();
    }

    /**
     * keyMapObjに対するアクセスメソッド.<br>
     *  任意の更新時間をセットする.<br>
     */
    private void keyMapObjRemoveSetTime(Integer key, long execTime) {
        this.keyMapObj.remove(key);
        this.keyMapObj.setKLastDataChangeTime(execTime);
        this.lastAccess = System.currentTimeMillis();
    }


    // 引数で渡されてストリームに対しkeyMapObjを書き出す
    public void outputKeyMapObj2Stream(PrintWriter pw) throws BatchException {
        if (!blocking) {
            try {
                synchronized(poolKeyLock) {
                    logger.info("outputKeyMapObj2Stream - synchronized - start");
                    synchronized(this.getKeyLock) {
                        String allDataSep = "";
                        StringBuffer allDataBuf = new StringBuffer();

                        // keyMapObjの全内容を1行文字列として書き出し
                        Set entrySet = this.keyMapObj.entrySet();
                        Iterator entryIte = entrySet.iterator();   
                        while(entryIte.hasNext()) {
                            Map.Entry obj = (Map.Entry)entryIte.next();

                            allDataBuf.append(allDataSep);
                            allDataBuf.append((Integer)obj.getKey());
                            allDataBuf.append(workFileSeq);
                            allDataBuf.append(this.keyMapObjGet((Integer)obj.getKey()));
                            allDataSep = ImdstDefine.imdstConnectAllDataSendDataSep;
                        }   

                        pw.println(allDataBuf.toString());
                    }
                    //logger.debug("outputKeyMapObj2Stream - synchronized - end");
                }
            } catch (Exception e) {
                logger.error("outputKeyMapObj2Stream - Error");
                blocking = true;
                StatusUtil.setStatusAndMessage(1, "outputKeyMapObj2Stream - Error [" + e.getMessage() + "]");
                throw new BatchException(e);
            }
        }
    }

    // 引数で渡されてストリームからの値でデータを作成する
    public void inputKeyMapObj2Stream(BufferedReader br) throws BatchException {
        if (!blocking) {
            try {
                int i = 0;
                String[] oneDatas = null;
                synchronized(poolKeyLock) {
                    logger.info("inputKeyMapObj2Stream - synchronized - start");
                    synchronized(this.getKeyLock) {
                        // 事前に不要なファイルを削除

                        // KeyMapファイルを消しこみ
                        File keyMapObjFile = new File(this.keyFilePath);
                        if (keyMapObjFile.exists()) {
                            keyMapObjFile.delete();
                            keyMapObjFile = null;
                        }

                        // TmpKeyMapファイルを消しこみ
                        File keyMapTmpObjFile = new File(this.keyFileTmpPath);
                        if (keyMapTmpObjFile.exists()) {
                            keyMapTmpObjFile.delete();
                            keyMapTmpObjFile = null;
                        }

                        // WorkKeyMapファイルを消しこみ
                        File workKeyMapObjFile = new File(this.workKeyFilePath);
                        if (workKeyMapObjFile.exists()) {
                            if (this.bw != null) this.bw.close();
                            if (this.osw != null) this.osw.close();
                            if (this.fos != null) this.fos.close();
                            workKeyMapObjFile.delete();
                        }

                        // Disk時はデータファイルを削除
                        if(this.keyMapObj != null) this.keyMapObj.close();
                        if (!this.dataMemory) {
                            File  dataFile = new File(this.diskModeRestoreFile);
                            if(dataFile.exists()) {
                                dataFile.delete();
                            }
                        }

                        this.keyMapObj = new KeyManagerValueMap(this.mapSize);
                        if (!dataMemory) {
                            this.keyMapObj.initNoMemoryModeSetting(this.diskModeRestoreFile);
                        }

                        // WorkKeyMapファイル用のストリームを作成
                        this.fos = new FileOutputStream(new File(this.workKeyFilePath));
                        this.osw = new OutputStreamWriter(fos , workMapFileEnc);
                        this.bw = new BufferedWriter(osw);

                        // ストリームからKeyMapの全内容を読み込み、パース後1件づつ登録
                        String allDataStr = br.readLine();
                        String[] allDataLines = allDataStr.split(ImdstDefine.imdstConnectAllDataSendDataSep);


                        if (allDataLines.length == 1) {
                            if (!allDataLines[0].trim().equals("")) {
                                oneDatas = allDataLines[0].split(workFileSeq);
                                this.keyMapObjPut(new Integer(oneDatas[0]), oneDatas[1]);
                            }
                        } else if (allDataLines.length > 1) {

                            for (i = 0; i < (allDataLines.length - 1); i++) {
                                if (!allDataLines[i].trim().equals("")) {
                                    oneDatas = allDataLines[i].split(workFileSeq);
                                    this.keyMapObjPutNoChange(new Integer(oneDatas[0]), oneDatas[1]);
                                }
                            }

                            if (!allDataLines[allDataLines.length - 1].trim().equals("")) {
                                oneDatas = allDataLines[allDataLines.length - 1].split(workFileSeq);
                                this.keyMapObjPut(new Integer(oneDatas[0]), oneDatas[1]);
                            }
                        }

                        // ファイルに書き込み
                        File keyFile = new File(this.keyFilePath);
                        FileOutputStream oF = new FileOutputStream(keyFile);
                        ObjectOutputStream oO = new ObjectOutputStream(oF);
                        // キャッシュ情報を保持したDataSetをファイル書き込み
                        oO.writeObject(this.keyMapObj);
                        oO.close();
                        oF.close();
                    }
                    logger.info("inputKeyMapObj2Stream - synchronized - end");
                }
            } catch (Exception e) {
                logger.error("writeKeyMapObj2Stream - Error");
                blocking = true;
                StatusUtil.setStatusAndMessage(1, "writeKeyMapObj2Stream - Error [" + e.getMessage() + "]");
                throw new BatchException(e);
            }
        }
    }


    // データの最終更新時間を返す
    public long getLastDataChangeTime() {
        return this.keyMapObj.getKLastDataChangeTime();
    }

    // 自身のステータスがエラーでないかを返す
    public boolean checkError() {
        return this.blocking;
    }
}