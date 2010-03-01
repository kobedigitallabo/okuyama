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

    // Mapファイル本体を更新する時間間隔(ミリ秒)(時間間隔の合計 = updateInterval × intervalCount)
    private static int updateInterval = 10000;
    private static int intervalCount = 30;

    // workMapファイルのデータセパレータ文字列
    private static String workFileSeq = ImdstDefine.keyWorkFileSep;

    // workMapファイルの文字コード
    private static String workMapFileEnc = ImdstDefine.keyWorkFileEncoding;

    // workMapファイルのデータセパレータ文字列
    private static String workFileEndPoint = ImdstDefine.keyWorkFileEndPointStr;

    // workMapファイルをメモリーモードにするかの指定
    private boolean workFileMemory = false;

    // Dataのメモリーモードかファイルモードかの指定
    private boolean dataMemory = true;

	private String diskModeRestoreFile = null;

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

                                // データは必ず3つに分解できる
                                if (workSplitStrs.length == 3) {
                                    keyMapObjPut(new Integer(workSplitStrs[0]), workSplitStrs[1]);
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
                    logger.error("init - Error" + e);
                    blocking = true;
                    StatusUtil.setStatus(1);
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
                logger.info ("KeyMapManager - run - システム停止1");
                break;
            }

            try {
                // 1サイクル30秒の停止を規定回数行う(途中で停止要求があった場合は無条件で処理実行)
                for (int count = 0; count < intervalCount; count++) {

                    // システム停止要求を監視
                    if (StatusUtil.getStatus() != 0) {
                        logger.info ("KeyMapManager - run - システム停止2");
                        break;
                    }

                    Thread.sleep(updateInterval);
                }

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
                            oO.writeObject(keyMapObj);
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
                }
            } catch (Exception e) {
                logger.error("run - Error" + e);
                blocking = true;
                StatusUtil.setStatus(1);
                e.printStackTrace();
            }   
        }
    }

    // キーを指定することでノードをセットする
    public void setKeyPair(Integer key, String keyNode) throws BatchException {
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
                        this.bw.write(new StringBuffer(key.toString()).append(workFileSeq).append(keyNode).append(workFileSeq).append(workFileEndPoint).toString());
                        this.bw.newLine();
                        this.bw.flush();
                    }
                    //logger.debug("setKeyPair - synchronized - end");
                }
            } catch (Exception e) {
                logger.error("setKeyPair - Error");
                blocking = true;
                StatusUtil.setStatus(1);
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

    // Tagとキーを指定することでTagとキーをセットする
    public void setTagPair(Integer tag, String key) throws BatchException {
        if (!blocking) {
            try {
                synchronized(this.setTagLock) {

                    String keyStrs = key;
                    Integer tagCnv = new Integer((tagStartStr + tag + tagEndStr).hashCode());

                    if (this.containsKeyPair(tagCnv)) {
                        keyStrs = this.getKeyPair(tagCnv);

                        if (keyStrs.indexOf(key) == -1) {

                            keyStrs = keyStrs + tagKeySep + key;
                        }
                    }

                    this.setKeyPair(tagCnv, keyStrs);
                }
            } catch (Exception e) {
                logger.error("setTagPair - Error");
                blocking = true;
                StatusUtil.setStatus(1);
                throw new BatchException(e);
            }
        }
    }

    // Tagを指定することでKeyリストを返す
    public String getTagPair(Integer tag) {
        String keyStrs = null;
        if (!blocking) {
            Integer tagCnv = new Integer((tagStartStr + tag + tagEndStr).hashCode());

            keyStrs =  (String)this.getKeyPair(tagCnv);
        }
        return keyStrs;
    }


    // キーを指定することでキーが存在するかを返す
    public boolean containsKeyPair(Integer key) {
        boolean ret = false;
        if (!blocking) {
            synchronized(this.getKeyLock) {
                ret =  keyMapObj.containsKey(key);
            }
        }
        return ret;
    }

    // キーを指定することでキーが存在するかを返す
    public boolean containsTagPair(Integer tag) {
        boolean ret = false;
        if (!blocking) {
            Integer tagCnv = new Integer((tagStartStr + tag + tagEndStr).hashCode());

            ret =  this.containsKeyPair(tagCnv);
        }
        return ret;
    }


    /**
     * keyMapObjに対するアクセスメソッド.<br>
     * put<br>
     */
    private void keyMapObjPut(Integer key, String val) {
        //this.keyMapObj.put(key, val.getBytes());
        this.keyMapObj.put(key, val);
    }

    /**
     * keyMapObjに対するアクセスメソッド.<br>
     * get<br>
     */
    private String keyMapObjGet(Integer key) {
    /*
        String ret = null;
        try {
            ret = new String((byte[])this.keyMapObj.get(key), ImdstDefine.keyWorkFileEncoding);

        } catch(Exception e) {
        }
        return ret;
    */
        return (String)this.keyMapObj.get(key);
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
                StatusUtil.setStatus(1);
                throw new BatchException(e);
            }
        }
    }

    // 引数で渡されてストリームに対しkeyMapObjを書き出す
    public void inputKeyMapObj2Stream(BufferedReader br) throws BatchException {
        if (!blocking) {
            try {
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

                        // WorkKeyMapファイル用のストリームを作成
                        this.fos = new FileOutputStream(new File(this.workKeyFilePath));
                        this.osw = new OutputStreamWriter(fos , workMapFileEnc);
                        this.bw = new BufferedWriter(osw);

                        // ストリームからKeyMapの全内容を読み込み、パース後1件づつ登録
                        String allDataStr = br.readLine();
                        String[] allDataLines = allDataStr.split(ImdstDefine.imdstConnectAllDataSendDataSep);

                        for (int i = 0; i < allDataLines.length; i++) {
                            if (!allDataLines[i].trim().equals("")) {
                                String[] oneDatas = allDataLines[i].split(workFileSeq);
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
                StatusUtil.setStatus(1);
                throw new BatchException(e);
            }
        }
    }



    // 自身のステータスがエラーでないかを返す
    public boolean checkError() {
        return this.blocking;
    }
}