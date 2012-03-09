<?php
/**
 * PHP用のクライアント.<br>
 * Javaをそのまま焼きなおしました.<br>
 * TODO:PHPのコーディングルールに従っていない.<br>
 * TODO:以下がJava版のOkuyamaClientに足りないメソッドです.<br>
 *      getTagValues
 *      getMultiTagValues
 *      getMultiTagKeys
 *      getTagKeysResult
 *      getMultiTagKeysResult
 *
 *
 */
class OkuyamaClient {

    // 接続先情報(AutoConnectionモード用)
    private $masterNodesList = null;

    // ソケット
    private $socket = null;

    // TransactionCode
    private $transactionCode = "0";

    // データセパレータ文字列
    private $sepStr = ",";

    // 接続時のデフォルトのエンコーディング
    private $connectDefaultEncoding = "UTF-8";

    // ブランク文字列の代行文字列
    private $blankStr = "(B)";

    // 接続要求切断文字列
    private $connectExitStr = "(&imdst9999&)";

    // Tagで取得出来るキー値のセパレータ文字列
    private $tagKeySep = ":";

    private $byteDataKeysSep = ":#:";

    // バイナリデータ分割保存サイズ
    private $saveSize = 2560;

    // 保存できるValue最大長
    private $maxValueSize = 2560;

    // 保存できるKey最大長
    private $maxKeySize = 320;

    // MasterServerへの接続時のタイムアウト時間
    private $connectTimeOut = 10;

    private $midleConnectTimeOut = 50;

    private $longConnectTimeOut = 600;


    private $errorno;

    private $errormsg;

    private $okuyamaVersionNo = 0.00;

    private $getMultiEndOfDataStr = "END";


    /**
     * MasterNodeの接続情報を設定する.<br>
     * 本メソッドでセットし、autoConnect()メソッドを<br>
     * 呼び出すと、自動的にその時稼動しているMasterNodeにバランシングして<br>
     * 接続される。接続出来ない場合は、別のMasterNodeに再接続される.<br>
     *
     * @param masterNodes 接続情報の配列 "IP:PORT"の形式
     */
    public function setConnectionInfos($masterNodes) { 

        $this->masterNodesList = array();
        for ($i = 0; $i < count($masterNodes); $i++) {
            $this->masterNodesList[$i] = $masterNodes[$i];
        } 
    }

    /**
     * 設定されたMasterNodeの接続情報を元に自動的に接続を行う.<br>
     * 接続出来ない場合自動的に別ノードへ再接続を行う.<br>
     *
     * @param masterNodes 接続情報の配列 "IP:PORT"の形式
     */
    public function autoConnect() {
        $ret = true;
        $tmpMasterNodeList = array();
        $repackList = null;
        $idx = 0;
        // MasterServerの候補からコピー表を作成
        foreach ($this->masterNodesList as $serverStr) {
            $tmpMasterNodeList[]  = $serverStr;
        }

        while(count($tmpMasterNodeList) > 0) {
            // ランダムに接続するMasterServerを決定
            $ran = rand(0, (count($tmpMasterNodeList) - 1));
            try {
                // 事前にソケットが残っている場合はClose
                try {
                    if ($this->socket != null) {
                        @fclose($this->socket);
                        $this->socket = null;
                    }
                } catch (Exception $e) {}

                // 次の接続候補を取り出し
                $nodeStr = $tmpMasterNodeList[$ran];
                unset($tmpMasterNodeList[$ran]);

                // 現在のサーバリストから作り直し(unsetだと添え字が切り詰められない為)
                $repackList = array();
                for ($idx = 0; $idx < count($this->masterNodesList); $idx++) {
                    if (isset($tmpMasterNodeList[$idx])) {
                        $repackList[] = $tmpMasterNodeList[$idx];
                    }
                }
                // 接続MasterServerの情報のみ抜き出したリストを作成
                $tmpMasterNodeList = $repackList;

                // MasterServerの名前部分とIP部分を分解
                $nodeInfo = explode(":", $nodeStr);
                // 接続
                $this->socket = @fsockopen($nodeInfo[0], $nodeInfo[1], $this->errorno, $this->errormsg, $this->connectTimeOut);
                // 接続できたか確認
                if (!$this->socket) {
                    // 接続失敗
                    if ($this->socket != null) @fclose($this->socket);
                    $ret = false;
                    // 最後の接続候補の場合はここで終了->全MasterServer接続失敗
                    if(count($tmpMasterNodeList) < 1) break;
                    // 接続出来ていないが、まだ候補が残っているため、再接続ループにもどる
                } else {
                    // 接続成功
                    $ret = $this->initClient();
                    break;
                }
            } catch (Exception $e) {}
        }

        return $ret;
    }


    /**
     * 接続処理.<br>
     * エンコーディング指定有り.<br>
     *
     * @param server
     * @param port
     * @param encoding
     * @throws Exception
     */
    public function connect($server, $port, $encoding="UTF-8") {
        $ret = true;
        try {
            $this->socket = fsockopen($server, $port, $this->errorno, $this->errormsg, $this->connectTimeOut);
            if (!$this->socket) {
                $ret = false;
            } else {
                $ret = $this->initClient();
            }
        } catch (Exception $e) {
            if ($this->socket != null) @fclose($this->socket);
            $ret = false;
        }
        return $ret;
    }


    /**
     * マスタサーバとの接続を切断.<br>
     *
     * @throw Exception
     */
    public function close() {
        try {

            if ($this->socket != null) {
                //@fputs($this->socket, $this->connectExitStr . "\n");
                
                @fclose($this->socket);
                $this->socket = null;
            }
        } catch (Exception $e) {
            throw $e;
        }
    }


    /**
     * Clientを初期化する.<br>
   * 今のところは最大保存サイズの初期化とバージョンの取得<br>
     *
     * @return boolean true:開始成功 false:開始失敗
     * @throws Exception
     */
    public function initClient()  {
        $ret = false;

        $serverRetStr = null;
        $serverRet = null;

        $serverRequestBuf = null;

        try {
            // エラーチェック
            if ($this->socket == null) throw new OkuyamaClientException("No ServerConnect!!");

            // 文字列バッファ初期化
            $serverRequestBuf = "";

            // 処理番号連結
            $serverRequestBuf = $serverRequestBuf . "0";
            // セパレータ連結
            $serverRequestBuf = $serverRequestBuf . $this->sepStr;

            // サーバ送信
            @fputs($this->socket, $serverRequestBuf . "\n");

            $serverRetStr = @fgets($this->socket);
            $serverRetStr = str_replace("\r", "", $serverRetStr);
            $serverRetStr = str_replace("\n", "", $serverRetStr);
            $serverRet = explode($this->sepStr, $serverRetStr);

            // 処理の妥当性確認
            if ($serverRet[0] === "0") {
                if ($serverRet[1] === "true") {
                    // 最大データサイズ取得
                    $ret = true;
                    $this->saveSize = $serverRet[2];
                    $this->maxValueSize = $this->saveSize;
                } else {

                    $ret = false;
                }
            } else {
                if ($this->masterNodesList != null && count($this->masterNodesList) > 1) {
                    if($this->autoConnect()) {
                        $ret = $this->initClient();
                    }
                } else {
                
                    // 妥当性違反
                    throw new OkuyamaClientException("Execute Violation of validity");
                }
            }

            // バージョン取得
            $versionRet = $this->getOkuyamaVersion();

            if ($versionRet[0] === "true") {

                $versionWork = explode("okuyama-", $versionRet[1]);

                $this->okuyamaVersionNo = (double)$versionWork[1];
            }
        } catch (OkuyamaClientException $oe) {
            throw $oe;
        } catch (Exception $e) {
            if ($this->masterNodesList != null && count($this->masterNodesList) > 1) {
                if($this->autoConnect()) {
                    $ret = $this->initClient();
                }
            } else {

              throw $e;
            }
        }
    return $ret;
    }

    /**
     * 接続先のokuyamaのバージョンを返す<br>
     *
     * @return String[] 要素1(データ有無):"true" or "false",要素2(データ):okuyamaのバージョン文字列
     * @throws OkuyamaClientException, Exception
     */
    public function getOkuyamaVersion()  {
        $ret = array(); 

        $serverRetStr = null;
        $serverRet = null;

        $serverRequestBuf = null;

        try {
            // エラーチェック
            if ($this->socket == null) throw new OkuyamaClientException("No ServerConnect!!");

            // 文字列バッファ初期化
            $serverRequestBuf = "";

            // 処理番号連結
            $serverRequestBuf = $serverRequestBuf . "999";

            // サーバ送信
            @fputs($this->socket, $serverRequestBuf . "\n");

            $serverRetStr = @fgets($this->socket);
            $serverRetStr = str_replace("\r", "", $serverRetStr);
            $serverRetStr = str_replace("\n", "", $serverRetStr);
            $serverRet = explode($this->sepStr, $serverRetStr);

            // 処理の妥当性確認
            if ($serverRet[0] === "999") {
                // 取得
                $ret[0] = "true";
                $ret[1] = $serverRet[1];

            } else {
                if ($this->masterNodesList != null && count($this->masterNodesList) > 1) {
                    if($this->autoConnect()) {
                        $ret = $this->getOkuyamaVersion();
                    }
                } else {
                
                    // 妥当性違反
                    throw new OkuyamaClientException("Execute Violation of validity");
                }
            }
        } catch (OkuyamaClientException $oe) {
            throw $oe;
        } catch (Exception $e) {
            if ($this->masterNodesList != null && count($this->masterNodesList) > 1) {
                if($this->autoConnect()) {
                    $ret = $this->getOkuyamaVersion();
                }
            } else {
            
                // 妥当性違反
                throw e;
            }
        }
        return $ret;
    }


    /**
     * Transactionを開始する.<br>
     * データロック、ロックリリースを使用する場合は、<br>
     * 事前に呼び出す必要がある<br>
     *
     * @throws Exception
     */
    public function startTransaction()  {
        $ret = false;

        $serverRetStr = null;
        $serverRet = null;

        $serverRequestBuf = null;

        try {
            // エラーチェック
            if ($this->socket == null) throw new OkuyamaClientException("No ServerConnect!!");

            // 文字列バッファ初期化
            $serverRequestBuf = "";

            // 処理番号連結
            $serverRequestBuf = $serverRequestBuf . "37";
            // セパレータ連結
            $serverRequestBuf = $serverRequestBuf . $this->sepStr;

            // サーバ送信
            @fputs($this->socket, $serverRequestBuf . "\n");

            $serverRetStr = @fgets($this->socket);
            $serverRetStr = str_replace("\r", "", $serverRetStr);
            $serverRetStr = str_replace("\n", "", $serverRetStr);
            $serverRet = explode($this->sepStr, $serverRetStr);

            // 処理の妥当性確認
            if ($serverRet[0] === "37") {
                if ($serverRet[1] === "true") {
                  // TransactionCode取得
                  $ret = true;
                  $this->transactionCode = $serverRet[2];
        } else {

          $ret = false;
        }
            } else {
                if ($this->masterNodesList != null && count($this->masterNodesList) > 1) {
                    if($this->autoConnect()) {
                        $ret = $this->startTransaction();
                    }
                } else {
                
                    // 妥当性違反
                    throw new OkuyamaClientException("Execute Violation of validity");
                }
            }
        } catch (OkuyamaClientException $oe) {
            throw $oe;
        } catch (Exception $e) {
            if ($this->masterNodesList != null && count($this->masterNodesList) > 1) {
                if($this->autoConnect()) {
                    $ret = $this->startTransaction();
                }
            } else {
            
                // 妥当性違反
                throw e;
            }
        }
        return $ret;
    }


    /**
     * Transactionを開始する.<br>
   * データロック、ロックリリースを使用する場合は、<br>
     * 事前に呼び出す必要がある<br>
     *
     * @throws Exception
     */
    public function endTransaction()  {
       $this->transactionCode = "0";
    }


  /**
   * 保存するデータの最大長を変更する.<br>
   *
   * @param size 保存サイズ(バイト長)
   */
  public function setSaveMaxDataSize($size) {
    $this->saveSize = $size;
    $this->maxValueSize = $size;
  }


    /**
     * データのLockを依頼する.<br>
     * 本メソッドは、startTransactionメソッドを呼び出した場合のみ有効である
   * 
     * @param keyStr
     * @param lockingTime Lockを取得後、維持する時間(この時間を経過すると自動的にLockが解除される)(単位は秒)(0は無制限)
     * @param waitLockTime Lockを取得する場合に既に取得中の場合この時間はLock取得をリトライする(単位は秒)(0は1度取得を試みる)
     * @return String[] 要素1(Lock成否):"true" or "false"
     * @throws Exception
   */
    public function lockData($keyStr, $lockingTime, $waitLockTime = 0) {
        $ret = array(); 
        $serverRetStr = null;
        $serverRet = null;

        $serverRequestBuf = null;
        try {

            if ($this->transactionCode === "" || $this->transactionCode === "0") throw new OkuyamaClientException("No Start Transaction!!");

            // Byte Lenghtチェック
            if ($this->checkStrByteLength($keyStr) > $this->maxKeySize) throw new OkuyamaClientException("Save Key Max Size " . $this->maxKeySize . " Byte");

            if ($this->socket == null) throw new OkuyamaClientException("No ServerConnect!!");

            // エラーチェック
            // Keyに対する無指定チェック
            if ($keyStr == null ||  $keyStr === "") {
                throw new OkuyamaClientException("The blank is not admitted on a key");
            }


            // 文字列バッファ初期化
            $serverRequestBuf = "";

            // 処理番号連結
            $serverRequestBuf = "30";
            // セパレータ連結
            $serverRequestBuf = $serverRequestBuf . $this->sepStr;

            // Key連結(Keyはデータ送信時には必ず文字列が必要)
            $serverRequestBuf = $serverRequestBuf. $this->dataEncoding($keyStr);
            // セパレータ連結
            $serverRequestBuf = $serverRequestBuf . $this->sepStr;

            // TransactionCode連結
            $serverRequestBuf = $serverRequestBuf. $this->transactionCode;
            // セパレータ連結
            $serverRequestBuf = $serverRequestBuf . $this->sepStr;

            // lockingTime連結
            $serverRequestBuf = $serverRequestBuf. $lockingTime;
            // セパレータ連結
            $serverRequestBuf = $serverRequestBuf . $this->sepStr;

            // waitLockTime連結
            $serverRequestBuf = $serverRequestBuf. $waitLockTime;

            // サーバ送信
            @fputs($this->socket, $serverRequestBuf . "\n");

            $serverRetStr = @fgets($this->socket);
            $serverRetStr = str_replace("\r", "", $serverRetStr);
            $serverRetStr = str_replace("\n", "", $serverRetStr);
            $serverRet = explode($this->sepStr, $serverRetStr);
  
            // 処理の妥当性確認
            if ($serverRet[0] === "30") {
                if ($serverRet[1] === "true") {

                    // 処理成功
                    $ret[0] = "true";
                } else if ($serverRet[1] === "false") {

                    // 処理失敗
                    $ret[0] = "false";
                } else if ($serverRet[1] === "error") {

                    // 処理失敗
                    $ret[0] = "false";
                }
            }  else {
                if ($this->masterNodesList != null && count($this->masterNodesList) > 1) {
                    if($this->autoConnect()) {
                        $ret = $this->lockData($keyStr, $lockingTime, $waitLockTime = 0);
                    }
                } else {
                
                    // 妥当性違反
                    throw new OkuyamaClientException("Execute Violation of validity");
                }
            }
        } catch (OkuyamaClientException $oe) {
            throw $oe;
        } catch (Exception $e) {
            if ($this->masterNodesList != null && count($this->masterNodesList) > 1) {
                if($this->autoConnect()) {
                    $ret = $this->lockData($keyStr, $lockingTime, $waitLockTime = 0);
                }
            } else {

              throw $e;
            }
        }
        return $ret;
    }


    /**
     * データのLock解除を依頼する.<br>
     * 本メソッドは、startTransactionメソッドを呼び出した場合のみ有効である
   *
     * @param keyStr 
     * @return String[] 要素1(Lock解除成否):"true" or "false"
     * @throws OkuyamaClientException, Exception
   */
    public function releaseLockData($keyStr) {
        $ret = array(); 
        $serverRetStr = null;
        $serverRet = null;

        $serverRequestBuf = null;
        try {

            if ($this->transactionCode === "" || $this->transactionCode === "0") throw new OkuyamaClientException("No Start Transaction!!");

            // Byte Lenghtチェック
            if ($this->checkStrByteLength($keyStr) > $this->maxKeySize) throw new OkuyamaClientException("Save Key Max Size " . $this->maxKeySize . " Byte");

            if ($this->socket == null) throw new OkuyamaClientException("No ServerConnect!!");

            // エラーチェック
            // Keyに対する無指定チェック
            if ($keyStr == null ||  $keyStr === "") {
                throw new OkuyamaClientException("The blank is not admitted on a key");
            }


            // 文字列バッファ初期化
            $serverRequestBuf = "";

            // 処理番号連結
            $serverRequestBuf = "31";
            // セパレータ連結
            $serverRequestBuf = $serverRequestBuf . $this->sepStr;

            // Key連結(Keyはデータ送信時には必ず文字列が必要)
            $serverRequestBuf = $serverRequestBuf. $this->dataEncoding($keyStr);
            // セパレータ連結
            $serverRequestBuf = $serverRequestBuf . $this->sepStr;

            // TransactionCode連結
            $serverRequestBuf = $serverRequestBuf. $this->transactionCode;

            // サーバ送信
            @fputs($this->socket, $serverRequestBuf . "\n");

            $serverRetStr = @fgets($this->socket);
            $serverRetStr = str_replace("\r", "", $serverRetStr);
            $serverRetStr = str_replace("\n", "", $serverRetStr);
            $serverRet = explode($this->sepStr, $serverRetStr);

            // 処理の妥当性確認
            if ($serverRet[0] === "31") {
                if ($serverRet[1] === "true") {

                    // 処理成功
                    $ret[0] = "true";
                } else if ($serverRet[1] === "false") {

                    // 処理失敗
                    $ret[0] = "false";
                } else if ($serverRet[1] === "error") {

                    // 処理失敗
                    $ret[0] = "false";
                }
            }  else {
                if ($this->masterNodesList != null && count($this->masterNodesList) > 1) {
                    if($this->autoConnect()) {
                        $ret = $this->releaseLockData($keyStr);
                    }
                } else {
                
                    // 妥当性違反
                    throw new OkuyamaClientException("Execute Violation of validity");
                }
            }
        } catch (OkuyamaClientException $oe) {
            throw $oe;
        } catch (Exception $e) {
            if ($this->masterNodesList != null && count($this->masterNodesList) > 1) {
                if($this->autoConnect()) {
                    $ret = $this->releaseLockData($keyStr);
                }
            } else {

                throw $e;
            }
        }
        return $ret;
    }


    /**
     * マスタサーバへデータを送信する.<br>
     * データ保存を行う.<br>
     * Tag有り.<br>
     * 有効期限が秒単位で設定可能
     *
     * @param keyStr Key値文字列
     * @param value Value文字列
     * @param tagStrs Tag文字配列
     * @param expireTime 有効期限(秒/単位)
     * @return boolean 成否
     * @throws OkuyamaClientException, Exception
     */
    public function setValue($keyStr, $value, $tagStrs = null, $expireTime = null) {
        $ret = false; 
        $serverRetStr = null;
        $serverRet = null;
        $serverRequestBuf = null;
        $encodeValue = "";

        try {
            // Byte Lenghtチェック
            if ($this->checkStrByteLength($keyStr) > $this->maxKeySize) throw new OkuyamaClientException("Save Key Max Size " . $this->maxKeySize . " Byte");
            if ($tagStrs != null) {
                for ($i = 0; $i < count($tagStrs); $i++) {
                    if ($this->checkStrByteLength($tagStrs[$i]) > $this->maxKeySize) throw new OkuyamaClientException("Tag Max Size " . $this->maxKeySize . " Byte");
                }
            }
            if ($this->checkStrByteLength($value) > $this->maxValueSize) throw new OkuyamaClientException("Save Value Max Size " . $this->maxValueSize . " Byte");

            if ($this->socket == null) throw new OkuyamaClientException("No ServerConnect!!");

            // エラーチェック
            // Keyに対する無指定チェック
            if ($keyStr == null ||  $keyStr === "") {
                throw new OkuyamaClientException("The blank is not admitted on a key");
            }

            // valueに対する無指定チェック(Valueはnullやブランクの場合は代行文字列に置き換える)
            if ($value == null ||  $value === "") {

                $encodeValue = $this->blankStr;
            } else {

                // ValueをBase64でエンコード
                $encodeValue = $this->dataEncoding($value);
            }

            // 文字列バッファ初期化
            $serverRequestBuf = "";


            // 処理番号連結
            $serverRequestBuf = "1";
            // セパレータ連結
            $serverRequestBuf = $serverRequestBuf . $this->sepStr;

            // Key連結(Keyはデータ送信時には必ず文字列が必要)
            $serverRequestBuf = $serverRequestBuf. $this->dataEncoding($keyStr);

            // セパレータ連結
            $serverRequestBuf = $serverRequestBuf . $this->sepStr;


            // Tag連結
            // Tag指定の有無を調べる
            if ($tagStrs == null || count($tagStrs) < 1) {

                // ブランク規定文字列を連結
                $serverRequestBuf = $serverRequestBuf . $this->blankStr;
            } else {

                // Tag数分連結
                $serverRequestBuf = $serverRequestBuf . $this->dataEncoding($tagStrs[0]);
                for ($i = 1; $i < count($tagStrs); $i++) {
                    $serverRequestBuf = $serverRequestBuf . $this->tagKeySep;
                    $serverRequestBuf = $serverRequestBuf . $this->dataEncoding($tagStrs[$i]);
                }
            }


            // セパレータ連結
            $serverRequestBuf = $serverRequestBuf . $this->sepStr;

            // TransactionCode連結
            $serverRequestBuf = $serverRequestBuf . $this->transactionCode;

            // セパレータ連結
            $serverRequestBuf = $serverRequestBuf . $this->sepStr;

            // Value連結
            $serverRequestBuf = $serverRequestBuf . $encodeValue;

            // 有効期限連結
            if ($expireTime != null) {
                // 有効期限あり
                if (0.880 > $this->okuyamaVersionNo) {

                    throw new OkuyamaClientException("The version of the server is old [The expiration date can be used since version 0.8.8]");
                } else {
                    // セパレータ連結
                    $serverRequestBuf = $serverRequestBuf . $this->sepStr;
                    $serverRequestBuf = $serverRequestBuf . intval($expireTime);
                    // セパレータ連結　最後に区切りを入れて送信データ終わりを知らせる
                    $serverRequestBuf = $serverRequestBuf . $this->sepStr;
                } 
            }


            // サーバ送信
            @fputs($this->socket, $serverRequestBuf . "\n");

            $serverRetStr = @fgets($this->socket);

            $serverRetStr = str_replace("\r", "", $serverRetStr);
            $serverRetStr = str_replace("\n", "", $serverRetStr);
            $serverRet = explode($this->sepStr, $serverRetStr);

            // 処理の妥当性確認
            if (count($serverRet) == 3 && $serverRet[0] === "1") {
                if ($serverRet[1] === "true") {

                    // 処理成功
                    $ret = true;
                } else{

                    // 処理失敗(メッセージ格納)
                    throw new OkuyamaClientException($serverRet[2]);
                }
            }  else {
                if ($this->masterNodesList != null && count($this->masterNodesList) > 1) {
                    if($this->autoConnect()) {
                        $ret = $this->setValue($keyStr, $value, $tagStrs, $expireTime);
                    }
                } else {
                
                    // 妥当性違反
                    throw new OkuyamaClientException("Execute Violation of validity");
                }
            }
        } catch (OkuyamaClientException $oe) {
            throw $oe;
        } catch (Exception $e) {
            if ($this->masterNodesList != null && count($this->masterNodesList) > 1) {
                if($this->autoConnect()) {
                    $ret = $this->setValue($keyStr, $value, $tagStrs, $expireTime);
                }
            } else {

                throw $e;
            }
        }

        return $ret;
    }


    /**
     * マスタサーバへデータを送信する.<br>
     * PHPのオブジェクトデータの保存を行う.<br>
     * 実装としてPHPのserialize関数を利用しているため、serialize関数で変換できないObjectは扱えない.<br>
     * Tag有り.<br>
     * 有効期限が秒単位で設定可能
     *
     * @param keyStr Key値文字列
     * @param objectValue Object値
     * @param tagStrs Tag文字配列
     * @param expireTime 有効期限(秒/単位)
     * @return boolean 成否
     * @throws OkuyamaClientException, Exception
     */
    public function setObjectValue($keyStr, $objectValue, $tagStrs = null, $expireTime = null) {
        return $this->setValue($keyStr, serialize($objectValue), $tagStrs, $expireTime);
    }


    /**
     * MasterNodeへデータを登録要求する.<br>
     * 登録と同時にValueの検索Indexを作成する<br>
     * 検索Indexを作成するので通常のSetに比べて時間がかかる.<br>
     * 全文Indexが作成されるので、値は検索可能な文字を指定すること。例えばBASE64エンコードの値などの場合は<br>
     * 検索時も同様にエンコードした値で検索する必要がある.<br>
     * ※okuyamaは検索Index作成前に、同様のKey値で値が登録されている場合は、そのKey値で登録されているValue値の<br>
     * 検索インデックスを削除してから登録が行われる.<br>
     * Tag有り.<br>
     *
     * @param keyStr Key値文字列
     * @param tagStrs Tag値の配列 例){"tag1","tag2","tag3"}
     * @param value value値
     * @param indexPrefix 作成する検索IndexをグルーピングするPrefix文字列.この値と同様の値を指定してsearchValueメソッドを呼び出すと、グループに限定して全文検索が可能となる.最大は128文字
     * @return boolean 登録成否
     * @throws OkuyamaClientException, Exception
     */
    public function setValueAndCreateIndex($keyStr, $value, $tagStrs = null, $indexPrefix = null) {
        $ret = false; 
        $serverRetStr = null;
        $serverRet = null;
        $serverRequestBuf = null;
        $encodeValue = "";


        try {
            // Byte Lenghtチェック
            if ($this->checkStrByteLength($keyStr) > $this->maxKeySize) throw new OkuyamaClientException("Save Key Max Size " . $this->maxKeySize . " Byte");
            if ($tagStrs != null) {
                for ($i = 0; $i < count($tagStrs); $i++) {
                    if ($this->checkStrByteLength($tagStrs[$i]) > $this->maxKeySize) throw new OkuyamaClientException("Tag Max Size " . $this->maxKeySize . " Byte");
                }
            }

            $valueLen = $this->checkStrByteLength($value);
            if ($valueLen > $this->maxValueSize) throw new OkuyamaClientException("Save Value Max Size " . $this->maxValueSize . " Byte");

            if ($this->socket == null) throw new OkuyamaClientException("No ServerConnect!!");

            // エラーチェック
            // Keyに対する無指定チェック
            if ($keyStr == null ||  $keyStr === "") {
                throw new OkuyamaClientException("The blank is not admitted on a key");
            }

            // valueに対する無指定チェック(Valueはnullやブランクの場合は代行文字列に置き換える)
            if ($value == null ||  $value === "") {

                $encodeValue = $this->blankStr;
            } else {

                // ValueをBase64でエンコード
                $encodeValue = $this->dataEncoding($value);
            }

            // 文字列バッファ初期化
            $serverRequestBuf = "";


            // 処理番号連結
            $serverRequestBuf = "42";
            // セパレータ連結
            $serverRequestBuf = $serverRequestBuf . $this->sepStr;

            // Key連結(Keyはデータ送信時には必ず文字列が必要)
            $serverRequestBuf = $serverRequestBuf. $this->dataEncoding($keyStr);

            // セパレータ連結
            $serverRequestBuf = $serverRequestBuf . $this->sepStr;


            // Tag連結
            // Tag指定の有無を調べる
            if ($tagStrs == null || count($tagStrs) < 1) {

                // ブランク規定文字列を連結
                $serverRequestBuf = $serverRequestBuf . $this->blankStr;
            } else {

                // Tag数分連結
                $serverRequestBuf = $serverRequestBuf . $this->dataEncoding($tagStrs[0]);
                for ($i = 1; $i < count($tagStrs); $i++) {
                    $serverRequestBuf = $serverRequestBuf . $this->tagKeySep;
                    $serverRequestBuf = $serverRequestBuf . $this->dataEncoding($tagStrs[$i]);
                }
            }


            // セパレータ連結
            $serverRequestBuf = $serverRequestBuf . $this->sepStr;

            // TransactionCode連結
            $serverRequestBuf = $serverRequestBuf . $this->transactionCode;

            // セパレータ連結
            $serverRequestBuf = $serverRequestBuf . $this->sepStr;

            // Value連結
            $serverRequestBuf = $serverRequestBuf . $encodeValue;

            // セパレータ連結
            $serverRequestBuf = $serverRequestBuf . $this->sepStr;

            // Indexプレフィックス指定の有無を調べてIndexプレフィックス連結
            if ($indexPrefix == null || $indexPrefix === "") {

                // ブランク規定文字列を連結
                $serverRequestBuf = $serverRequestBuf . $this->blankStr;
            } else {

                // Indexプレフィックス連結
                $serverRequestBuf = $serverRequestBuf . $this->dataEncoding($indexPrefix);
            }


            // タイムアウト時間を一番長く伸ばす
            socket_set_timeout($this->socket, $this->longConnectTimeOut);

            // サーバ送信
            @fputs($this->socket, $serverRequestBuf . "\n");

            $serverRetStr = @fgets($this->socket);

            $serverRetStr = str_replace("\r", "", $serverRetStr);
            $serverRetStr = str_replace("\n", "", $serverRetStr);
            $serverRet = explode($this->sepStr, $serverRetStr);

            // 処理の妥当性確認
            if (count($serverRet) == 3 && $serverRet[0] === "42") {
                if ($serverRet[1] === "true") {

                    // 処理成功
                    $ret = true;
                } else if ($serverRet[1] === "error") {

                    // 処理失敗(メッセージ格納)
                    throw new OkuyamaClientException($serverRet[2]);
                }
            }  else {
                if ($this->masterNodesList != null && count($this->masterNodesList) > 1) {
                    if($this->autoConnect()) {
                        $ret = $this->setValueAndCreateIndex($keyStr, $value, $tagStrs, $indexPrefix);
                    }
                } else {
                
                    // 妥当性違反
                    throw new OkuyamaClientException("Execute Violation of validity");
                }
            }
        } catch (OkuyamaClientException $oe) {
            throw $oe;
        } catch (Exception $e) {
            if ($this->masterNodesList != null && count($this->masterNodesList) > 1) {
                if($this->autoConnect()) {
                    $ret = $this->setValueAndCreateIndex($keyStr, $value, $tagStrs, $indexPrefix);
                }
            } else {

                throw $e;
            }
        }
        return $ret;
    }


    /**
     * マスタサーバへデータを送信する.<br>
     * 1度のみ登録可能.<br>
     * Tag有り.<br>
     *
     * @param keyStr
     * @param tagStrs
     * @param value
     * @return boolean
     * @throws OkuyamaClientException, Exception
     */
    public function setNewValue($keyStr, $value, $tagStrs = null, $expireTime = null) {
        $ret = false; 
        $serverRetStr = null;
        $serverRet = null;
        $serverRequestBuf = null;
        try {
            // Byte Lenghtチェック
            if ($this->checkStrByteLength($keyStr) > $this->maxKeySize) throw new OkuyamaClientException("Save Key Max Size " . $this->maxKeySize . " Byte");
            if ($tagStrs != null) {
                for ($i = 0; $i < count($tagStrs); $i++) {
                    if ($this->checkStrByteLength($tagStrs[$i]) > $this->maxKeySize) throw new OkuyamaClientException("Tag Max Size " . $this->maxKeySize . " Byte");
                }
            }
            if ($this->checkStrByteLength($value) > $this->maxValueSize) throw new OkuyamaClientException("Save Value Max Size " . $this->maxValueSize . " Byte");

            if ($this->socket == null) throw new OkuyamaClientException("No ServerConnect!!");

            // エラーチェック
            // Keyに対する無指定チェック
            if ($keyStr == null ||  $keyStr === "") {
                throw new OkuyamaClientException("The blank is not admitted on a key");
            }

            // valueに対する無指定チェック(Valueはnullやブランクの場合は代行文字列に置き換える)
            if ($value == null ||  $value === "") {

                $encodeValue = $this->blankStr;
            } else {

                // ValueをBase64でエンコード
                $encodeValue = $this->dataEncoding($value);
            }

            // 文字列バッファ初期化
            $serverRequestBuf = "";


            // 処理番号連結
            $serverRequestBuf = "6";
            // セパレータ連結
            $serverRequestBuf = $serverRequestBuf . $this->sepStr;

            // Key連結(Keyはデータ送信時には必ず文字列が必要)
            $serverRequestBuf = $serverRequestBuf. $this->dataEncoding($keyStr);

            // セパレータ連結
            $serverRequestBuf = $serverRequestBuf . $this->sepStr;


            // Tag連結
            // Tag指定の有無を調べる
            if ($tagStrs == null || count($tagStrs) < 1) {

                // ブランク規定文字列を連結
                $serverRequestBuf = $serverRequestBuf . $this->blankStr;
            } else {

                // Tag数分連結
                $serverRequestBuf = $serverRequestBuf . $this->dataEncoding($tagStrs[0]);
                for ($i = 1; $i < count($tagStrs); $i++) {
                    $serverRequestBuf = $serverRequestBuf . $this->tagKeySep;
                    $serverRequestBuf = $serverRequestBuf . $this->dataEncoding($tagStrs[$i]);
                }
            }


            // セパレータ連結
            $serverRequestBuf = $serverRequestBuf . $this->sepStr;

            // TransactionCode連結
            $serverRequestBuf = $serverRequestBuf . $this->transactionCode;

            // セパレータ連結
            $serverRequestBuf = $serverRequestBuf . $this->sepStr;

            // Value連結
            $serverRequestBuf = $serverRequestBuf . $encodeValue;

            // 有効期限連結
            if ($expireTime != null) {
                // 有効期限あり
                if (0.880 > $this->okuyamaVersionNo) {

                    throw new OkuyamaClientException("The version of the server is old [The expiration date can be used since version 0.8.8]");
                } else {
                    // セパレータ連結
                    $serverRequestBuf = $serverRequestBuf . $this->sepStr;
                    $serverRequestBuf = $serverRequestBuf . intval($expireTime);
                    // セパレータ連結　最後に区切りを入れて送信データ終わりを知らせる
                    $serverRequestBuf = $serverRequestBuf . $this->sepStr;
                } 
            }

            // サーバ送信
            @fputs($this->socket, $serverRequestBuf . "\n");

            $serverRetStr = @fgets($this->socket);
            $serverRetStr = str_replace("\r", "", $serverRetStr);
            $serverRetStr = str_replace("\n", "", $serverRetStr);
            $serverRet = explode($this->sepStr, $serverRetStr);

            // 処理の妥当性確認
            if ($serverRet != null  && $serverRet[0] === "6") {
                if ($serverRet[1] === "true") {

                    // 処理成功
                    $ret = array();
                    $ret[0] = "true";
                } else{
                    $ret = array();
                    $ret[0] = "false";
                    $ret[1] = $serverRet[2];
                }
            }  else {
                if ($this->masterNodesList != null && count($this->masterNodesList) > 1) {
                    if($this->autoConnect()) {
                        $ret = $this->setNewValue($keyStr, $value, $tagStrs, $expireTime);
                    }
                } else {
                
                    // 妥当性違反
                    throw new OkuyamaClientException("Execute Violation of validity");
                }
            }
        } catch (OkuyamaClientException $oe) {
            throw $oe;
        } catch (Exception $e) {
            if ($this->masterNodesList != null && count($this->masterNodesList) > 1) {
                if($this->autoConnect()) {
                    $ret = $this->setNewValue($keyStr, $value, $tagStrs, $expireTime);
                }
            } else {

                throw $e;
            }
        }
        return $ret;
    }

    /**
     * マスタサーバへObjectデータを送信する.<br>
     * 値の新規登録を保証する<br>
     * PHPのオブジェクトデータの保存を行う.<br>
     * 実装としてPHPのserialize関数を利用しているため、serialize関数で変換できないObjectは扱えない.<br>
     * Tag有り.<br>
     * 有効期限が秒単位で設定可能<br>
     *
     * @param keyStr Key値文字列
     * @param objectValue Object値
     * @param tagStrs Tag文字配列
     * @param expireTime 有効期限(秒/単位)
     * @return boolean 成否
     * @throws OkuyamaClientException, Exception
     */
    public function setNewObjectValue($keyStr, $objectValue, $tagStrs = null, $expireTime = null) {
        return $this->setNewValue($keyStr, serialize($objectValue), $tagStrs, $expireTime);
    }

    /**
     * マスタサーバへデータを送信する.<br>
     * 排他的バージョンチェックを行い、更新する.<br>
     * バージョン番号をgetValueVersionCheckメソッドで事前にバージョン値を取得して更新時のチェック値として利用する
     * memcachedのcasに相当する.<br>
     *
     * @param keyStr 更新対象のKey値文字列
     * @param value 更新Value
     * @param tagStrs Tag値 ※必要ない場合はNULLを渡す
     * @param versionNo getValueVersionCheckで取得した戻り値のバージョンNo値
     * @return boolean 成否
     * @throws OkuyamaClientException, Exception
     */
    public function setValueVersionCheck($keyStr, $value, $tagStrs, $versionNo) {
        $ret = false; 
        $serverRetStr = null;
        $serverRet = null;
        $serverRequestBuf = null;
        try {
            // Byte Lenghtチェック
            if ($this->checkStrByteLength($keyStr) > $this->maxKeySize) throw new OkuyamaClientException("Save Key Max Size " . $this->maxKeySize . " Byte");
            if ($tagStrs != null) {
                for ($i = 0; $i < count($tagStrs); $i++) {
                    if ($this->checkStrByteLength($tagStrs[$i]) > $this->maxKeySize) throw new OkuyamaClientException("Tag Max Size " . $this->maxKeySize . " Byte");
                }
            }

            if ($this->checkStrByteLength($value) > $this->maxValueSize) throw new OkuyamaClientException("Save Value Max Size " . $this->maxValueSize . " Byte");

            if ($this->socket == null) throw new OkuyamaClientException("No ServerConnect!!");

            // エラーチェック
            // Keyに対する無指定チェック
            if ($keyStr == null ||  $keyStr === "") {
                throw new OkuyamaClientException("The blank is not admitted on a key");
            }

            // valueに対する無指定チェック(Valueはnullやブランクの場合は代行文字列に置き換える)
            if ($value == null ||  $value === "") {

                $encodeValue = $this->blankStr;
            } else {

                // ValueをBase64でエンコード
                $encodeValue = $this->dataEncoding($value);
            }

            // 文字列バッファ初期化
            $serverRequestBuf = "";


            // 処理番号連結
            $serverRequestBuf = "16";
            // セパレータ連結
            $serverRequestBuf = $serverRequestBuf . $this->sepStr;

            // Key連結(Keyはデータ送信時には必ず文字列が必要)
            $serverRequestBuf = $serverRequestBuf. $this->dataEncoding($keyStr);

            // セパレータ連結
            $serverRequestBuf = $serverRequestBuf . $this->sepStr;


            // Tag連結
            // Tag指定の有無を調べる
            if ($tagStrs == null || count($tagStrs) < 1) {

                // ブランク規定文字列を連結
                $serverRequestBuf = $serverRequestBuf . $this->blankStr;
            } else {

                // Tag数分連結
                $serverRequestBuf = $serverRequestBuf . $this->dataEncoding($tagStrs[0]);
                for ($i = 1; $i < count($tagStrs); $i++) {
                    $serverRequestBuf = $serverRequestBuf . $this->tagKeySep;
                    $serverRequestBuf = $serverRequestBuf . $this->dataEncoding($tagStrs[$i]);
                }
            }


            // セパレータ連結
            $serverRequestBuf = $serverRequestBuf . $this->sepStr;

            // TransactionCode連結
            $serverRequestBuf = $serverRequestBuf . $this->transactionCode;

            // セパレータ連結
            $serverRequestBuf = $serverRequestBuf . $this->sepStr;

            // Value連結
            $serverRequestBuf = $serverRequestBuf . $encodeValue;

            // セパレータ連結
            $serverRequestBuf = $serverRequestBuf . $this->sepStr;

            // VersionNo連結
            $serverRequestBuf = $serverRequestBuf . $versionNo;

            // サーバ送信
            @fputs($this->socket, $serverRequestBuf . "\n");

            $serverRetStr = @fgets($this->socket);
            $serverRetStr = str_replace("\r", "", $serverRetStr);
            $serverRetStr = str_replace("\n", "", $serverRetStr);
            $serverRet = explode($this->sepStr, $serverRetStr);

            // 処理の妥当性確認
            if ($serverRet != null  && $serverRet[0] === "16") {
                if ($serverRet[1] === "true") {

                    // 処理成功
                    $ret = array();
                    $ret[0] = "true";
                } else{
                    $ret = array();
                    $ret[0] = "false";
                    $ret[1] = $serverRet[2];
                }
            }  else {
                if ($this->masterNodesList != null && count($this->masterNodesList) > 1) {
                    if($this->autoConnect()) {
                        $ret = $this->setValueVersionCheck($keyStr, $value, $tagStrs, $versionNo);
                    }
                } else {
                
                    // 妥当性違反
                    throw new OkuyamaClientException("Execute Violation of validity");
                }
            }
        } catch (OkuyamaClientException $oe) {
            throw $oe;
        } catch (Exception $e) {
            if ($this->masterNodesList != null && count($this->masterNodesList) > 1) {
                if($this->autoConnect()) {
                    $ret = $this->setValueVersionCheck($keyStr, $value, $tagStrs, $versionNo);
                }
            } else {

                throw $e;
            }
        }
        return $ret;
    }


    /**
     * MasterNodeへデータの加算を要求する.<br>
     *
     * @param keyStr Key値文字列
     * @param value 加算値
     * @param initCalcValue 計算対象のKey-Valueが存在しない場合の初期化指定(TRUE=初期化する(0のvalueが作成される), FALSE=初期化されずfalseが返る)
     * @return 要素1(処理成否):Boolean true/false,要素2(演算後の結果):double 数値
     * @throws OkuyamaClientException, Exception
     */
    public function incrValue($keyStr, $value, $initCalcValue=FALSE) {
        $ret = false; 
        $serverRetStr = null;
        $serverRet = null;
        $serverRequestBuf = null;
        try {
            // Byte Lenghtチェック
            if ($this->checkStrByteLength($keyStr) > $this->maxKeySize) throw new OkuyamaClientException("Save Key Max Size " . $this->maxKeySize . " Byte");

            if (!is_int((int)$value)) throw new OkuyamaClientException("Save Value Integer Only");

            if ($this->socket == null) throw new OkuyamaClientException("No ServerConnect!!");

            // エラーチェック
            // Keyに対する無指定チェック
            if ($keyStr == null ||  $keyStr === "") {
                throw new OkuyamaClientException("The blank is not admitted on a key");
            }

            // ValueをBase64でエンコード
            $encodeValue = $this->dataEncoding($value);


            // 文字列バッファ初期化
            $serverRequestBuf = "";


            // 処理番号連結
            $serverRequestBuf = "13";
            // セパレータ連結
            $serverRequestBuf = $serverRequestBuf . $this->sepStr;

            // Key連結(Keyはデータ送信時には必ず文字列が必要)
            $serverRequestBuf = $serverRequestBuf. $this->dataEncoding($keyStr);

            // セパレータ連結
            $serverRequestBuf = $serverRequestBuf . $this->sepStr;

            // TransactionCode連結
            $serverRequestBuf = $serverRequestBuf . $this->transactionCode;

            // セパレータ連結
            $serverRequestBuf = $serverRequestBuf . $this->sepStr;

            // Value連結
            $serverRequestBuf = $serverRequestBuf . $encodeValue;

            // 初期化設定
            if ($initCalcValue === TRUE) {
                // セパレータ連結
                $serverRequestBuf = $serverRequestBuf . $this->sepStr;

                // Value連結
                $serverRequestBuf = $serverRequestBuf . "1";
            }

            // サーバ送信
            @fputs($this->socket, $serverRequestBuf . "\n");

            $serverRetStr = @fgets($this->socket);
            $serverRetStr = str_replace("\r", "", $serverRetStr);
            $serverRetStr = str_replace("\n", "", $serverRetStr);
            $serverRet = explode($this->sepStr, $serverRetStr);

            // 処理の妥当性確認
            if (count($serverRet) == 3 && $serverRet[0] === "13") {
                if ($serverRet[1] === "true") {

                    // データ有り
                    $ret[0] =$serverRet[1];

                    // Value文字列をBase64でデコード
                    $ret[1] = (double)$this->dataDecoding($serverRet[2], $encoding);
                } else if ($serverRet[1] === "false") {

                    // データなし
                    $ret[0] = "false";
                    $ret[1] = NULL;
                } else if ($serverRet[1] === "error") {
                    // 妥当性違反
                    throw new OkuyamaClientException($ret[2]);
                }
            }  else {
                if ($this->masterNodesList != null && count($this->masterNodesList) > 1) {
                    if($this->autoConnect()) {
                        $ret = $this->incrValue($keyStr, $value);
                    }
                } else {
                
                    // 妥当性違反
                    throw new OkuyamaClientException("Execute Violation of validity");
                }
            }

        } catch (OkuyamaClientException $oe) {
            throw $oe;
        } catch (Exception $e) {
            if ($this->masterNodesList != null && count($this->masterNodesList) > 1) {
                if($this->autoConnect()) {
                    $ret = $this->incrValue($keyStr, $value);
                }
            } else {

                throw $e;
            }
        }
        return $ret;
    }



    /**
     * MasterNodeへデータの減算を要求する.<br>
     *
     * @param keyStr Key値文字列
     * @param value 減算値
     * @param initCalcValue 計算対象のKey-Valueが存在しない場合の初期化指定(TRUE=初期化する(0のvalueが作成される), FALSE=初期化されずfalseが返る)
     * @return 要素1(処理成否):Boolean true/false,要素2(演算後の結果):double 数値
     * @throws OkuyamaClientException, Exception
     */
    public function decrValue($keyStr, $value, $initCalcValue=FALSE) {
        $ret = false; 
        $serverRetStr = null;
        $serverRet = null;
        $serverRequestBuf = null;
        try {
            // Byte Lenghtチェック
            if ($this->checkStrByteLength($keyStr) > $this->maxKeySize) throw new OkuyamaClientException("Save Key Max Size " . $this->maxKeySize . " Byte");

            if (!is_int((int)$value)) throw new OkuyamaClientException("Save Value Integer Only");

            if ($this->socket == null) throw new OkuyamaClientException("No ServerConnect!!");

            // エラーチェック
            // Keyに対する無指定チェック
            if ($keyStr == null ||  $keyStr === "") {
                throw new OkuyamaClientException("The blank is not admitted on a key");
            }

            // ValueをBase64でエンコード
            $encodeValue = $this->dataEncoding($value);


            // 文字列バッファ初期化
            $serverRequestBuf = "";


            // 処理番号連結
            $serverRequestBuf = "14";
            // セパレータ連結
            $serverRequestBuf = $serverRequestBuf . $this->sepStr;

            // Key連結(Keyはデータ送信時には必ず文字列が必要)
            $serverRequestBuf = $serverRequestBuf. $this->dataEncoding($keyStr);

            // セパレータ連結
            $serverRequestBuf = $serverRequestBuf . $this->sepStr;

            // TransactionCode連結
            $serverRequestBuf = $serverRequestBuf . $this->transactionCode;

            // セパレータ連結
            $serverRequestBuf = $serverRequestBuf . $this->sepStr;

            // Value連結
            $serverRequestBuf = $serverRequestBuf . $encodeValue;

            // 初期化設定
            if ($initCalcValue === TRUE) {
                // セパレータ連結
                $serverRequestBuf = $serverRequestBuf . $this->sepStr;

                // Value連結
                $serverRequestBuf = $serverRequestBuf . "1";
            }


            // サーバ送信
            @fputs($this->socket, $serverRequestBuf . "\n");

            $serverRetStr = @fgets($this->socket);
            $serverRetStr = str_replace("\r", "", $serverRetStr);
            $serverRetStr = str_replace("\n", "", $serverRetStr);
            $serverRet = explode($this->sepStr, $serverRetStr);

            // 処理の妥当性確認
            if (count($serverRet) == 3 && $serverRet[0] === "14") {
                if ($serverRet[1] === "true") {

                    // データ有り
                    $ret[0] =$serverRet[1];

                    // Value文字列をBase64でデコード
                    $ret[1] = (double)$this->dataDecoding($serverRet[2], $encoding);
                } else if ($serverRet[1] === "false") {

                    // データなし
                    $ret[0] = "false";
                    $ret[1] = NULL;
                } else if ($serverRet[1] === "error") {
                    // 妥当性違反
                    throw new OkuyamaClientException($ret[2]);
                }
            }  else {
                if ($this->masterNodesList != null && count($this->masterNodesList) > 1) {
                    if($this->autoConnect()) {
                        $ret = $this->decrValue($keyStr, $value);
                    }
                } else {
                
                    // 妥当性違反
                    throw new OkuyamaClientException("Execute Violation of validity");
                }
            }

        } catch (OkuyamaClientException $oe) {
            throw $oe;
        } catch (Exception $e) {
            if ($this->masterNodesList != null && count($this->masterNodesList) > 1) {
                if($this->autoConnect()) {
                    $ret = $this->decrValue($keyStr, $value);
                }
            } else {

                throw $e;
            }
        }
        return $ret;
    }


    /**
     * マスタサーバからKeyでデータを取得する.<br>
     * 文字列エンコーディング指定あり.<br>
     *
     * @param keyStr
     * @param encoding
     * @return String[] 要素1(データ有無):"true" or "false",要素2(データ):"データ文字列"
     * @throws OkuyamaClientException, Exception
     */
    public function getValue($keyStr, $encoding="UTF-8")  {
        $ret = array(); 
        $serverRetStr = null;
        $serverRet = null;

        $serverRequestBuf = null;

        try {
            if ($this->socket == null) throw new OkuyamaClientException("No ServerConnect!!");

            // エラーチェック
            // Keyに対する無指定チェック
            if ($keyStr == null ||  $keyStr === "") {
                throw new OkuyamaClientException("The blank is not admitted on a key");
            }

            // 文字列バッファ初期化
            $serverRequestBuf = "";

            // 処理番号連結
            $serverRequestBuf = $serverRequestBuf . "2";
            // セパレータ連結
            $serverRequestBuf = $serverRequestBuf . $this->sepStr;

            // Key連結(Keyはデータ送信時には必ず文字列が必要)
            $serverRequestBuf = $serverRequestBuf . $this->dataEncoding($keyStr);

            // サーバ送信
            @fputs($this->socket, $serverRequestBuf . "\n");

            $serverRetStr = @fgets($this->socket);
            $serverRetStr = str_replace("\r", "", $serverRetStr);
            $serverRetStr = str_replace("\n", "", $serverRetStr);
            $serverRet = explode($this->sepStr, $serverRetStr);

            // 処理の妥当性確認
            if ($serverRet[0] === "2") {
                if ($serverRet[1] === "true") {

                    // データ有り
                    $ret[0] =$serverRet[1];

                    // Valueがブランク文字か調べる
                    if ($serverRet[2] === $this->blankStr) {
                        $ret[1] = "";
                    } else {

                        // Value文字列をBase64でデコード
                        $ret[1] = $this->dataDecoding($serverRet[2], $encoding);
                    }
                } else if($serverRet[1] === "false") {

                    // データなし
                    $ret[0] = $serverRet[1];
                    $ret[1] = null;
                } else if($serverRet[1] === "error") {

                    // エラー発生
                    $ret[0] = $serverRet[1];
                    $ret[1] = $serverRet[2];
                }
            } else {
                if ($this->masterNodesList != null && count($this->masterNodesList) > 1) {
                    if($this->autoConnect()) {
                        $ret = $this->getValue($keyStr, $encoding);
                    }
                } else {

                    // 妥当性違反
                    throw new OkuyamaClientException("Execute Violation of validity");
                }
            }
        } catch (OkuyamaClientException $oe) {
            throw $oe;
        } catch (Exception $e) {
            if ($this->masterNodesList != null && count($this->masterNodesList) > 1) {
                if($this->autoConnect()) {
                    $ret = $this->getValue($keyStr, $encoding);
                }
            } else {
                throw $e;
            }
        }
        return $ret;
    }


    /**
     * マスタサーバからKeyでデータを取得する.<br>
     * setObjectValueで保存したObjectを取得する.<br>
     * 実装としてはPHPのunserialize関数を利用しているため、unserialize関数で変換できない値は扱えない
     *
     * @param keyStr
     * @param encoding
     * @return Object[] 要素1(データ有無(String)):"true" or "false" or "error",要素2(データ):Object型の値はもしくは"false"の場合はnull(データ有無がerrorの場合のみエラーメッセージ文字列(String型固定))
     * @throws OkuyamaClientException, Exception
     */
    public function getObjectValue($keyStr)  {

        $ret = array();
        $strRet = $this->getValue($keyStr);
        if ($strRet[0] === "true") {
            $ret[0] = "true";
            $ret[1] = unserialize($strRet[1]);
        } else {
            $ret[0] = $strRet[0];
            $ret[1] = $strRet[1];
        }
        return $ret;
    }


    /**
     * マスタサーバからKeyを複数個指定することで一度に複数個のKeyとValueを取得する.<br>
     * 取得されたKeyとValueがarrayにKeyとValueの組になって格納され返される<br>
     * 存在しないKeyを指定した場合は返却される連想配列には含まれない<br> 
     * Key値にブランクを指定した場合はKeyを指定していないものとみなされる<br> 
     * 
     * 文字列エンコーディング指定あり.<br>
     *
     * @param keyStrList Key値配列<br>1つだけのKeyを指定することは出来ない
     * @param encoding エンコーディング指定
     * @return array 取得データの連想配列 取得キーに同一のKey値を複数指定した場合は束ねられる arrayのキー値は指定されたKeyとなりValueは取得した値となる<br>全てのKeyに紐付くValueが存在しなかった場合は、nullが返る
     * @throws OkuyamaClientException, Exception
     */
    public function getMultiValue($keyStrList, $encoding="UTF-8")  {
        $ret = array(); 
        $serverRetStr = null;
        $serverRet = null;

        $serverRequestBuf = null;

        try {
            if ($this->socket == null) throw new OkuyamaClientException("No ServerConnect!!");

            // エラーチェック
            // Keyに対する無指定チェック
            if ($keyStrList == null ||  count($keyStrList) < 2) {
                throw new OkuyamaClientException("The blank is not admitted on a key");
            }

            $sendKeyList = array();

            // 文字列バッファ初期化
            $serverRequestBuf = "";

            // 処理番号連結
            $serverRequestBuf = $serverRequestBuf . "22";
            // セパレータ連結
            $serverRequestBuf = $serverRequestBuf . $this->sepStr;

            // Key連結(Keyはデータ送信時には必ず文字列が必要)
            $keysSep = "";
            for ($i = 0; $i < count($keyStrList); $i++) {
                if ($keyStrList[$i] != null && trim($keyStrList[$i]) !== "") {
                    $serverRequestBuf = $serverRequestBuf . $keysSep;
                    $serverRequestBuf = $serverRequestBuf . $this->dataEncoding($keyStrList[$i]);
                    $sendKeyList[] = $keyStrList[$i];
                    $keysSep = $this->sepStr;
                }
            }

            if ($sendKeyList == null ||  count($sendKeyList) < 2) {
                throw new OkuyamaClientException("The blank is not admitted on a key");
            }

            // サーバ送信
            @fputs($this->socket, $serverRequestBuf . "\n");


            $readIdx = 0;
            while (true) {
                $serverRetStr = @fgets($this->socket);
                if ($serverRetStr === FALSE) break;

                $serverRetStr = str_replace("\r", "", $serverRetStr);
                $serverRetStr = str_replace("\n", "", $serverRetStr);
                if ($serverRetStr === $this->getMultiEndOfDataStr) break;

                $serverRet = explode($this->sepStr, $serverRetStr);

                // 処理の妥当性確認
                if ($serverRet[0] === "22") {
                    if ($serverRet[1] === "true") {
    
                        // データ有り
                        $oneDataKey = null;
                        $oneDataValue = null;

                        $oneDataKey = $sendKeyList[$readIdx];
                        // Valueがブランク文字か調べる
                        if ($serverRet[2] === $this->blankStr) {
                            $oneDataValue = "";
                        } else {

                            // Value文字列をBase64でデコード
                            $oneDataValue = $this->dataDecoding($serverRet[2], $encoding);
                        }

                        $ret[$oneDataKey] =  $oneDataValue;
                    }
                } else {
    
                    // 妥当性違反
                    throw new OkuyamaClientException("Execute Violation of validity");
                }
                $readIdx++;
            }

            if (count($ret) < 1) $ret = null;

        } catch (OkuyamaClientException $oe) {
            throw $oe;
        } catch (Exception $e) {
            if ($this->masterNodesList != null && count($this->masterNodesList) > 1) {
                if($this->autoConnect()) {
                    $ret = $this->getMultiValue($keyStrList, $encoding);
                }
            } else {
                throw $e;
            }
        }
        return $ret;
    }


    /**
     * マスタサーバからKeyでデータを取得する.<br>
     * 文字列エンコーディング指定あり.<br>
     * Valueのバージョン値を合わせて返す.<br>
     * setValueVersionCheckにて取得したバージョンNoを利用する
     * memcachedのgetsに相当する.<br>
     *
     * @param keyStr 取得対象のKey値文字列
     * @param encoding Valueの文字コード
     * @return String[] 要素1(データ有無):"true" or "false",要素2(データ):"データ文字列",要素3(VersionNo):"数字" 
     * @throws OkuyamaClientException, Exception
     */
    public function getValueVersionCheck($keyStr, $encoding="UTF-8")  {
        $ret = array(); 
        $serverRetStr = null;
        $serverRet = null;

        $serverRequestBuf = null;

        try {
            if ($this->socket == null) throw new OkuyamaClientException("No ServerConnect!!");

            // エラーチェック
            // Keyに対する無指定チェック
            if ($keyStr == null ||  $keyStr === "") {
                throw new OkuyamaClientException("The blank is not admitted on a key");
            }

            // 文字列バッファ初期化
            $serverRequestBuf = "";

            // 処理番号連結
            $serverRequestBuf = $serverRequestBuf . "15";
            // セパレータ連結
            $serverRequestBuf = $serverRequestBuf . $this->sepStr;

            // Key連結(Keyはデータ送信時には必ず文字列が必要)
            $serverRequestBuf = $serverRequestBuf . $this->dataEncoding($keyStr);

            // サーバ送信
            @fputs($this->socket, $serverRequestBuf . "\n");

            $serverRetStr = @fgets($this->socket);
            $serverRetStr = str_replace("\r", "", $serverRetStr);
            $serverRetStr = str_replace("\n", "", $serverRetStr);
            $serverRet = explode($this->sepStr, $serverRetStr);

            // 処理の妥当性確認
            if ($serverRet[0] === "15") {
                if ($serverRet[1] === "true") {

                    // データ有り
                    $ret[0] =$serverRet[1];

                    // Valueがブランク文字か調べる
                    if ($serverRet[2] === $this->blankStr) {
                        $ret[1] = "";
                    } else {

                        // Value文字列をBase64でデコード
                        $ret[1] = $this->dataDecoding($serverRet[2], $encoding);
                    }

                    if (count($serverRet) > 2) 
                        $ret[2] = $serverRet[3];

                } else if($serverRet[1] === "false") {

                    // データなし
                    $ret[0] = $serverRet[1];
                    $ret[1] = null;
                    $ret[2] = null;
                } else if($serverRet[1] === "error") {

                    // エラー発生
                    $ret[0] = $serverRet[1];
                    $ret[1] = $serverRet[2];

                    if (count($serverRet) > 2) 
                        $ret[2] = $serverRet[3];
                }
            } else {
                if ($this->masterNodesList != null && count($this->masterNodesList) > 1) {
                    if($this->autoConnect()) {
                        $ret = $this->getValueVersionCheck($keyStr, $encoding);
                    }
                } else {

                    // 妥当性違反
                    throw new OkuyamaClientException("Execute Violation of validity");
                }
            }
        } catch (OkuyamaClientException $oe) {
            throw $oe;
        } catch (Exception $e) {
            if ($this->masterNodesList != null && count($this->masterNodesList) > 1) {
                if($this->autoConnect()) {
                    $ret = $this->getValueVersionCheck($keyStr, $encoding);
                }
            } else {
                throw $e;
            }
        }
        return $ret;
    }


    /**
     * マスタサーバからKeyでデータを取得する.<br>
     * 文字列エンコーディング指定あり.<br>
     * 取得と同時に値の有効期限を取得時から最初に設定した時間分延長更新<br>
     * 有効期限を設定していない場合は更新されない.<br>
     * Sessionキャッシュなどでアクセスした時間から所定時間有効などの場合にこちらのメソッドで<br>
     * 値を取得していれば自動的に有効期限が更新される<br>
     *
     * @param keyStr
     * @param encoding
     * @return String[] 要素1(データ有無):"true" or "false",要素2(データ):"データ文字列"
     * @throws OkuyamaClientException, Exception
     */
    public function getValueAndUpdateExpireTime($keyStr, $encoding="UTF-8")  {
        $ret = array(); 
        $serverRetStr = null;
        $serverRet = null;

        $serverRequestBuf = null;

        try {
            if ($this->socket == null) throw new OkuyamaClientException("No ServerConnect!!");

            // エラーチェック
            // Keyに対する無指定チェック
            if ($keyStr == null ||  $keyStr === "") {
                throw new OkuyamaClientException("The blank is not admitted on a key");
            }

            // 文字列バッファ初期化
            $serverRequestBuf = "";

            // 処理番号連結
            $serverRequestBuf = $serverRequestBuf . "17";
            // セパレータ連結
            $serverRequestBuf = $serverRequestBuf . $this->sepStr;

            // Key連結(Keyはデータ送信時には必ず文字列が必要)
            $serverRequestBuf = $serverRequestBuf . $this->dataEncoding($keyStr);

            // サーバ送信
            @fputs($this->socket, $serverRequestBuf . "\n");

            $serverRetStr = @fgets($this->socket);
            $serverRetStr = str_replace("\r", "", $serverRetStr);
            $serverRetStr = str_replace("\n", "", $serverRetStr);
            $serverRet = explode($this->sepStr, $serverRetStr);

            // 処理の妥当性確認
            if ($serverRet[0] === "17") {
                if ($serverRet[1] === "true") {

                    // データ有り
                    $ret[0] =$serverRet[1];

                    // Valueがブランク文字か調べる
                    if ($serverRet[2] === $this->blankStr) {
                        $ret[1] = "";
                    } else {

                        // Value文字列をBase64でデコード
                        $ret[1] = $this->dataDecoding($serverRet[2], $encoding);
                    }
                } else if($serverRet[1] === "false") {

                    // データなし
                    $ret[0] = $serverRet[1];
                    $ret[1] = null;
                } else if($serverRet[1] === "error") {

                    // エラー発生
                    $ret[0] = $serverRet[1];
                    $ret[1] = $serverRet[2];
                }
            } else {
                if ($this->masterNodesList != null && count($this->masterNodesList) > 1) {
                    if($this->autoConnect()) {
                        $ret = $this->getValueAndUpdateExpireTime($keyStr, $encoding);
                    }
                } else {

                    // 妥当性違反
                    throw new OkuyamaClientException("Execute Violation of validity");
                }
            }
        } catch (OkuyamaClientException $oe) {
            throw $oe;
        } catch (Exception $e) {
            if ($this->masterNodesList != null && count($this->masterNodesList) > 1) {
                if($this->autoConnect()) {
                    $ret = $this->getValueAndUpdateExpireTime($keyStr, $encoding);
                }
            } else {
                throw $e;
            }
        }
        return $ret;
    }


    /**
     * マスタサーバからKeyでObjectを取得する.<br>
     * setObjectDataで登録したデータを取り出す.<br>
     * 取得と同時に値の有効期限を取得時から最初に設定した時間分延長更新<br>
     * 有効期限を設定していない場合は更新されない.<br>
     * Sessionキャッシュなどでアクセスした時間から所定時間有効などの場合にこちらのメソッドで<br>
     * 値を取得していれば自動的に有効期限が更新される<br>
     *
     * @param keyStr
     * @param encoding
     * @return Object[] 要素1(データ有無(String)):"true" or "false" or "error",要素2(データ):Object型の値はもしくは"false"の場合はnull(データ有無がerrorの場合のみエラーメッセージ文字列(String型固定))
     * @throws OkuyamaClientException, Exception
     */
    public function getObjectValueAndUpdateExpireTime($keyStr)  {
        $ret = array();
        $strRet = $this->getValueAndUpdateExpireTime($keyStr);
        if ($strRet[0] === "true") {
            $ret[0] = "true";
            $ret[1] = unserialize($strRet[1]);
        } else {
            $ret[0] = $strRet[0];
            $ret[1] = $strRet[1];
        }
        return $ret;
    }


    /**
     * マスタサーバからKeyでデータを取得する.<br>
     * 取得データに対してJavaScriptを実行する.<br>
     * 文字列エンコーディング指定あり.<br>
     *
     * @param keyStr Key値文字列
     * @param scriptStr JavaScriptコード
     * @param encoding 取得Valueの文字コード
     * @return String[] 要素1(データ有無):"true" or "false",要素2(データ):"データ文字列"
     * @throws OkuyamaClientException, Exception
     */
    public function getValueScript($keyStr, $scriptStr, $encoding="UTF-8")  {
        $ret = array(); 
        $serverRetStr = null;
        $serverRet = null;

        $serverRequestBuf = null;

        try {
            if ($this->socket == null) throw new OkuyamaClientException("No ServerConnect!!");

            // エラーチェック
            // Keyに対する無指定チェック
            if ($keyStr == null ||  $keyStr === "") {
                throw new OkuyamaClientException("The blank is not admitted on a key");
            }

            // Keyに対する無指定チェック
            if ($scriptStr == null ||  $scriptStr === "") {
                throw new OkuyamaClientException("The blank is not admitted on a Script");
            }

            // 文字列バッファ初期化
            $serverRequestBuf = "";

            // 処理番号連結
            $serverRequestBuf = $serverRequestBuf . "8";
            // セパレータ連結
            $serverRequestBuf = $serverRequestBuf . $this->sepStr;

            // Key連結(Keyはデータ送信時には必ず文字列が必要)
            $serverRequestBuf = $serverRequestBuf . $this->dataEncoding($keyStr);

            // セパレータ連結
            $serverRequestBuf = $serverRequestBuf . $this->sepStr;
            // JavaScriptコード連結
            $serverRequestBuf = $serverRequestBuf . $this->dataEncoding($scriptStr);


            // サーバ送信
            @fputs($this->socket, $serverRequestBuf . "\n");

            $serverRetStr = @fgets($this->socket);
            $serverRetStr = str_replace("\r", "", $serverRetStr);
            $serverRetStr = str_replace("\n", "", $serverRetStr);
            $serverRet = explode($this->sepStr, $serverRetStr);

            // 処理の妥当性確認
            if ($serverRet[0] === "8") {
                if ($serverRet[1] === "true") {

                    // データ有り
                    $ret[0] =$serverRet[1];

                    // Valueがブランク文字か調べる
                    if ($serverRet[2] === $this->blankStr) {
                        $ret[1] = "";
                    } else {

                        // Value文字列をBase64でデコード
                        $ret[1] = $this->dataDecoding($serverRet[2], $encoding);
                    }
                } else if($serverRet[1] === "false") {

                    // データなし
                    $ret[0] = $serverRet[1];
                    $ret[1] = null;
                } else if($serverRet[1] === "error") {

                    // エラー発生
                    $ret[0] = $serverRet[1];
                    $ret[1] = $serverRet[2];
                }
            } else {
                if ($this->masterNodesList != null && count($this->masterNodesList) > 1) {
                    if($this->autoConnect()) {
                        $ret = $this->getValueScript($keyStr, $encoding);
                    }
                } else {
                
                    // 妥当性違反
                    throw new OkuyamaClientException("Execute Violation of validity");
                }
            }
        } catch (OkuyamaClientException $oe) {
            throw $oe;
        } catch (Exception $e) {
            if ($this->masterNodesList != null && count($this->masterNodesList) > 1) {
                if($this->autoConnect()) {
                    $ret = $this->getValueScript($keyStr, $encoding);
                }
            } else {
                throw $e;
            }
        }
        return $ret;
    }


    /**
     * マスタサーバからKeyでデータを取得する.<br>
     * 取得データに対してJavaScriptを実行する.<br>
     * 文字列エンコーディング指定あり.<br>
     *
     * @param keyStr Key値文字列
     * @param scriptStr JavaScriptコード
     * @param encoding 取得Valueの文字コード
     * @return String[] 要素1(データ有無):"true" or "false",要素2(データ):"データ文字列"
     * @throws OkuyamaClientException, Exception
     */
    public function getValueScriptForUpdate($keyStr, $scriptStr, $encoding="UTF-8")  {
        $ret = array(); 
        $serverRetStr = null;
        $serverRet = null;

        $serverRequestBuf = null;

        try {
            if ($this->socket == null) throw new OkuyamaClientException("No ServerConnect!!");

            // エラーチェック
            // Keyに対する無指定チェック
            if ($keyStr == null ||  $keyStr === "") {
                throw new OkuyamaClientException("The blank is not admitted on a key");
            }

            // Keyに対する無指定チェック
            if ($scriptStr == null ||  $scriptStr === "") {
                throw new OkuyamaClientException("The blank is not admitted on a Script");
            }

            // 文字列バッファ初期化
            $serverRequestBuf = "";

            // 処理番号連結
            $serverRequestBuf = $serverRequestBuf . "9";
            // セパレータ連結
            $serverRequestBuf = $serverRequestBuf . $this->sepStr;

            // Key連結(Keyはデータ送信時には必ず文字列が必要)
            $serverRequestBuf = $serverRequestBuf . $this->dataEncoding($keyStr);

            // セパレータ連結
            $serverRequestBuf = $serverRequestBuf . $this->sepStr;
            // JavaScriptコード連結
            $serverRequestBuf = $serverRequestBuf . $this->dataEncoding($scriptStr);


            // サーバ送信
            @fputs($this->socket, $serverRequestBuf . "\n");

            $serverRetStr = @fgets($this->socket);
            $serverRetStr = str_replace("\r", "", $serverRetStr);
            $serverRetStr = str_replace("\n", "", $serverRetStr);
            $serverRet = explode($this->sepStr, $serverRetStr);

            // 処理の妥当性確認
            if ($serverRet[0] === "9") {
                if ($serverRet[1] === "true") {

                    // データ有り
                    $ret[0] =$serverRet[1];

                    // Valueがブランク文字か調べる
                    if ($serverRet[2] === $this->blankStr) {
                        $ret[1] = "";
                    } else {

                        // Value文字列をBase64でデコード
                        $ret[1] = $this->dataDecoding($serverRet[2], $encoding);
                    }
                } else if($serverRet[1] === "false") {

                    // データなし
                    $ret[0] = $serverRet[1];
                    $ret[1] = null;
                } else if($serverRet[1] === "error") {

                    // エラー発生
                    $ret[0] = $serverRet[1];
                    $ret[1] = $serverRet[2];
                }
            } else {
                if ($this->masterNodesList != null && count($this->masterNodesList) > 1) {
                    if($this->autoConnect()) {
                        $ret = $this->getValueScriptForUpdate($keyStr, $encoding);
                    }
                } else {
                
                    // 妥当性違反
                    throw new OkuyamaClientException("Execute Violation of validity");
                }
            }
        } catch (OkuyamaClientException $oe) {
            throw $oe;
        } catch (Exception $e) {
            if ($this->masterNodesList != null && count($this->masterNodesList) > 1) {
                if($this->autoConnect()) {
                    $ret = $this->getValueScriptForUpdate($keyStr, $encoding);
                }
            } else {
                throw $e;
            }
        }
        return $ret;
    }


    /**
     * マスタサーバからTagでKey値群を取得する.<br>
     *
     * @param tagStr Tag文字列
     * @param noExistsData Keyが存在しない場合の取得指定 true=過去にtagを登録した場合はKey値は返す false=現時Keyが存在しなければ返却しない
     * @return Object[] 要素1(データ有無):"true" or "false",要素2(データ):"データ文字列"
     * @throws OkuyamaClientException, Exception
     */
    public function getTagKeys($tagStr, $noExistsData=true) {
        $ret = array(); 
        $serverRetStr = null;
        $serverRet = null;

        $serverRequestBuf = null;

        try {
            if ($this->socket == null) throw new OkuyamaClientException("No ServerConnect!!");

            // エラーチェック
            // Keyに対する無指定チェック
            if ($tagStr == null ||  $tagStr === "") {
                throw new OkuyamaClientException("The blank is not admitted on a tag");
            }

            // 文字列バッファ初期化
            $serverRequestBuf = "";

            // 処理番号連結
            $serverRequestBuf = $serverRequestBuf . "3";

            // セパレータ連結
            $serverRequestBuf = $serverRequestBuf . $this->sepStr;


            // tag連結(Keyはデータ送信時には必ず文字列が必要)
            $serverRequestBuf = $serverRequestBuf . $this->dataEncoding($tagStr);

            // セパレータ連結
            $serverRequestBuf = $serverRequestBuf . $this->sepStr;

            // 存在指定
            if ($noExistsData === true) {
                $serverRequestBuf = $serverRequestBuf . "true";
            } else {
                $serverRequestBuf = $serverRequestBuf . "false";
            }


            // サーバ送信
            @fputs($this->socket, $serverRequestBuf . "\n");

            $serverRetStr = @fgets($this->socket);
            $serverRetStr = str_replace("\r", "", $serverRetStr);
            $serverRetStr = str_replace("\n", "", $serverRetStr);
            $serverRet = explode($this->sepStr, $serverRetStr);

            // 処理の妥当性確
            if ($serverRet[0] === "4") {
                if ($serverRet[1] === "true") {

                    // データ有り
                    $ret[0] = $serverRet[1];

                    // Valueがブランク文字か調べる
                    if ($serverRet[2] === $this->blankStr) {
                        $tags = array();
                        $tags[0] = "";
                        $ret[1] = $tags;
                    } else {
                        $tags = null;

                        $serverRet[2] = str_replace("\n", "", $serverRet[2]);
                        $tags = explode($this->tagKeySep, $serverRet[2]);

                        $decTags = array();
                        for ($i = 0; $i < count($tags); $i++) {
                            $decTags[$i] = $this->dataDecoding($tags[$i]);
                        }
                        $ret[1] = $decTags;
                    }
                } else if($serverRet[1] === "false") {

                    // データなし
                    $ret[0] = $serverRet[1];
                    $ret[1] = null;
                } else if($serverRet[1] === "error") {

                    // エラー発生
                    $ret[0] = $serverRet[1];
                    $ret[1] = $serverRet[2];
                }
            }  else {
                if ($this->masterNodesList != null && count($this->masterNodesList) > 1) {
                    if($this->autoConnect()) {
                        $ret = $this->getTagKeys($tagStr);
                    }
                } else {

                    // 妥当性違反
                    throw new OkuyamaClientException("Execute Violation of validity");
                }
            }
        } catch (OkuyamaClientException $oe) {
            throw $oe;
        } catch (Exception $e) {
            if ($this->masterNodesList != null && count($this->masterNodesList) > 1) {
                if($this->autoConnect()) {
                    $ret = $this->getTagKeys($tagStr);
                }
            } else {

                throw $e;
            }
        }
        return $ret;
    }



    /**
     * MasterNodeからTag値を渡すことで紐付くValue値の集合を取得する.<br>
     * 文字列エンコーディング指定あり.<br>
     * Keyは削除されTagとの紐付けだけ残っている値は返却されない.<br>
     * 存在しないTagを指定した場合はNULLが返される<br>
     *
     * @param tagStr Tag文字列
     * @param encoding エンコーディング指定
     * @return array 取得データの連想配列 キー値はTag紐付くKeyとなりValueはそのKeyに紐付く値となる(存在しないTagを指定した場合はNULLが返される)
     * @throws OkuyamaClientException, Exception
     */
    public function getTagValues($tagStr, $encoding="UTF-8") {
        $ret = array(); 
        $serverRetStr = null;
        $serverRet = null;

        $serverRequestBuf = null;

        try {
            if ($this->socket == null) throw new OkuyamaClientException("No ServerConnect!!");

            // エラーチェック
            // Keyに対する無指定チェック
            if ($tagStr == null ||  $tagStr === "") {
                throw new OkuyamaClientException("The blank is not admitted on a tag");
            }


            $sendKeyList = array();

            // 文字列バッファ初期化
            $serverRequestBuf = "";

            // 処理番号連結
            $serverRequestBuf = $serverRequestBuf . "23";
            // セパレータ連結
            $serverRequestBuf = $serverRequestBuf . $this->sepStr;

            // Tag連結
            $serverRequestBuf = $serverRequestBuf . $this->dataEncoding($tagStr);


            // サーバ送信
            @fputs($this->socket, $serverRequestBuf . "\n");

            while (true) {
                $serverRetStr = @fgets($this->socket);
                if ($serverRetStr === FALSE) break;

                $serverRetStr = str_replace("\r", "", $serverRetStr);
                $serverRetStr = str_replace("\n", "", $serverRetStr);
                if ($serverRetStr === $this->getMultiEndOfDataStr) break;

                $serverRet = explode($this->sepStr, $serverRetStr);



                // 処理の妥当性確認
                if ($serverRet[0] === "23") {
                    if ($serverRet[1] === "true") {
    
                        // データ有り
                        $oneDataRet = array();

                        $oneDataRet[0] = $this->dataDecoding($serverRet[2], $encoding);

                        // Valueがブランク文字か調べる
                        if ($serverRet[3] === $this->blankStr) {
                            $oneDataRet[1] = "";
                        } else {
    
                            // Value文字列をBase64でデコード
                            $oneDataRet[1] =  $this->dataDecoding($serverRet[3], $encoding);
                        }
                        $ret[$oneDataRet[0]] =  $oneDataRet[1];
                    } else {
                        // データなし or エラー 今のところ無視
                    }
                } else {
    
                    // 妥当性違反
                    throw new OkuyamaClientException("Execute Violation of validity");
                }
            }

            if (count($ret) < 1) $ret = null;

        } catch (OkuyamaClientException $oe) {
            throw $oe;
        } catch (Exception $e) {
            if ($this->masterNodesList != null && count($this->masterNodesList) > 1) {
                if($this->autoConnect()) {
                    $ret = $this->getTagValues($keyStrList, $encoding);
                }
            } else {
                throw $e;
            }
        }
        return $ret;
    }

    /**
     * MasterNodeからTag値を渡すことで紐付くKey値の配列を取得する<br>
     * 複数のTagを指定することで、一度に関連する値を取得可能<br>
     * 複数のTagに紐付く値はマージされて1つとなる<br>
     * 引数のmargeTypeを指定することで、ANDとORを切り替えることが出来る<br>
     *
     * @param tagList Tag値のリスト
     * @param margeType 取得方法指定(true = AND、false=OR)
     * @param noExistsData 存在していないデータを取得するかの指定(true:取得する false:取得しない)
     * @return String[] 取得データのKey配列 取得キーに同一の値を複数指定した場合は束ねられる。結果が0件の場合はnullが返る
     */
    public function getMultiTagKeys($tagList, $margeType=true, $noExistsData=true) {
        $ret = null;
        $tagRet = null;
        $retKeyList = null;
        $serverRetStr = null;
        $serverRet = null;
        $margeRet = null;
        $tmpKeys = null;

        if ($this->socket == null) throw new OkuyamaClientException("No ServerConnect!!");

        $tagRet = $this->getTagKeys($tagList[0], $noExistsData);
        if($margeType === true && $tagRet[0] === "false") return $ret;

        if ($tagRet[0] === "true") {
            $tmpKeys = array();
            $retTmpKeys = $tagRet[1];

            for ($i = 0; $i < count($retTmpKeys); $i++) {
                $tmpKeys[$retTmpKeys[$i]] = "";
            }           
            $retKeyList = $tmpKeys;

        } else {
            $retKeyList = array();
        }

        for ($i = 1; $i < count($tagList); $i++) {
            $tagRet = $this->getTagKeys($tagList[$i]);
            if($margeType === true && $tagRet[0] === "false") return NULL;

            if ($tagRet[0] === "true") {
                if ($margeType === true) {

                    $tmpKeys = array();
                    $retTmpKeys = $tagRet[1];
                    for ($retTmpKeysIdx = 0; $retTmpKeysIdx < count($retTmpKeys); $retTmpKeysIdx++) {
                        $tmpKeys[$retTmpKeys[$retTmpKeysIdx]] = "";
                    }
                    $deleteKeys = array_diff_key($retKeyList, $tmpKeys);

                    foreach ($deleteKeys as $key => $val){
                        unset($retKeyList[$key]);
                    }   
                } else {
                    $tmpKeys = array();
                    $retTmpKeys = $tagRet[1];
                    for ($retTmpKeysIdx = 0; $retTmpKeysIdx < count($retTmpKeys); $retTmpKeysIdx++) {
                        $tmpKeys[$retTmpKeys[$retTmpKeysIdx]] = "";
                    }
                    $tmpRet = array_merge($retKeyList, $tmpKeys);
                    $retKeyList = $tmpRet;
                }
            } else {
                $tmpKeys = array();
                $retTmpKeys = $tagRet[1];
                for ($retTmpKeysIdx = 0; $retTmpKeysIdx < count($retTmpKeys); $retTmpKeysIdx++) {
                    $tmpKeys[$retTmpKeys[$retTmpKeysIdx]] = "";
                }
                $tmpRet = array_merge($retKeyList, $tmpKeys);
                $retKeyList = $tmpRet;
            }
        }

        if (count($retKeyList) < 1) return null;

        $tmpKeys = array();

        foreach ($retKeyList as $key => $val){
            $tmpKeys[] = $key;
        }

        $retKeyList = $tmpKeys;

        return $retKeyList;
    }

    /**
     * MasterNodeからTag値を渡すことで紐付くValue値の集合を取得する<br>
     * 複数のTagを指定することで、一度に関連する値を取得可能<br>
     * 複数のTagに紐付く値はマージされて1つとなる<br>
     * 引数のmargeTypeを指定することで、ANDとORを切り替えることが出来る<br>
     *
     * @param tagList Tag値のリスト
     * @param margeType 取得方法指定(true = AND、false=OR)
     * @return array KeyとValueが格納された連想配列がかえされる。1件もデータが存在しない場合はnullが返る
     */
    public function getMultiTagValues($tagList, $margeType=true) {
        $ret = null;
        $tagRet = null;
        $retKeyList = null;
        $serverRetStr = null;
        $serverRet = null;
        $margeRet = null;
        $tmpKeys = null;

        if ($this->socket == null) throw new OkuyamaClientException("No ServerConnect!!");

        $tagRet = $this->getTagValues($tagList[0]);
        if($margeType === true && $tagRet[0] === "false") return $ret;

        $ret = $tagRet;

        for ($i = 1; $i < count($tagList); $i++) {
            $tagRet = $this->getTagValues($tagList[$i]);
            if($margeType === true && $tagRet === null) return NULL;

            if ($tagRet !== null) {
                if ($margeType === true) {

                    $deleteKeys = array_diff_key($ret, $tagRet);
                    foreach ($deleteKeys as $key => $val){
                        unset($ret[$key]);
                    }   
                } else {
                    $tmpRet = array_merge($ret, $tagRet);
                    $ret = $tmpRet;
                }
            }
        }

        if (count($ret) < 1) return null;

        return $ret;
    }


    /**
     * MasterNodeからsetValueAndCreateIndexで作成されたIndexを使って検索して該当する値を取得する.<br>
     * 検索可能な文字列は漢字の場合は1文字からで、それ以外は2文字から.<br>
     * Prefxiあり.<br>
     * 
     * @param searchCharacterList 取得したい値の文字配列(エンコードはUTF-8固定)<文字配列>
     * @param  searchType 1:AND検索　2:OR検索
     * @param  prefix 検索Index作成時に指定したPrefix値
     * @return object[] 要素1(データ有無):"true" or "false",要素2(該当のKey値配列):Stringの配列
     * @throws OkuyamaClientException, Exception
     */
    public function searchValue($searchCharacterList, $searchType, $prefix=null) {
        $ret = array(); 
        $serverRetStr = null;
        $serverRet = null;

        $serverRequestBuf = null;

        try {
            if ($this->socket == null) throw new OkuyamaClientException("No ServerConnect!!");

            if ($searchCharacterList == null) throw new OkuyamaClientException("The blank is not admitted on a searchCharacterList");

            // エラーチェック
            // 検索Wordに対するチェック
            for ($i = 0; $i < count($searchCharacterList); $i++) {
                if ($this->checkStrByteLength($searchCharacterList[$i]) > 64) throw new OkuyamaClientException("SearchWord Max Size 64 Character");
            }

            // 検索Typeを調整
            if ($searchType == null || ($searchType !== "1" && $searchType !== "2")) $searchType = "2";

            // Prefixをチェック
            if ($prefix == null || $prefix === "") $prefix = null;


            // 文字列バッファ初期化
            $serverRequestBuf = "";

            // 処理番号連結
            $serverRequestBuf = $serverRequestBuf . "43";

            // セパレータ連結
            $serverRequestBuf = $serverRequestBuf . $this->sepStr;


            // 検索ワード値連結
            // 複数の検索Wordは":"で連結して送る
            $sep = "";
            for ($i = 0; $i < count($searchCharacterList); $i++) {

                $serverRequestBuf = $serverRequestBuf . $sep;
                $serverRequestBuf = $serverRequestBuf . $this->dataEncoding($searchCharacterList[$i]);
                $sep = ":";
            }


            // セパレータ連結
            $serverRequestBuf = $serverRequestBuf . $this->sepStr;

            // SearchType連結
            $serverRequestBuf = $serverRequestBuf . $searchType;

            // セパレータ連結
            $serverRequestBuf = $serverRequestBuf . $this->sepStr;

            // prefix連結
            // Indexプレフィックス指定の有無を調べてIndexプレフィックス連結
            if ($prefix == null) {

                // ブランク規定文字列を連結
                $serverRequestBuf = $serverRequestBuf . $this->blankStr;
            } else {

                // Indexプレフィックス連結
                $serverRequestBuf = $serverRequestBuf . $this->dataEncoding($prefix);
            }

            // サーバ送信
            @fputs($this->socket, $serverRequestBuf . "\n");

            $serverRetStr = @fgets($this->socket);
            $serverRetStr = str_replace("\r", "", $serverRetStr);
            $serverRetStr = str_replace("\n", "", $serverRetStr);
            $serverRet = explode($this->sepStr, $serverRetStr);


            // 処理の妥当性確
            if ($serverRet[0] === "43") {
                if ($serverRet[1] === "true") {

                    // データ有り
                    $ret[0] = $serverRet[1];

                    $keys = null;

                    $serverRet[2] = str_replace("\n", "", $serverRet[2]);
                    $keys = explode($this->tagKeySep, $serverRet[2]);

                    $cnvKeys = array();
                    for ($i = 0; $i < count($keys); $i++) {
                        $cnvKeys[$i] = $this->dataDecoding($keys[$i]);
                    }
                    $ret[1] = $cnvKeys;
                } else if($serverRet[1] === "false") {

                    // データなし
                    $ret[0] = $serverRet[1];
                    $ret[1] = null;
                } else if($serverRet[1] === "error") {

                    // エラー発生
                    $ret[0] = $serverRet[1];
                    $ret[1] = $serverRet[2];
                }
            }  else {
                if ($this->masterNodesList != null && count($this->masterNodesList) > 1) {
                    if($this->autoConnect()) {
                        $ret = $this->searchValue($searchCharacterList, $searchType, $prefix);
                    }
                } else {

                    // 妥当性違反
                    throw new OkuyamaClientException("Execute Violation of validity");
                }
            }
        } catch (OkuyamaClientException $oe) {
            throw $oe;
        } catch (Exception $e) {
            if ($this->masterNodesList != null && count($this->masterNodesList) > 1) {
                if($this->autoConnect()) {
                    $ret = $this->searchValue($searchCharacterList, $searchType, $prefix);
                }
            } else {

                throw $e;
            }
        }
        return $ret;
    }


    /**
     * マスタサーバからKeyでデータを削除する.<br>
     * 取得値のエンコーディング指定が可能.<br>
     *
     * @param keyStr 削除対象のKey値文字列
     * @return String[] 削除したデータ 内容) 要素1(データ削除有無):"true" or "false",要素2(削除データ):"データ文字列"
     * @throws OkuyamaClientException, Exception
     */
    public function removeValue($keyStr, $encoding="UTF-8")  {
        $ret = array(); 
        $serverRetStr = null;
        $serverRet = null;

        $serverRequestBuf = null;

        try {
            if ($this->socket == null) throw new OkuyamaClientException("No ServerConnect!!");

            // エラーチェック
            // Keyに対する無指定チェック
            if ($keyStr == null ||  $keyStr === "") {
                throw new OkuyamaClientException("The blank is not admitted on a key");
            }

            // 文字列バッファ初期化
            $serverRequestBuf = "";

            // 処理番号連結
            $serverRequestBuf = $serverRequestBuf . "5";
            // セパレータ連結
            $serverRequestBuf = $serverRequestBuf . $this->sepStr;
            // Key連結(Keyはデータ送信時には必ず文字列が必要)
            $serverRequestBuf = $serverRequestBuf . $this->dataEncoding($keyStr);
            // セパレータ連結
            $serverRequestBuf = $serverRequestBuf . $this->sepStr;
            // TransactionCode連結
            $serverRequestBuf = $serverRequestBuf . $this->transactionCode;

            // サーバ送信
            @fputs($this->socket, $serverRequestBuf . "\n");

            $serverRetStr = @fgets($this->socket);
            $serverRetStr = str_replace("\r", "", $serverRetStr);
            $serverRetStr = str_replace("\n", "", $serverRetStr);
            $serverRet = explode($this->sepStr, $serverRetStr);

            // 処理の妥当性確認
            if ($serverRet[0] === "5") {
                if ($serverRet[1] === "true") {

                    // データ有り
                    $ret[0] =$serverRet[1];

                    // Valueがブランク文字か調べる
                    if ($serverRet[2] === $this->blankStr) {
                        $ret[1] = "";
                    } else {

                        // Value文字列をBase64でデコード
                        $ret[1] = $this->dataDecoding($serverRet[2], $encoding);
                    }
                } else if($serverRet[1] === "false") {

                    // データなし
                    $ret[0] = $serverRet[1];
                    $ret[1] = null;
                } else if($serverRet[1] === "error") {

                    // エラー発生
                    $ret[0] = $serverRet[1];
                    $ret[1] = $serverRet[2];
                }
            } else {
                if ($this->masterNodesList != null && count($this->masterNodesList) > 1) {
                    if($this->autoConnect()) {
                        $ret = $this->removeValue($keyStr, $encoding);
                    }
                } else {
                
                    // 妥当性違反
                    throw new OkuyamaClientException("Execute Violation of validity");
                }
            }
        } catch (OkuyamaClientException $oe) {
            throw $oe;
        } catch (Exception $e) {
            if ($this->masterNodesList != null && count($this->masterNodesList) > 1) {
                if($this->autoConnect()) {
                    $ret = $this->removeValue($keyStr, $encoding);
                }
            } else {

                throw $e;
            }
        }
        return $ret;
    }


    /**
     * MasterNodeへKey値とTag値を指定してTagの紐付きを削除する.<br>
     *
     * @param keyStr Key値文字列
     * @param tagStr Tag値文字列
     * @return boolean 削除成否
     * @throws OkuyamaClientException, Exception
     */
    public function removeTagFromKey($keyStr, $tagStr) {
        $ret = false; 
        $serverRetStr = null;
        $serverRet = null;
        $serverRequestBuf = null;
        try {
            // Byte Lenghtチェック
            if ($this->checkStrByteLength($keyStr) > $this->maxKeySize) throw new OkuyamaClientException("Save Key Max Size " . $this->maxKeySize . " Byte");
            if ($tagStr != null) {
                if ($this->checkStrByteLength($tagStr) > $this->maxKeySize) throw new OkuyamaClientException("Tag Max Size " . $this->maxKeySize . " Byte");
            } 

            if ($this->socket == null) throw new OkuyamaClientException("No ServerConnect!!");

            // エラーチェック
            // Keyに対する無指定チェック
            if ($keyStr == null ||  $keyStr === "") {
                throw new OkuyamaClientException("The blank is not admitted on a key");
            }


            // エラーチェック
            // Tagに対する無指定チェック
            if ($tagStr == null ||  $tagStr === "") {
                throw new OkuyamaClientException("The blank is not admitted on a tag");
            }

            // 文字列バッファ初期化
            $serverRequestBuf = "";


            // 処理番号連結
            $serverRequestBuf = "40";
            // セパレータ連結
            $serverRequestBuf = $serverRequestBuf . $this->sepStr;

            // Tag連結(Tagはデータ送信時には必ず文字列が必要)
            $serverRequestBuf = $serverRequestBuf. $this->dataEncoding($tagStr);

            // セパレータ連結
            $serverRequestBuf = $serverRequestBuf . $this->sepStr;

            // Key連結(Keyはデータ送信時には必ず文字列が必要)
            $serverRequestBuf = $serverRequestBuf. $this->dataEncoding($keyStr);

            // セパレータ連結
            $serverRequestBuf = $serverRequestBuf . $this->sepStr;

            // TransactionCode連結
            $serverRequestBuf = $serverRequestBuf . $this->transactionCode;


            // サーバ送信
            @fputs($this->socket, $serverRequestBuf . "\n");

            $serverRetStr = @fgets($this->socket);

            $serverRetStr = str_replace("\r", "", $serverRetStr);
            $serverRetStr = str_replace("\n", "", $serverRetStr);
            $serverRet = explode($this->sepStr, $serverRetStr);

            // 処理の妥当性確認
            if (count($serverRet) == 3 && $serverRet[0] === "40" && $serverRet[2] === "") {
                if ($serverRet[1] === "true") {

                    // 処理成功
                    $ret = true;
                } else{

                    // 処理失敗(データなし)
                    $ret = false;
                }
            }  else {
                if ($this->masterNodesList != null && count($this->masterNodesList) > 1) {
                    if($this->autoConnect()) {
                        $ret = $this->removeTagFromKey($keyStr, $tagStr);
                    }
                } else {
                
                    // 妥当性違反
                    throw new OkuyamaClientException("Execute Violation of validity");
                }
            }
        } catch (OkuyamaClientException $oe) {
            throw $oe;
        } catch (Exception $e) {
            if ($this->masterNodesList != null && count($this->masterNodesList) > 1) {
                if($this->autoConnect()) {
                    $ret = $this->removeTagFromKey($keyStr, $tagStr);
                }
            } else {

                throw $e;
            }
        }
        return $ret;
    }


    /**
     * 全文検索用のIndexを削除する。
     * Prefixあり<br>
     * 検索Index長さ指定あり<br>
     *
     *
     * @param keyStr Key値文字列
     * @param indexPrefix 作成時に設定したIndexのPrefix値
     * @param indexLength 作成時に指定した作成Indexの長さ指定
     * @return boolean 削除成否
     * @throws OkuyamaClientException, Exception
     */
    public function removeSearchIndex($keyStr, $indexPrefix = null, $indexLength = 3) {
        $ret = false; 
        $serverRetStr = null;
        $serverRet = null;
        $serverRequestBuf = null;
        try {
            // Byte Lenghtチェック
            if ($this->checkStrByteLength($keyStr) > $this->maxKeySize) throw new OkuyamaClientException("Save Key Max Size " . $this->maxKeySize . " Byte");
            if ($tagStr != null) {
                if ($this->checkStrByteLength($tagStr) > $this->maxKeySize) throw new OkuyamaClientException("Tag Max Size " . $this->maxKeySize . " Byte");
            } 

            if ($this->socket == null) throw new OkuyamaClientException("No ServerConnect!!");

            // エラーチェック
            // Keyに対する無指定チェック
            if ($keyStr == null ||  $keyStr === "") {
                throw new OkuyamaClientException("The blank is not admitted on a key");
            }



            // 文字列バッファ初期化
            $serverRequestBuf = "";


            // 処理番号連結
            $serverRequestBuf = "44";

            // セパレータ連結
            $serverRequestBuf = $serverRequestBuf . $this->sepStr;
            // Key連結(Keyはデータ送信時には必ず文字列が必要)

            $serverRequestBuf = $serverRequestBuf. $this->dataEncoding($keyStr);

            // セパレータ連結
            $serverRequestBuf = $serverRequestBuf . $this->sepStr;
            // TransactionCode連結
            $serverRequestBuf = $serverRequestBuf . $this->transactionCode;

            // セパレータ連結
            $serverRequestBuf = $serverRequestBuf . $this->sepStr;
            // プレフィックスを有無に合わせて連結
            if ($indexPrefix == null ||  $indexPrefix === "") {

                // ブランク規定文字列を連結
                $serverRequestBuf = $serverRequestBuf . $this->blankStr;
            } else {

                // Indexプレフィックス連結
                $serverRequestBuf = $serverRequestBuf . $this->dataEncoding($indexPrefix);
            }

            // セパレータ連結
            $serverRequestBuf = $serverRequestBuf . $this->sepStr;
            // createIndexLen連結
            $serverRequestBuf = $serverRequestBuf . $indexLength;


            // サーバ送信
            @fputs($this->socket, $serverRequestBuf . "\n");

            $serverRetStr = @fgets($this->socket);

            $serverRetStr = str_replace("\r", "", $serverRetStr);
            $serverRetStr = str_replace("\n", "", $serverRetStr);
            $serverRet = explode($this->sepStr, $serverRetStr);

            // 処理の妥当性確認
            if ($serverRet[0] === "44") {
                if ($serverRet[1] === "true") {

                    // 処理成功
                    $ret = true;
                } else{

                    // 処理失敗(データなし)
                    $ret = false;
                }
            }  else {
                if ($this->masterNodesList != null && count($this->masterNodesList) > 1) {
                    if($this->autoConnect()) {
                        $ret = $this->removeSearchIndex($keyStr, $indexPrefix, $indexLength);
                    }
                } else {
                
                    // 妥当性違反
                    throw new OkuyamaClientException("Execute Violation of validity");
                }
            }
        } catch (OkuyamaClientException $oe) {
            throw $oe;
        } catch (Exception $e) {
            if ($this->masterNodesList != null && count($this->masterNodesList) > 1) {
                if($this->autoConnect()) {
                    $ret = $this->removeSearchIndex($keyStr, $indexPrefix, $indexLength);
                }
            } else {

                throw $e;
            }
        }
        return $ret;
    }


    /**
     * マスタサーバからKeyでデータを取得する(バイナリ).<br>
     *
     * @param keyStr Key値文字列
     * @return Object[] 要素1(String)(データ有無):"true" or "false",要素2(byte[])(データ):{バイト配列}
     * @throws OkuyamaClientException, Exception
     */
    public function getByteValue($keyStr) {
        try {
            $workKeyRet = $this->getValue($keyStr);
            if ($workKeyRet[0] === "true") {
                $workKeyRet = explode($this->byteDataKeysSep, $workKeyRet[1]);
                
                if (count($workKeyRet) > 1) {
                    $keyWork = explode("_", $workKeyRet[1]);
                    
                    $keyStrPre = $keyWork[0];
                    //右辺のIndex数値+1が配列サイズ
                    $maxKeyIndexSize = (int)($keyWork[1] + 1);

                    $workKeyRet = "";
                    for($i=0; $i<$maxKeyIndexSize; $i++) {
                        $workKeyRet[$i] = $keyStrPre."_".$i;
                    }
                    
                    $ret = array();
                    $ret[0] = "true";
                    $ret[1] = "";
                    foreach($workKeyRet as $val) {
                        $rettmp = $this->getByteData($val);
                        if ($rettmp[0] ===  "true") {
                            $ret[1] .= $rettmp[1];
                        } else {
                            //エラー処理
                            $ret[0] = "false";
                            break;
                        }
                    }
                }
            }

        } catch (Exception $e) {
            throw $e;
        }
        return $ret;
    }


    /**
     * マスタサーバからKeyでデータを取得する(バイナリ).<br>
     *
     * @param  $keyStr Key値文字列
     * @return Object[] 要素1(String)(データ有無):"true" or "false",要素2(byte[])(データ):{バイト配列}
     * @throws OkuyamaClientException, Exception
     */
    private function getByteData($keyStr) {
        $ret = array(); 
        $serverRetStr = null;
        $serverRet = null;

        $serverRequestBuf = null;

        try {
            if ($this->socket == null) throw new OkuyamaClientException("No ServerConnect!!");

            // エラーチェック
            // Keyに対する無指定チェック
            if ($keyStr == null ||  $keyStr === "") {
                throw new OkuyamaClientException("The blank is not admitted on a key");
            }

            // 文字列バッファ初期化
            $serverRequestBuf = "";

            // 処理番号連結
            $serverRequestBuf = $serverRequestBuf . "2";
            // セパレータ連結
            $serverRequestBuf = $serverRequestBuf . $this->sepStr;

            // Key連結(Keyはデータ送信時には必ず文字列が必要)
            $serverRequestBuf = $serverRequestBuf . $this->dataEncoding($keyStr);

            // サーバ送信
            @fputs($this->socket, $serverRequestBuf . "\n");

            $serverRetStr = @fgets($this->socket);
            $serverRetStr = str_replace("\r", "", $serverRetStr);
            $serverRetStr = str_replace("\n", "", $serverRetStr);
            $serverRet = explode($this->sepStr, $serverRetStr);

            // 処理の妥当性確認
            if ($serverRet[0] === "2") {
                if ($serverRet[1] === "true") {

                    // データ有り
                    $ret[0] =$serverRet[1];

                    // Valueがブランク文字か調べる
                    if ($serverRet[2] === $this->blankStr) {
                        $ret[1] = "";
                    } else {

                        // Value文字列をBase64でデコード
                        $ret[1] = $this->dataDecoding($serverRet[2]);
                    }
                } else if($serverRet[1] === "false") {

                    // データなし
                    $ret[0] = $serverRet[1];
                    $ret[1] = null;
                } else if($serverRet[1] === "error") {

                    // エラー発生
                    $ret[0] = $serverRet[1];
                    $ret[1] = $serverRet[2];
                }
            } else {
                if ($this->masterNodesList != null && count($this->masterNodesList) > 1) {
                    if($this->autoConnect()) {
                        $ret = $this->getByteData($keyStr);
                    }
                } else {
                
                    // 妥当性違反
                    throw new OkuyamaClientException("Execute Violation of validity");
                }
            }
        } catch (OkuyamaClientException $oe) {
            throw $oe;
        } catch (Exception $e) {
            if ($this->masterNodesList != null && count($this->masterNodesList) > 1) {
                if($this->autoConnect()) {
                    $ret = $this->getByteData($keyStr);
                }
            } else {

                throw $e;
            }
        }
        return $ret;
    }

    // 文字列の長さを返す
    private function checkStrByteLength($targetStr) {

        $ret = 0;
        $strWidth = strlen($targetStr);

        //$strWidth = mb_strlen($targetStr);
        //$ret = $strWidth = mb_strlen($targetStr);
        return $strWidth;
    }

    // BASE64でエンコードする
    private function dataEncoding($val){
        return base64_encode($val);
    }


    // BASE64でデコードする
    private function dataDecoding($val){
        return base64_decode($val);
    }

}

class OkuyamaClientException extends Exception {
    public function __construct($msg) {
        parent::__construct($msg);
    }
}
?>