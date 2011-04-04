<?php
/**
 * PHP用のクライアント.<br>
 * Javaをそのまま焼きなおしました.<br>
 * TODO:PHPのコーディングルールに従っていない.<br>
 * TODO:サーバ接続部分のエラーがうまくハンドリグ出来ていない.<br>
 *
 *
 * ・稼動条件
 *   mbstringが読み込まれている必要があります.<br>
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
    private $maxKeySize = 386;

    // MasterServerへの接続時のタイムアウト時間
    private $connectTimeOut = 10;

    private $errorno;

    private $errormsg;

    /**
     * MasterNodeの接続情報を設定する.<br>
     * 本メソッドでセットし、autoConnect()メソッドを<br>
     * 呼び出すと、自動的にその時稼動しているMasterNodeにバランシングして<br>
     * 接続される。接続出来ない場合は、別のMasterNodeに再接続される.<br>
     *
     * @param masterNodes 接続情報の配列 "IP:PORT"の形式
     */
    public function setConnectionInfos($masterNodes) { 

        if ($balanceType) 
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
                    if ($tmpMasterNodeList[$idx] !== null && $tmpMasterNodeList[$idx] !== "") {
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
   * 今のところは最大保存サイズの初期化のみ<br>
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
     * @throws Exception
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
     *
     * @param keyStr
     * @param tagStrs
     * @param value
     * @return boolean
     * @throws Exception
     */
    public function setValue($keyStr, $value, $tagStrs = null) {
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
                        $ret = $this->setValue($keyStr, $value, $tagStrs);
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
                    $ret = $this->setValue($keyStr, $value, $tagStrs);
                }
            } else {

                throw $e;
            }
        }
        return $ret;
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
     * @param keyStr Key値
     * @param tagStrs Tag値の配列 例){"tag1","tag2","tag3"}
     * @param value value値
     * @param indexPrefix 作成する検索IndexをグルーピングするPrefix文字列.この値と同様の値を指定してsearchValueメソッドを呼び出すと、グループに限定して全文検索が可能となる.最大は128文字
     * @return boolean 登録成否
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
     * @throws Exception
     */
    public function setNewValue($keyStr, $value, $tagStrs = null) {
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
                        $ret = $this->setNewValue($keyStr, $value, $tagStrs);
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
                    $ret = $this->setNewValue($keyStr, $value, $tagStrs);
                }
            } else {

                throw $e;
            }
        }
        return $ret;
    }

    /**
     * マスタサーバへデータを送信する.<br>
     * バージョンチェックを行う.<br>
     * Tag有り.<br>
     * memcachedのcasに相当する.<br>
     *
     * @param keyStr
     * @param value
     * @param tagStrs
     * @param versionNo
     * @return boolean
     * @throws Exception
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
     * @param keyStr Key値
     * @param value 加算値
     * @return 要素1(処理成否):Boolean true/false,要素2(演算後の結果):double 数値
     * @throws Exception
     */
    public function incrValue($keyStr, $value) {
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
     * @param keyStr Key値
     * @param value 減算値
     * @return 要素1(処理成否):Boolean true/false,要素2(演算後の結果):double 数値
     * @throws Exception
     */
    public function decrValue($keyStr, $value) {
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
     * @throws Exception
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
     * 文字列エンコーディング指定あり.<br>
     * Valueのバージョン値を合わせて返す.<br>
     * memcachedのgetsに相当する.<br>
     *
     * @param keyStr
     * @param encoding
     * @return String[] 要素1(データ有無):"true" or "false",要素2(データ):"データ文字列",要素3(Version):"0始まりの数字" 
     * @throws Exception
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
     * 取得データに対してJavaScriptを実行する.<br>
     * 文字列エンコーディング指定あり.<br>
     *
     * @param keyStr
     * @param scriptStr JavaScriptコード
     * @param encoding
     * @return String[] 要素1(データ有無):"true" or "false",要素2(データ):"データ文字列"
     * @throws Exception
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
     * @param keyStr
     * @param scriptStr JavaScriptコード
     * @param encoding
     * @return String[] 要素1(データ有無):"true" or "false",要素2(データ):"データ文字列"
     * @throws Exception
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
     * @param tagStr
     * @param noExistsData Keyが存在しない場合の取得指定 true=過去にtagを登録した場合はKey値は返す false=現時Keyが存在しなければ返却しない
     * @return Object[] 要素1(データ有無):"true" or "false",要素2(データ):"データ文字列"
     * @throws Exception
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
     * MasterNodeからsetValueAndCreateIndexで作成されたIndexを使って検索して該当する値を取得する.<br>
     * 検索可能な文字列は1文字からで、最大は32文字(ソフトリミット).<br>
     * Prefxiあり.<br>
     * 
     * @param searchCharacterList 取得したい値の文字配列(エンコードはUTF-8固定)<文字配列>
     * @param  searchType 1:AND検索　2:OR検索
     * @param  prefix 検索Index作成時に指定したPrefix値
     * @return object[] 要素1(データ有無):"true" or "false",要素2(該当のKey値配列):Stringの配列
     * @throws OkuyamaClientException
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
     * @param keyStr
     * @return String[] 削除したデータ 内容) 要素1(データ削除有無):"true" or "false",要素2(削除データ):"データ文字列"
     * @throws Exception
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
     * @param keyStr Key値
     * @param tagStr tag値
     * @return boolean 削除成否
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
     * マスタサーバからKeyでデータを取得する(バイナリ).<br>
     *
     * @param keyStr
     * @return Object[] 要素1(String)(データ有無):"true" or "false",要素2(byte[])(データ):{バイト配列}
     * @throws Exception
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
     * @param  $keyStr
     * @return Object[] 要素1(String)(データ有無):"true" or "false",要素2(byte[])(データ):{バイト配列}
     * @throws Exception
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
        return $ret;
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