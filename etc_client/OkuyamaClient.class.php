<?php
/**
 * PHP用のクライアント.<br>
 * Javaをそのまま焼きなおしました.<br>
 * PHPのコーディングルールに従っていない.<br>
 * サーバ接続部分のエラーがうまくハンドリグ出来ていない.<br>
 *
 * TODO:バイトデータ格納が未実装.<br>
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
    private $saveSize = 8192;

    // 保存できる最大長
    private $maxValueSize = 8192;

    // MasterServerへの接続時のタイムアウト時間
    private $connectTimeOut = 3;

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
                    $ret = true;
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
     * マスタサーバへデータを送信する.<br>
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
        try {
            // Byte Lenghtチェック
            if ($this->chechStrByteLength($keyStr) > $this->maxValueSize) throw new Exception("Save Key Max Size " . $this->maxValueSize . " Byte");
            if ($tagStrs != null) {
                for ($i = 0; $i < count($tagStrs); $i++) {
                    if ($this->chechStrByteLength($tagStrs[$i]) > $this->maxValueSize) throw new Exception("Tag Max Size " . $this->maxValueSize . " Byte");
                }
            }
            if ($this->chechStrByteLength($value) > $this->maxValueSize) throw new Exception("Save Value Max Size " . $this->maxValueSize . " Byte");

            if ($this->socket == null) throw new Exception("No ServerConnect!!");

            // エラーチェック
            // Keyに対する無指定チェック
            if ($keyStr == null ||  $keyStr === "") {
                throw new Exception("The blank is not admitted on a key");
            }

            // valueに対する無指定チェック(Valueはnullやブランクの場合は代行文字列に置き換える)
            if ($value == null ||  $value === "") {

                $value = $this->blankStr;
            } else {

                // ValueをBase64でエンコード
                $value = $this->dataEncoding($value);
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
            $serverRequestBuf = $serverRequestBuf . $value;

            // サーバ送信
            @fputs($this->socket, $serverRequestBuf . "\n");

            $serverRetStr = @fgets($this->socket);
            $serverRetStr = str_replace("\n", "", $serverRetStr);
            $serverRet = explode($this->sepStr, $serverRetStr);

            // 処理の妥当性確認
            if (count($serverRet) == 3 && $serverRet[0] === "1") {
                if ($serverRet[1] === "true") {

                    // 処理成功
                    $ret = true;
                } else{

                    // 処理失敗(メッセージ格納)
                    throw new Exception($serverRet[1]);
                }
            }  else {
                if ($this->masterNodesList != null && count($this->masterNodesList) > 1) {
                    if($this->autoConnect()) {
                        $ret = $this->setValue($keyStr, $value, $tagStrs);
                    }
                } else {
                
                    // 妥当性違反
                    throw new Exception("Execute Violation of validity");
                }
            }
        } catch (Exception $e) {
            throw $e;
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
            if ($this->socket == null) throw new Exception("No ServerConnect!!");

            // エラーチェック
            // Keyに対する無指定チェック
            if ($keyStr == null ||  $keyStr === "") {
                throw new Exception("The blank is not admitted on a key");
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
                    throw new Exception("Execute Violation of validity");
                }
            }

        } catch (Exception $e) {
            throw $e;
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
            if ($this->socket == null) throw new Exception("No ServerConnect!!");

            // エラーチェック
            // Keyに対する無指定チェック
            if ($keyStr == null ||  $keyStr === "") {
                throw new Exception("The blank is not admitted on a key");
            }

            // Keyに対する無指定チェック
            if ($scriptStr == null ||  $scriptStr === "") {
                throw new Exception("The blank is not admitted on a Script");
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
                    throw new Exception("Execute Violation of validity");
                }
            }

        } catch (Exception $e) {
            throw $e;
        }
        return $ret;
    }


    /**
     * マスタサーバからTagでKey値群を取得する.<br>
     *
     * @param tagStr
     * @return Object[] 要素1(データ有無):"true" or "false",要素2(データ):"データ文字列"
     * @throws Exception
     */
    public function getTagKeys($tagStr) {
        $ret = array(); 
        $serverRetStr = null;
        $serverRet = null;

        $serverRequestBuf = null;

        try {
            if ($this->socket == null) throw new Exception("No ServerConnect!!");

            // エラーチェック
            // Keyに対する無指定チェック
            if ($tagStr == null ||  $tagStr === "") {
                throw new Exception("The blank is not admitted on a tag");
            }

            // 文字列バッファ初期化
            $serverRequestBuf = "";

            // 処理番号連結
            $serverRequestBuf = $serverRequestBuf . "3";

            // セパレータ連結
            $serverRequestBuf = $serverRequestBuf . $this->sepStr;


            // tag連結(Keyはデータ送信時には必ず文字列が必要)
            $serverRequestBuf = $serverRequestBuf . $this->dataEncoding($tagStr);


            // サーバ送信
            @fputs($this->socket, $serverRequestBuf . "\n");

            $serverRetStr = @fgets($this->socket);
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
                        $cnvTags = null;

                        $serverRet[2] = str_replace("\n", "", $serverRet[2]);
                        $tags = explode($this->tagKeySep, $serverRetStr);

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
                    throw new Exception("Execute Violation of validity");
                }
            }
        } catch (Exception $e) {
            throw $e;
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
            if ($this->socket == null) throw new Exception("No ServerConnect!!");

            // エラーチェック
            // Keyに対する無指定チェック
            if ($keyStr == null ||  $keyStr === "") {
                throw new Exception("The blank is not admitted on a key");
            }

            // 文字列バッファ初期化
            $serverRequestBuf = "";

            // 処理番号連結
            $serverRequestBuf = $serverRequestBuf . "5";
            // セパレータ連結
            $serverRequestBuf = $serverRequestBuf . $this->sepStr;

            // Key連結(Keyはデータ送信時には必ず文字列が必要)
            $serverRequestBuf = $serverRequestBuf . $this->dataEncoding($keyStr);

            // サーバ送信
            @fputs($this->socket, $serverRequestBuf . "\n");

            $serverRetStr = @fgets($this->socket);
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
                        $ret = $this->getValue($keyStr, $encoding);
                    }
                } else {
                
                    // 妥当性違反
                    throw new Exception("Execute Violation of validity");
                }
            }

        } catch (Exception $e) {
            throw $e;
        }
        return $ret;
    }

    // 文字列の長さを返す
    private function chechStrByteLength($targetStr) {
        $ret = 0;
        //$strWidth = mb_strlen($targetStr);
        $ret = $strWidth = mb_strlen($targetStr);
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
?>