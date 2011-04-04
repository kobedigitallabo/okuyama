<?php

  // PHPテストスクリプト
  // PHPでのデータ登録
  // 引数は
  // 1.実行モード:"1" or "1.1" ro "2" or "2.1" or "2.3" or "3" or "4" or "8"
  // (1=登録(自動インクリメント), 1.1=登録(値指定), 2=データ取得(自動インクリメント), 2.1=取得(値指定), 2.3=取得(JavaScript実行), 3=Tagで値をKeyとValueを登録(自動インクリメント), 4=Tagで値をKeyを取得(Tag値指定), 8=Key値を指定で削除))
  // 2.マスターノードIP:127.0.0.1
  // 3.マスターノードPort:8888
  // 4.実行回数:0～n(実行モード1.1及び2.1及び2.3及び3及び8時は登録、取得、削除したいKey or Tag値)
  // 5.登録データ:(実行モード1.1及び2.3時のみ有効 1.1時は登録したいValue値、2.3時は実行したいJavaScript)

    require_once("OkuyamaClient.class.php");

  if ($argc > 3) {

    // クライアント作成
    $client = new OkuyamaClient();

    // 接続(MasterServer固定)
    if(!$client->connect($argv[2], $argv[3])) {
      print_r("Sever Connection refused !!");
      exit;
    }

/*
    // AutoConnectモードで接続
    $serverInfos = array();
    $serverInfos[0] = "localhost:8888";
    $serverInfos[1] = "localhost:8889";
    // 候補のMasterServerの情報を設定
    $client->setConnectionInfos($serverInfos);

    // 自動接続
    if(!$client->autoConnect()) {
      // 全ての候補のサーバにつながらない
      print_r("Sever Connection refused !!");
      exit;
    }
*/
    // 分岐
    if ($argv[1] === "1") {

      // データを引数の回数分登録
      for ($i = 0; $i < $argv[4]; $i++) {
        
        if(!$client->setValue("datasavekey_" . $i, "savedatavaluestr_" . $i)) {
          print_r("Registration failure");
        }
      }
    } else if ($argv[1] === "1.1") {

      // データを引数の回数分登録
      if(!$client->setValue($argv[4], $argv[5])) {
        print_r("Regist Error");
      }

    } else if ($argv[1] === "42") {

      // データを引数の回数分登録
      for ($i = 0; $i < $argv[4]; $i++) {
        
        if(!$client->setValueAndCreateIndex("datasavekey_" . $i, "savedatavaluestr_" . $i)) {
          print_r("Registration failure");
        }
      }
    } else if ($argv[1] === "42.1") {

      // データを引数の回数分登録
      for ($i = 0; $i < $argv[4]; $i++) {
        
        if(!$client->setValueAndCreateIndex("datasavekey_" . $argv[5] . "_" . $i, "savedatavaluestr_" . $argv[5] . "_" . $i, null, $argv[5])) {
          print_r("Registration failure");
        }
      }
    } else if ($argv[1] === "43") {


      // データを検索Wordで取得(検索Word1つ、AND,OR、Prefix指定)
      $searchWordList = array();
      $searchWordList[0] = $argv[4];

      var_dump($client->searchValue($searchWordList, $argv[5], $argv[6]));
    } else if ($argv[1] === "43.1") {


      // データを検索Wordで取得(検索Word2つ、AND,OR、Prefix指定)
      $searchWordList = array();
      $searchWordList[0] = $argv[4];
      $searchWordList[1] = $argv[5];
      var_dump($client->searchValue($searchWordList, $argv[6], $argv[7]));
    } else if ($argv[1] === "43.2") {


      // データを検索Wordで取得(検索Word2つ、AND,OR指定)
      $searchWordList = array();
      $searchWordList[0] = $argv[4];
      $searchWordList[1] = $argv[5];
      var_dump($client->searchValue($searchWordList, $argv[6]));
    } else if ($argv[1] === "2") {

      // データを引数の回数分取得
      for ($i = 0; $i < $argv[4]; $i++) {
        $ret = $client->getValue("datasavekey_" . $i);
        if ($ret[0] === "true") {
          print_r($ret[1]);
          print_r("\r\n");
        } else {
          print_r("There is no data");
          print_r("\r\n");
        }
      }
    } else if ($argv[1] === "2.1") {
      // 指定のKey値でデータを取得

      $ret = $client->getValue($argv[4]);
      if ($ret[0] === "true") {
        print_r($ret[1]);
        print_r("\r\n");
      } else if ($ret[0] === "false") {
        print_r("There is no data");
        print_r("\r\n");
      }
    } else if ($argv[1] === "2.3") {
      // 指定のKey値でデータを取得

      $ret = $client->getValueScript($argv[4], $argv[5]);
      if ($ret[0] === "true") {
        print_r($ret[1]);
        print_r("\r\n");
      } else if ($ret[0] === "false") {
        print_r("There is no data");
        print_r("\r\n");
      }
    } else if ($argv[1] === "2.4") {
      // 指定のKey値でデータを取得

      $ret = $client->getValueScriptForUpdate($argv[4], $argv[5]);
      if ($ret[0] === "true") {
        print_r($ret[1]);
        print_r("\r\n");
      } else if ($ret[0] === "false") {
        print_r("There is no data");
        print_r("\r\n");
      }
    } else if ($argv[1] === "3") {

      // データを引数の回数分登録(Tagを登録)
      $counter = 0;
      for ($i = 0; $i < $argv[4]; $i++) {
        $tags = array();
        if ($counter === 0) {
          $tags[0] = "tag1";
          $counter++;
        } else if($counter === 1) {
          $tags[0] = "tag1";
          $tags[1] = "tag2";
          $counter++;
        } else if($counter === 2) {
          $tags[0] = "tag1";
          $tags[1] = "tag2";
          $tags[2] = "tag3";
          $counter++;
        } else if($counter === 3) {
          $tags[0] = "tag4";
          $counter++;
        } else if($counter === 4) {
          $tags[0] = "tag4";
          $tags[1] = "tag2";
          $counter = 0;
        }
        if(!$client->setValue("datasavekey_" . $i, "savedatavaluestr_" . $i, $tags)) {
          print_r("Registration failure");
        }
      }
    } else if ($argv[1] === "4") {

      // データを引数の回数分取得(Tagで取得)
      $counter = 0;
      if ($argv[5] === "true") {
        var_dump($client->getTagKeys($argv[4], true));
      } else if($argv[5] === "false") {
        var_dump($client->getTagKeys($argv[4], false));
      } else {
        var_dump($client->getTagKeys($argv[4]));
      }


    } else if ($argv[1] === "7") {

      // データを引数の回数分取得
      for ($i = 0; $i < $argv[4]; $i++) {
        $ret = $client->removeValue("datasavekey_" . $i);
        if ($ret[0] === "true") {
          // 削除成功
          print_r($ret[1]);
          print_r("\r\n");
        } else if ($ret[0] === "false") {
          // Key値でデータなし
          print_r("There is no data");
          print_r("\r\n");
        } else if ($ret[0] === "error") {
          // 削除処理でエラー
          print_r($ret[1]);
          print_r("\r\n");
        }
      }
    } else if ($argv[1] === "8") {

      // 引数のKey値のデータを削除
      $ret = $client->removeValue($argv[4]);
      if ($ret[0] === "true") {
        // 削除成功
        print_r($ret[1]);
        print_r("\r\n");
      } else if ($ret[0] === "false") {
        // Key値でデータなし
        print_r("There is no data");
        print_r("\r\n");
      } else if ($ret[0] === "error") {
        // 削除処理でエラー
        print_r($ret[1]);
        print_r("\r\n");
      }
    } else if ($argv[1] === "9") {
      if(!$client->startTransaction()) {

        print_r("Transaction Start Error !!");
        exit;
      }
      // 引数のKey値のデータを削除

      $ret = $client->lockData($argv[4], $argv[5], $argv[6]);
      if ($ret[0] === "true") {
        // Lock成功
        print_r("Lock成功");
        print_r("\r\n");
      } else if ($ret[0] === "false") {
        // Key値でデータなし
        print_r("Lock失敗");
        print_r("\r\n");
        } else if ($ret[0] === "error") {
        // 削除処理でエラー
        print_r("Lock Error");
        print_r("\r\n");
      }

      // 自身でロックしているので更新可能
      if(!$client->setValue($argv[4], "LockDataPhp")) {
        print_r("Registration failure");
      }

      $ret = $client->getValue($argv[4]);
      if ($ret[0] === "true") {
        print_r($ret[1]);
        print_r("\r\n");
      } else {
        print_r("There is no data");
        print_r("\r\n");
      }

      // 自身でロックしているので削除可能
      $ret = $client->removeValue($argv[4]);
      if ($ret[0] === "true") {
        // 削除成功
        print_r("削除 [" . $ret[1] . "]");
        print_r("\r\n");
      } else if ($ret[0] === "false") {
        // Key値でデータなし
        print_r("There is no data");
        print_r("\r\n");
      } else if ($ret[0] === "error") {
        // 削除処理でエラー
        print_r($ret[1]);
        print_r("\r\n");
      }

      $ret = $client->releaseLockData($argv[4]);
      var_dump($ret);

    } else if ($argv[1] === "10") {
      // データを引数の回数分登録
      var_dump($client->setNewValue($argv[4], $argv[5]));

    } else if ($argv[1] === "11") {
      // gets
      var_dump($client->getValueVersionCheck($argv[4]));

    } else if ($argv[1] === "12") {
      // cas
      var_dump($client->setValueVersionCheck($argv[4], $argv[5], null, $argv[6]));

    } else if ($argv[1] === "13") {
      // cas tag付
      var_dump($client->setValueVersionCheck($argv[4], $argv[5], $argv[6], $argv[7]));

    } else if ($argv[1] === "20") {

      // incr
      var_dump($client->incrValue($argv[4], $argv[5]));
    } else if ($argv[1] === "21") {

      // decr
      var_dump($client->decrValue($argv[4], $argv[5]));
    } else if ($argv[1] === "22") {

      // Tag削除
      var_dump($client->removeTagFromKey($argv[4], $argv[5]));
    }




    $client->close();
  } else {
    print_r("Args Error!!");
  }
?>