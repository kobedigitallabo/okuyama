<?php
    // PHPテストスクリプト
    // PHPでのデータ登録
    // 最終的にクライアントを作成予定
    $host = "127.0.0.1";
    $port = "8888";
    $sock = fsockopen($host, $port);

    // Get
    $key =  base64_encode("datasavekey_php_0");

    $request =  "2#imdst3674#" . $key . "\r\n";

    if(!$sock){

        $data = 'socket error：' . $host;
    }else{

        fputs($sock, $request);

        $work = fgets($sock);
        $work = str_replace("\r\n", "", $work);
        $strs = explode("#imdst3674#", $work);

        var_dump(base64_decode($strs[2]));
        fclose($sock);
    }

?>