<?php
    
    $host = "127.0.0.1";
    $port = "8888";
    $sock = fsockopen($host, $port);

    // Set
    $key =  base64_encode("datasavekey_php_0");
    $value = base64_encode("datasavedata_php_0");

    // ValueをBase64でエンコード
    value = new String(BASE64EncoderStream.encode(value.getBytes()));
            }


            // 文字列バッファ初期化
            serverRequestBuf = new StringBuffer();


            // 処理番号連結
            serverRequestBuf.append("1");
            // セパレータ連結
            serverRequestBuf.append(ImdstKeyValueClient.sepStr);


            // Key連結(Keyはデータ送信時には必ず文字列が必要)
            serverRequestBuf.append(new String(BASE64EncoderStream.encode(keyStr.getBytes())));
            // セパレータ連結
            serverRequestBuf.append(ImdstKeyValueClient.sepStr);


            // Tag連結
            // Tag指定の有無を調べる
            if (tagStrs == null || tagStrs.length < 1) {

                // ブランク規定文字列を連結
                serverRequestBuf.append(ImdstKeyValueClient.blankStr);
            } else {

                // Tag数分連結
                serverRequestBuf.append(new String(BASE64EncoderStream.encode(tagStrs[0].getBytes())));
                for (int i = 1; i < tagStrs.length; i++) {
                    serverRequestBuf.append(tagKeySep);
                    serverRequestBuf.append(new String(BASE64EncoderStream.encode(tagStrs[i].getBytes())));
                }
            }

            // セパレータ連結
            serverRequestBuf.append(ImdstKeyValueClient.sepStr);

            // Value連結
            serverRequestBuf.append(value);

            // サーバ送信
            pw.println(serverRequestBuf.toString());
            pw.flush();









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