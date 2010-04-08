rem キー値を自動でインクリメントして10個登録
php PhpTestSock.php 1 127.0.0.1 8888 10
rem キー値をkey_aでバリュー値value_bを登録
php PhpTestSock.php 1.1 127.0.0.1 8888 key_a value_b
rem キー値を自動でインクリメントして10個valueを取得
php PhpTestSock.php 2 127.0.0.1 8888 10
rem キー値をkey_aでvalueを取得
php PhpTestSock.php 2.1 127.0.0.1 8888 key_a
rem キー値をkey_aで取得したvalueに対してJavaScriptを実行
php PhpTestSock.php 2.3 127.0.0.1 8888 key_a "var dataValue; var retValue = dataValue.replace('b', 'dummy'); var execRet = '1';"
rem Tag値を自動で変えて、KeyとValueを10回登録
php PhpTestSock.php 3 127.0.0.1 8888 10
rem Tag値をtag1を指定して、tag1に属するKey値を取得
php PhpTestSock.php 4 127.0.0.1 8888 tag1
rem キー値をkey_aでValueを削除
php PhpTestSock.php 8 127.0.0.1 8888 key_a
rem 分散ロックを使用する
php PhpTestSock.php 9 127.0.0.1 8888 key_a 10 5