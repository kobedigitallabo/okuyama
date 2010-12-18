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
rem キー値をkey_aで取得したvalueに対してJavaScriptを実行
php PhpTestSock.php 2.4 127.0.0.1 8888 key_a "var dataValue; var dataKey; var retValue = dataValue.replace('b', 'dummy'); if(dataKey == 'key_a') {var execRet = '2'} else {var execRet = '1'}"
rem Tag値を自動で変えて、KeyとValueを10回登録
php PhpTestSock.php 3 127.0.0.1 8888 10
rem Tag値をtag1を指定して、tag1に属するKey値を取得(Key値存在指定有り(true))
php PhpTestSock.php 4 127.0.0.1 8888 tag1 true
rem Tag値をtag1を指定して、tag1に属するKey値を取得(Key値存在指定有り(false))
php PhpTestSock.php 4 127.0.0.1 8888 tag1 false
rem キー値をkey_aでValueを削除
php PhpTestSock.php 8 127.0.0.1 8888 key_a
rem 分散ロックを使用する
php PhpTestSock.php 9 127.0.0.1 8888 key_a 10 5
rem 値の新規登録をおこなう
php PhpTestSock.php 10 127.0.0.1 8888 newkey newvalue
rem gets
php PhpTestSock.php 11 127.0.0.1 8888 newkey
rem cas
php PhpTestSock.php 12 127.0.0.1 8888 newkey value_cas 0
rem cas Miss
php PhpTestSock.php 12 127.0.0.1 8888 newkey value_cas 1
rem cas Tag
php PhpTestSock.php 13 127.0.0.1 8888 newkey value_cas tag1 2
rem gets
php PhpTestSock.php 11 127.0.0.1 8888 newkey
