rem キー値を自動でインクリメントして10個登録
php PhpTestSock.php 1 127.0.0.1 8888 100
rem キー値をkey_aでバリュー値value_bを登録
php PhpTestSock.php 1.1 127.0.0.1 8888 key_a value_b
rem キー値をkey_aでバリュー値value_cを登録-有効期限テスト
php PhpTestSock.php 1.2 127.0.0.1 8888 key_c value_c 3
rem キー値を自動でインクリメントして10個valueを取得
php PhpTestSock.php 2 127.0.0.1 8888 100
rem キー値をkey_aでvalueを取得
php PhpTestSock.php 2.1 127.0.0.1 8888 key_a
rem キー値をkey_aで取得したvalueに対してJavaScriptを実行
php PhpTestSock.php 2.3 127.0.0.1 8888 key_a "var dataValue; var retValue = dataValue.replace('b', 'dummy'); var execRet = '1';"
rem キー値をkey_aで取得したvalueに対してJavaScriptを実行
php PhpTestSock.php 2.4 127.0.0.1 8888 key_a "var dataValue; var dataKey; var retValue = dataValue.replace('b', 'dummy'); if(dataKey == 'key_a') {var execRet = '2'} else {var execRet = '1'}"
rem キー値をkey_aでバリュー値value_dを登録-有効期限設定を行って有効期限到達前に更新での取得
php PhpTestSock.php 2.5 127.0.0.1 8888 key_d value_d 3
rem キー値を複数指定してkeyとvalueの集合を取得
php PhpTestSock.php 2.6 127.0.0.1 8888
rem キー値をkey_objでバリュー値Objectを登録-有効期限設定を行って有効期限到達前に更新での取得
php PhpTestSock.php 2.7 127.0.0.1 8888 key_obj 3
rem Tag値を自動で変えて、KeyとValueを10回登録
php PhpTestSock.php 3 127.0.0.1 8888 100
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
rem get
php PhpTestSock.php 11 127.0.0.1 8888 newkey
rem 値の新規登録をおこなう-有効期限あり
php PhpTestSock.php 10.1 127.0.0.1 8888 newkeyT newvalueT 3
rem cas
php PhpTestSock.php 12 127.0.0.1 8888 newkey valuecas 0
rem cas Miss
php PhpTestSock.php 12 127.0.0.1 8888 newkey valuecas 1
rem cas Tag
php PhpTestSock.php 13 127.0.0.1 8888 newkey valuecas tag1 2
rem gets
php PhpTestSock.php 11 127.0.0.1 8888 newkey
rem incr
php PhpTestSock.php 20 127.0.0.1 8888 newkey 1
rem incr
php PhpTestSock.php 20 127.0.0.1 8888 newkey 109
rem decr
php PhpTestSock.php 21 127.0.0.1 8888 newkey 1
rem decr
php PhpTestSock.php 21 127.0.0.1 8888 newkey 109
rem removeTagFromkey
php PhpTestSock.php 22 127.0.0.1 8888 datasavekey_46 tag1
rem setObjectValue
php PhpTestSock.php 23 127.0.0.1 8888 objectKey1
rem getObjectValue
php PhpTestSock.php 24 127.0.0.1 8888 objectKey1
rem getObjectValue
php PhpTestSock.php 24 127.0.0.1 8888 objectKey2
rem setValueAndCreateIndex
php PhpTestSock.php 42 127.0.0.1 8888 1000
rem searchValue
php PhpTestSock.php 43 127.0.0.1 8888 "datavaluestr_716" 1 ""
rem setValueAndCreateIndex
php PhpTestSock.php 42.1 127.0.0.1 8888 1000 Prefix1
rem searchValue
php PhpTestSock.php 43.1 127.0.0.1 8888 "7" "716" 2 "Prefix1"
rem removeSearchIndex
php PhpTestSock.php 44 127.0.0.1 8888 "datavaluestr_716" 1 ""
rem MaxSizeTest
php PhpTestSock.php size-true 127.0.0.1 8888
rem MaxSizeOverTest
php PhpTestSock.php size-false 127.0.0.1 8888
