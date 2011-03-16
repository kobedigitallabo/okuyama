rem ファイル登録用バッチファイル
rem 引数は1つ目は保存したいファイルのフルパス
rem 引数は2つ目は保存するキー値
java -Xmx256m -cp ./;./classes;./lib/javamail-1.4.1.jar TestSock 5 127.0.0.1 8888 1 %1 %2