rem ファイル取得用バッチファイル
rem 引数は1つ目はローカルに展開されるファイルのフルパス(c:\restore.txt)
rem 引数は2つ目は取得するキー値(登録時のキー)
java -Xmx256m -cp ./;./classes;./lib/javamail-1.4.1.jar TestSock 6 127.0.0.1 8888 1 %1 %2