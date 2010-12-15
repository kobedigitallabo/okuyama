okuyamaサーバインストールパッケージ
※Linux環境を想定
※事前に展開したトップディレクトリにてantタスクにて"jar"を実行する
  okuyama-version.jarがbinディレクトリ配下に作成されます
  $ant jar

本ファイル配下のディレクトリ、ファイルをすべてをサーバに配置する
(説明では/home/okuyama/okuyamaにインストールした想定)
1.環境変数OKUYAMA_HOMEを配置ディレクトリとして設定する
  例)
    OKUYAMA_HOME=/home/okuyama/okuyam
    export OKUYAMA_HOME

2.bin/okuyamaにパスを通す

3.DataNode.propertiesのファイルパス指定を変更する

上記まで完了すれば以下のコマンドで起動、停止可能
起動時引数
1.動作指定: start 
2.バックグラウンド指定(-server)
3.起動用設定ファイル

okuyama start -server /home/okuyama/okuyama/conf/MasterNode.properties


停止時引数
1.動作指定: stop 
2.停止用ポート番号(MasterNode.propertiesおよび、DataNode.propertiesの"ServerControllerHelper.Init="で指定している":"より右辺の番号)

okuyama stop 18888
