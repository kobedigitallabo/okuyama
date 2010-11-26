okuyamaサーバインストールパッケージ
※Linux環境を想定

本ファイル配下のディレクトリ、ファイルをすべてをサーバに配置する
環境変数OKUYAMA_HOMEを配置ディレクトリとして設定する
bin/okuyamaにパスを通す
DataNode.propertiesのファイルパス指定を変更する(本ファイルは/home/okuyama/okuyamaにインストールした想定)

上記まで完了すれば以下のコマンドで起動、停止可能
起動時引数
1.動作指定: start 
2.バックグラウンド指定(-server or 省略)
3.起動用設定ファイル

okuyama start -server /home/okuyama/okuyama/conf/MasterNode.properties



停止時引数
1.動作指定: stop 
2.停止用ポート番号(MasterNode.propertiesおよび、DataNode.propertiesの"ServerControllerHelper.Init="で指定している":"より右辺の番号)

okuyama stop 18888
