okuyamaサーバインストールパッケージ
※Linux環境を想定
※javaにパスが通っていること
※環境変数JAVA_HOMEを作成しておくこと


インストール手順
■手順1)
  事前にokuyama-{version}.zipを展開したトップディレクトリにてantタスクにて"jar"を実行する
  okuyama-{version}.jarが./install/binディレクトリ配下に作成される

  コマンド)
  $ant jar



■手順2)
  本ファイル配下のディレクトリ、ファイルをすべてをサーバに配置する
  (説明では/home/okuyama/okuyamaにインストールした想定)
   配置時のディレクトリ構成
   /home/okuyama/okuyama/
                        │↓
                        ├→bin 
                        │
                        ├→conf
                        │
                        ├→keymapfile
                        │
                        ├→lib
                        │
                        ├→logs
                        │
                        └→log4j.properties

  1.環境変数OKUYAMA_HOMEを配置ディレクトリとして設定する
    例)
      OKUYAMA_HOME=/home/okuyama/okuyama
      export OKUYAMA_HOME

  2.bin/okuyamaにパスを通す
    例)
      PATH=$PATH:$OKUYAMA_HOME/bin
      export PATH

  3.DataNode.propertiesのファイルパス指定を変更する
    以下の項目を環境に合わせて変更
    ・KeyManagerJob1.Option
    ・KeyManagerJob1.virtualStoreDirs
    ・KeyManagerJob1.keyStoreDirs

  4.log4j.properties
    ・ログ出力先全て


■手順3)
  以下のコマンドで起動、停止

  1.起動

    第1引数.動作指定: start 
    第2引数.バックグラウンド指定(-server)
    第3引数.起動用設定ファイル
    第4引数.標準出力ファイル
    第5引数.標準エラー出力ファイル

    例)
    okuyama start -server /home/okuyama/okuyama/conf/MasterNode.properties /home/okuyama/okuyama/master.out /home/okuyama/okuyama/master.err


  2.停止

    第1引数.動作指定: stop 
    第2引数.停止用ポート番号(MasterNode.propertiesおよび、DataNode.propertiesの"ServerControllerHelper.Init="で指定している":"より右辺の番号)

    例)
    okuyama stop 18888
