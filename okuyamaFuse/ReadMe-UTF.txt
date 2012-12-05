okuyamaFuseはokuyamaはストレージとして利用するファイルシステムです。
Linux用のファイルシステムとして実装されており、CentOS5.8上で開発しました。
ベータ版なので、テスト用とでご使用ください。
現状ではマウントディレクトリは11TBに見えます。ディレクトリ全体の容量は増減しません。


[仕組み]
 Fuseベースでファイルシステムを実装しています。
 そのため、Fuseに対応するOSでのみ稼働可能です。
 動作確認はCentOS5.8、6.0でのみ実施しています。


[依存]
 LinuxカーネルモジュールであるFuseに依存します。
 また、FuseのJavaバインディングであるFUSE-Jを利用しています。
 
   FUSE-J
    http://sourceforge.net/projects/fuse-j/
   Version
    2.4

 okuyamaを利用しているため、okuyamaの実行環境が必要です。
   Version-0.9.4以上
   ※DataNodeはどのストレージモードでも動きます。
     「DataSaveMapType=serialize」として圧縮メモリを利用する場合は、
     「SerializerClassName=」に「okuyama.imdst.util.serializemap.ByteDataMemoryStoreSerializer」を
      設定することを推奨します。圧縮率、性能共に「ObjectStreamSerializer」よりも優れています。


[利用方法](JavaやAntは全てセットアップ済みとします。okuyamaは192.168.1.1と192.168.1.2サーバで8888番のポートで起動しているものとします)
 1.FUSEをセットアップします。
   $yum install fuse* 
   (devel等も全てインストールしてください)
   ※「$modprobe fuse」を行いエラーが出ないことを確認

 2.FUSE-Jをセットアップ
   ■ダウンロード及び配置
   $wget http://jaist.dl.sourceforge.net/project/fuse-j/fuse-j/FUSE-J%202.4%20prerelease1/fuse-j-2.4-prerelease1.tar.gz
   $tar -zxvf fuse-j-2.4-prerelease1.tar.gz


   ■JNIをセットアップ
    ※コンパイル前にbuild.confの「JDK_HOME=/opt/jdk1.5.0」が正しいかチェック
   $cd fuse-j-2.4-prerelease1
   $mkdir build
   $make
    ※jniディレクトリの配下にlibjavafs.soが作成されていれば成功です。

   ■FUSE-Jをセットアップ
   $ant compile
   $ant dist
    ※distディレクトリの配下にfuse-j.jarが作成されていれば成功です。

   ■実行環境を用意
    libjavafs.so、fuse-j.jar、okuyama-{version}.jar、okuyamaFuse-{version}.jar、okuyama/lib/javamail-1.4.1.jar、okuyama/etc_client/okuyamaFuse/lib/fuse-j-2.4/lib/commons-logging-1.0.4.jarを
    上記のファイルを全て適当な1ディレクトリに配置

   ■マウントディレクトリを作成
   $mkdir /var/tmp/okuyamafuse

   ■マウント
     ※以下でMount実行(最終行はMasterNodeのIPとポート番号を":"で連結し、","で繋いで列挙する(一台の場合1つだけ記述)
      ※/usr/local/lib配下にFUSEのライブラリが配置されている想定
   LD_LIBRARY_PATH=./:/usr/local/lib java -classpath \
    ./okuyamaFuse-0.0.1.jar:./fuse-j.jar:./commons-logging-1.0.4.jar:./okuyama-0.9.4.jar:./javamail-1.4.1.jar \
    -Dorg.apache.commons.logging.Log=fuse.logging.FuseLog \
    -Dfuse.logging.level=ERROR -Xmx1024m -Xms1024m -server \
    -XX:+UseConcMarkSweepGC -XX:+CMSParallelRemarkEnabled -XX:+UseParNewGC \
    fuse.okuyamafs.OkuyamaFuse \
    -f -o allow_other \
    /var/tmp/okuyamafuse \
    192.168.1.1:8888,192.168.1.2:8888

   ■アンマウント
   $fusermount -u /var/tmp/okufs
   $kill -9 実行プロセス



