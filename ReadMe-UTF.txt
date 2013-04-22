====== 分散Key-Valueストア 「okuyama」=====================================================
Javaで実装された、永続化型分散Key-Valueストア「okuyama」を
ダウンロード頂きありがとうございます。

※起動方法はhttp://thinkit.co.jp/story/2011/02/17/2010をご覧頂くか、
  本テキストの「■機能説明とサンプルの実行方法」をご覧ください。
  blog:http://d.hatena.ne.jp/okuyamaoo/

  ・okuyamaに関する執筆記事
    [Think IT] "分散KVS「okuyama」の全貌"
    第1回 NOSQLは「知る時代」から「使う時代」へ http://thinkit.co.jp/story/2011/02/03/1990
    第2回 NOSQLの新顔、分散KVS「okuyama」の機能 http://thinkit.co.jp/story/2011/02/10/2002
    第3回 分散KVS「okuyama」のインストール http://thinkit.co.jp/story/2011/02/17/2010
    第4回 分散KVS「okuyama」の活用ノウハウ http://thinkit.co.jp/story/2011/02/24/2026

    [Think IT] "分散KVS「okuyama」実践TIPS"
    第1回 okuyamaを導入するまでに知っておきたいサーバリソースとの4つの関係 http://thinkit.co.jp/story/2011/10/12/2303
    第2回 okuyamaを運用するために知っておきたい基本的な操作 http://thinkit.co.jp/story/2011/10/26/2316
    第3回 okuyamaでのアプリ開発で押さえておきたい機能 http://thinkit.co.jp/story/2011/11/10/2325


・改修履歴
========================================================================================================
[New - 新機能追加、不具合対応]
[[リリース Ver 0.9.5 - (2013/04/XX)]]
■リカバリ機能を強化
 従来リカバリ中にMainMasterNodeが停止するとDataNodeのリカバリ処理が中途半端に終了してしまう問題が報告されました。
 その問題への対処として、リカバリ途中のDataNodeを判断し再度切り離す機能を追加。また、リカバリが必要なDataNodeを起動する際に
 起動引数として"-rr true"として起動することで、起動前が障害で停止したことに関係なくリカバリが行わるようになった。
 例えばMasterNodeを起動する前にDataNodeを起動してしまうと、そのDataNodeはデータリカバリ対象にならなかった。
 それがこのオプションを指定することで必ずリカバリされる。
 DataNode起動引数に以下を追加
 
    引数名 -rr
      記述：true/false
      意味：true=かならずリカバリ対象となる/ false=リカバリを自動判断する
      省略時:false
      設定例： -rr true

■各DataNodeが保存しているKeyの一覧を取得する機能を追加
 DataNodeが持つ全てのKeyとTagの一覧を取得する機能をUtilClientに追加しました。
 各DataNodeに接続し、当該機能を実行することでKeyの一覧が開業区切りで標準出力に出力される。
 利用方法は以下
     使い方)
     $ java -classpath ./:./lib/javamail-1.4.1.jar:./okuyama-0.9.5.jar okuyama.imdst.client.UtilClient keylist datanodeip:5553
     引数説明
      1)keylist : 命令
      2)datanodeip:5553 : Keyを確認したい、DataNodeのIPアドレスとポート番号を"IP:Port番号"のフォーマットで指定
 

 
========================================================================================================
[New - 新機能追加、不具合対応]
[[リリース Ver 0.9.4 - (2012/12/05)]]
■okuyamaFuseを追加
  ※ベータ版のため、予期せぬエラーなどが起こる可能性があります!!
    テスト時は仮想環境等での利用を推奨します。

  okuyamaFuseディレクトリに追加
  okuyamaをストレージとして動くファイルシステムです。
  ファイル、ディレクトリの新規作成、追記、参照、部分追記、部分参照、削除といった一般的な操作に対応しています、
  また、所有ユーザ、権限にも対応しています。
  ※シンボリックリンク、a-timeのみの変更は対応していません。

  okuyamaをストレージとして動くためokuyama側の特性を引き継ぎます。
  そのため、okuyama側をメモリモードで高速なストレージや、
  ファイルモードで大容量のファイルサーバが作成可能です

  ファイルのデータを1ファイル1Valueとして管理するのではなく、1ファイルをブロックに分解し、
  okuyama上で管理します。そのため、ランダムリード、ライトなどファイルの一部を操作する処理に
  性能を発揮します。また、複数の処理からの同時参照にも高い処理性能を発揮します。
  また、1ファイルの上限も実質存在しないため、テラバイト以上の巨大なファイルも作成可能です。
  その際もファイルの一部分へのアクセス速度は落ちません。

  その反面、ファイルのシーケンシャルなアクセスではそこまで性能はでません。
  また、現状ではrmによるファイル削除に時間がかかります。これは1ファイルを構成するブロックである
  okuyamaのKey-Valueを全てremoveしていることに起因します。

  作者はMySQLのデータファイル置き場やVirtualBoxの仮想イメージ置き場としてテストしています。
  MySQLのデータ置き場として利用した場合、検索や更新などの処理が混在した場合性能の向上が見られました。

  ストレージの性能や容量はokuyamaに依存するため、okuyama側をスケールアウトすることで
  okuyamaFuseもスケールアウトします。
  その際もokuyama側が無停止であるため、okuyamaFuseも停止する必要はありません。

  ファイルシステムを構成する全てのデータ(メタデータ、ファイルのブロックデータ)は全てokuyamaで
  管理されるため、あるサーバにマウントしファイルを作成すれば同じokuyamaを利用してmountしている
  okuyamaFuseは全てのデータは同期されています。
  そのため、mountしているサーバがダウンした際など別サーバでmountを行えばサーバダウン前までの
  データにアクセスできます。
  その反面、同一のファイルを複数サーバから更新するとファイルのデータを壊してしまいます。

  詳しい利用方法等はokuyamaFuse/ReadMe-UTF.txtをご覧ください。


■sendMultiByteValueをJava版のOkuyamaClientに追加
  一度に複数のKeyとbyte型のValueを登録する機能
  通信回数の節約になる。ただし一度にokuyamaに大量のデータを送ることになるので、
  okuyama自体のメモリを枯渇させないように注意する必要がある。
  引数はMap型となり値はKeyとValueのセットとなる。
  戻り値はMap型となり値は引数で渡されたKeyとそのKeyの登録結果をBoolean型で格納したMap

    [OkuyamaClientでのメソッド]
    public Map sendMultiByteValue(Map<String, byte[]> requestData) throws OkuyamaClientException;


■getTagObjectValues追加
  Tagを指定してValueを取得する際にそのValueをObject型に復元して返す。
  getTagValuesのValue取得部分をgetObjectValueとしたバージョンである
  そのため、紐付くValueが全てsetObjectValueで登録した値でないといけない
  指定したTag内にsetObjectValueで登録した値以外が含まれるとエラーとなる

    [OkuyamaClientでのメソッド]
    public Map getTagObjectValues(String tagStr) throws OkuyamaClientException;


■MasterNoed.propertiesのMyNodeInfoを設定していなくても、自身のHost名を利用して設定を補填するように機能追加
  正しく各サーバのHost名を設定していないMasterNodeの冗長構成が正しく機能しない可能性があるため、注意が必要である。
  本機能はMasterNoed.propertiesのMyNodeInfoの設定を省略することで機能する。

■Valueがディスクモード時にスナップショットオブジェクトからデータを復元した場合は-s起動オプションで指定した
  バイト数もしくは12888バイトを超えるデータを取得出来ないバグを修正。このバグが発生した場合は
  スナップショットオブジェクトを消し、操作記録ログもしくはバックアップで作成したファイルから復元すると
  完全にデータが復元できる。


■バックアップ用のスナップショットObjectを出力するかの有無
  keymapfile配下に出力されるスナップショット機能によるバックアップファイル(1.key.obj等)の出力有無を
  選択できるように起動引数を追加。okuyama起動時にデータ復元が全く必要ない場合は、この設定を利用することで
  スナップショットファイルが作成されなくなる。

    引数名 -efsmo
      記述：true/false
      意味：true=スナップショットファイルが作成される/ false=作成されない
      省略時:true
      設定例： -efsmo false


■DataNodeを動かすサーバがリカバリ時、DataNode追加時のデータ再配置時に高負荷になる
  場合に設定する起動パラメータを追加。この設定はデータを出力する側のサーバに設定する。
  リカバリ時の場合は正常に稼働している側のDataNode。追加時は元から稼働してる側のDataNode
  [注意!!]本設定を行うと設定前に比べてデータノードのリカバリ、データの再配置処理に余分に時間がかかる

    負荷を掛けたくないことを指定
     引数名 -lsdn
       記述：true/false
       意味：true=負荷を掛けたくないことを指示/ false=負荷を指定をしない
       省略時:false
       設定例： -lsdn true

    負荷を掛けたくないことを指定
     引数名 -lsdnsdc
       記述：整数
       意味：一度にリカバリ対象or再配置対象に転送するデータ数
             本値は-lsdnを指定しない場合50000～100000が利用される
       省略時:2000
       設定例： -lsdnsdc 4000


■検索Index作成時にダブルバイト文字の1文字の場合作成されない問題を修正

■バグフィックスと性能向上を実施

========================================================================================================
[New - 新機能追加、不具合対応]
[[リリース Ver 0.9.3 - (2012/03/10)]]
バグ報告をいただいた為、急遽リリースを行います。
■バグ内容
  1.完全ファイルモード時に、DataNode.propertiesの'KeyManagerJob1.keySize='に大きな値を設定し(1000万など)
    少ない登録数の場合にバックアップコマンドや、DataNodeのリカバリが失敗する事象

  2.DataNode追加時のデータ移行中のごく短い一定期間の間、getMultiValueで一部のデータを取得出来ない事象

  3.PHP版のOkuyamaClientにvar_dumpの記述があった為、削除


■追加機能

  ■incrValue及び、decrValueに値の初期化機能を追加
    incrValue及び、decrValueは存在しないKey-Valueに対して処理を行うと失敗するが、新たに初期化指定を追加
    初期化指定引数にtrueを渡して実行すると値が存在しない場合、
   「Key=指定されたKey値:Value=0」
    というセットで登録をおこなった後に指定された加算、減算を行うようになる。

    追加されたメソッドは以下(PHP版のOkuyamaClient.class.phpも同様)
    /**
     * MasterNodeへデータの加算を要求する.<br>
     *
     * @param keyStr Key値
     * @param value 加算値
	 * @param initCalcValue あたいの初期化指定 true=計算対象のKey-Valueがokuyama上に存在しない場合、0の値を作成してから、計算を行う false=存在しない場合は計算失敗
     * @return Object[] 要素1(処理成否):Boolean true/false,要素2(演算後の結果):Long 数値
     * @throws OkuyamaClientException
     */
     public Object[] incrValue(String keyStr, long value, boolean initCalcValue) throws OkuyamaClientException;

    /**
     * MasterNodeへデータの減算を要求する.<br>
     *
     * @param keyStr Key値
     * @param value 減算値
	 * @param initCalcValue あたいの初期化指定 true=計算対象のKey-Valueがokuyama上に存在しない場合、0の値を作成してから、計算を行う false=存在しない場合は計算失敗
     * @return Object[] 要素1(処理成否):Boolean true/false,要素2(演算後の結果):Long 数値
     * @throws OkuyamaClientException
     */
     public Object[] decrValue(String keyStr, long value, boolean initCalcValue) throws OkuyamaClientException;


    (例) Java版のOkuyamaClientの場合は以下の構文になる。
    ----------------------------------------------------------
     // 存在しない値に加算を実行
     Object[] incrRet = okuyamaClient.incrValue("NO_DATA_KEY", 10, true); // 第三引数にtrueを指定

     if (incrRet[0].equals("true")) {
         System.out.println("ResultValue=" + (Long)incrRet[1]);
	 }
    ----------------------------------------------------------

  ■MasterNodeを追加する機能をUtilClientに追加
    従来MasterNodeを追加する場合はWebベースのマネージャを起動し、そこから行うしかなかったが、
    UtilClientに追加する機能を追加
     使い方)
     $ java -classpath ./:./lib/javamail-1.4.1.jar:./okuyama-0.9.3.jar okuyama.imdst.client.UtilClient addmasternode masternode:8888 masternode2:8889
     引数説明
      1)addmasternode : 追加命令
      2)masternode:8888 : 追加を依頼するMasterNodeのアドレスとPort番号(フォーマット "アドレス:ポート番号")
      3)masternode2:8889 : 追加するMasterNodeのアドレスとPort番号(フォーマット "アドレス:ポート番号")



========================================================================================================
[New - 新機能追加、不具合対応]
[[リリース Ver 0.9.2 - (2012/03/03)]]
■メモリオブジェクトの自動スナップショット機能を追加
  メモリオブジェクトのスナップショット機能により、今までDataNode復帰時に操作記録ログから復旧していたのに対して、
  高速に停止前の状態に復元されるようになった。

  メモリオブジェクトのスナップショットは通常25分に一度作成される。作成されたログはDataNodeの標準出力に
  開始時間と終了時間が表示される。作成される場所及びファイルの名前は、DataNodeの設定ファイルである
  DataNode.propertiesの以下の設定値の第一設定値の名前に拡張子に「.obj」が付加されて作成される。
  "KeyManagerJob1.Option="
  例)
     "KeyManagerJob1.Option=./keymapfile/1.key,./keymapfile/1.work.key"
     ※上記の設定の場合はkeymapfileディレクトリ内に「1.key.obj」として作成される。

  また、一度作成され、その後25分が経過した場合は同じファイルに上書きとして作成される。
  そのため、履歴管理などは行われない。
  DataNodeのストレージモードとこのメモリオブジェクトにストアされるデータの関係は以下となる
   1.Key=メモリ、Value=メモリ
     ->メモリオブジェクトにKeyとValueの両方がストアされる。
   2.Key=メモリ、Value=ディスク
     ->メモリオブジェクトにKeyとValueのディスク上の位置がストアされる。
   3.Key=ディスク、Value=ディスク
     ->メモリオブジェクトは作成されない

  「1.」、「2.」それぞれのDataNode再起動時のデータ復元の挙動は以下となる。
    1.メモリオブジェクトが存在した場合はメモリオブジェクトを読み込む。その後、操作記録ログが存在した場合は、
      操作記録ログからメモリオブジェクト作成開始直前からの操作がトレースされ、DataNode停止直前の完全な状態が
      復元される。
    2.メモリオブジェクトが存在した場合はメモリオブジェクトを読み込む。その後、Valueを保存しているディスク上の
      ファイルの損傷チェックを行い、損傷が発生している場合はそのデータを削除する。そして、操作記録ログが存在
      した場合は、操作記録ログからメモリオブジェクト作成開始直前からの操作がトレースされ、DataNode停止直前の
      状態に復元される。
      メモリオブジェクトは存在するが、Valueを保存しているディスク上のファイルが存在しない、サイズが0である
      場合などは、操作記録ログに残されているデータのみが復元される。



■完全ファイルモード時にDataNodeを停止後、再起動した際に操作記録ログがなくても
  停止直前の状態に復元されるように起動時の挙動を変更。

  従来のバージョンでは完全ファイルモード(Key=ディスク、Value=ディスク)で合っても
  再起動時は操作記録ログから全てのデータを復元するしかデータを前回停止前の状態にす
  方法はなかった。そのため、大量のデータを保存している場合は起動時に大量の操作記録ログを
  読み出す必要があり非常に時間がかかってしまう場合があった。

  そこで本機能はDataNode停止時のKey用、Value用のディスク上のファイル群がディスク上に
  残っていれば、再起動時にそのファイル群を再度利用するように変更し、起動時間の短縮をおこなった。
  なお、Key用、Value用のディスク上のファイルが損傷している場合は可能な範囲で修復され、
  そもそもディスク上にデータがが残されていない場合は従来取り、操作記録ログから復元するプロセスとなる。

  ※本機能により、完全ファイルモード時はローテーション済みの操作記録ログ(1.work.key1など最後に数値のついたファイル)を
    残す必要はなくなるが、定期的にバックアップをUtilClientによって取得することで保全性を高めることが出来る。
  利用方法は特に意識する必要なく、本機能は有効となる。



■ディスクキャッシュ機能を追加
  本機能はValueをディスクに保存している場合のみ有効となる。

  okuyamaはValueの場所をディスクにした場合は、一定までのサイズのValueを全て1つの
  ファイルで集中的に管理している。

  ここに保存している1Valueのデータはブロックデータと呼ばれる。
  getなどの取得処理の場合は、このファイル上をシークしブロックデータを取り出している。
  SSDのようなシーク処理を必要としないディスクの場合は高速に稼働するが、HDDなどの場合は
  アクセスが集中しかつ、ValueのファイルがOSのバッファサイズを超えるようになると、
  アクセス速度が低速化する。

  そこで本機能では、一度読みだしたブロックデータをHDDよりもランダムリードが高速な
  デバイスに一時的にキャッシュすることでget処理を高速化するものである。

  一時的にキャッシュされるValueはレコード数で指定可能である。Defaultは1DataNode当たり、
  10000件となる。キャッシュに利用するディスクサイズの算出は、ブロックデータのサイズを
  利用して計算することが可能であり。そのブロックサイズはDefaultでは12888byteとなる。
  そのため、利用されるディスクサイズは、以下の計算式で求める
  --------------------------------------------------------------------
   [12888byte × 10000件 = 128880000byte]
  --------------------------------------------------------------------
  このブロックサイズはDataNodeの起動引数である「-s]を利用して「-s 8192」のように変更が可能である。

  キャッシュの有効化及び、キャシュファイルの作成場所指定は、DataNode.propertiesの「KeyManagerJob*.cacheFilePath=」
  要素を指定し、ファイルパスを記述することで有効化される。利用しない場合はこの要素そのものを消すか、ファイル指定を
  消すことで無効化かされる。
  --------------------------------------------------------------------
  [DataNode.propertiesでの設定例]
    例) "KeyManagerJob1.cacheFilePath=/usbdisk/cachedata/cache1.data"
  --------------------------------------------------------------------
  キャッシュされるValueの件数はDataNodeの起動引数である「-mdcs」を利用して「-mdcs 50000」のように
  キャッシュしたい件数を指定して変更可能である。デフォルトは前述の通り、10000件

  利用中にUSBフラッシュメモリが壊れた場合は、キャッシュの利用が停止されるだけとなり、DataNodeの停止は
  発生しない。壊れた場合は、USBをサーバから引き抜き、新しいUSBディスクを故障前と同じ名前でマウントすれば、
  自動的に再認識され、利用される。その際DataNodeの停止は伴わない。



■Java&PHPのOkuyamaClientにObjectの新規登録を保証するsetNewObjectValueメソッドを追加
  setNewValueはValueにString型しか対応していなかったが、ValueをObjectとするバージョンを追加。
  利用方法はsetNewValueと同様
  利用例)※Java版
  ------------------------------------------------------
    OkuyamaClient client = new OkuyamaClient();
    client.connect("127.0.0.1", 8888);

    Map objectValue = new HashMap();
    objectValue.put("Key_XXX", "Value_XXX");
    objectValue.put("Key_YYY", "Value_YYY");
    objectValue.put("Key_ZZZ", "Value_ZZZ");

    String[] setResult = client.setNewObjectValue("Object_Key", objectValue); // 新規登録

    if (setResult[0].equals("true")) { // 登録を確認
		System.out.println("登録成功");
    } else {
		System.out.println("登録失敗 Message=[" + setResult[1] + "]");
    }
  ------------------------------------------------------



■UtilClientを利用したバックアップ機能でのokuyamaの負荷軽減機能を追加
  UtilClientを利用したバックアップを行った場合okuyama側はリソースの限界まで利用して
  バックアップ作成をおこなっていたが、バックアップの指定引き数をあたえることで、低速で徐々にバックアップを
  作成する機能を追加。これによりバックアップ作成時にokuyama側がbusyにならないようにすることが可能
  従来から2つ引き数を追加
   第4引き数:クライアントがなんだかの理由でアボートした場合にokuyama側がそれを検知する時間(ミリ秒)
   第5引き数:okuyama側データを出力する際に一定量排出した後にここで指定したミリ秒だけ停止する。大きくすると負荷は下がるが時間がかかる
 
  利用方法)
  java -classpath ./:./okuyama-0.9.2.jar:./lib/javamail-1.4.1.jar okuyama.imdst.client.UtilClient bkup 127.0.0.1 5553 10 20 > bkupFor5553.dump



■起動パラメータを以下の通り追加
  DataNode用の起動パラメータ
   -crcm 記述：true/false
         説明：MasterNodeとDataNode間の処理に失敗した場合に強制的に1度だけ再処理を行うようにするかの設定 
	           Networkが不安定な場合や遅い場合はtrueにするとよいことがある
               true/再接続する, false/再接続は自動
               デフィルトはfalse
         設定例： -crcm true

   -dcmuc 記述：整数(利用回数)
         説明：MasterNodeとDataNode間のSockeの最大再利用回数 (整数) 少ない値にすると接続コストがかかる
               デフィルトは50000
         設定例： -dcmuc 10000

   -smbsmf 記述：整数(格納数)
        説明：SerializeMapのBucketサイズのJVMへのメモリ割当1MB単位への格納係数(整数)
              デフィルトは400(レスポンスを向上したい場合は小さな値にすると良いが、メモリ当たりの格納数は減る)
        設定例： -smbsmf 200

   -red 記述：true/false
         説明：完全ファイルモード時に既に存在するデータを再利用する設定
               デフィルトはtrue。
               falseにすることで停止前のデータを全て削除してトランザクションログからの起動を選択する
        設定例： -red false

   -wfsrf 記述：true/false
         説明：DataNode起動時に操作記録ログ(トランザクションログ)を読み込む設定
               trueの場合は読み込む(デフォルト)
               falseの場合は読みこまない.この場合はデータは常に0件からの起動になる
        設定例： -wfsrf false

   -udt 記述：1/2
         説明：データファイルを保存するディスクのタイプを指定することで、ディスクへのアクセスが最適化される 
               1=HDD(デフォルト) 2=SSD
         設定例： -udt 2

   -mdcs 記述：整数(格納数)
         説明：ディスクキャッシュ利用時に、どれだけの件数をキャッシュに乗せるかを件数で指定する 
               「■ディスクキャッシュ機能を追加」も参照
               デフォルトでは10000件
         設定例： -mdcs 50000



■PHP版のOkuyamaClient.class.phpで未実装だった以下のメソッドを実装。挙動はJava版と同様である
  1.getTagValues
  2.getMultiTagValues
  3.getMultiTagKeys

  ※以下のメソッドは未実装
    getTagKeysResult
    getMultiTagKeysResult


■Linux用のサービス化スクリプトを試験的に作成。install/配下に
  MasterNode用[okuyamaMasterNodeServiceScript]
  DataNode用[okuyamaDataNodeServiceScript]
  として配置


■有効期限を30日以上に設定出来ないバグを修正
■ノード追加に伴うデータ移行中にTagデータを新規登録すると、正しく取得出来ないバグを修正
■DataNodeのリカバリー処理、ノード追加時のデータ再配置処理の堅牢性を向上
■getMultiValuesメソッドで特定の条件下で応答が返ってこないバグを修正
■いくつかの性能向上

========================================================================================================
[New - 新機能追加、不具合対応]
[[リリース Ver 0.9.1 - (2011/12/19)]]

・PHP版のOkuyamaClientの不具合対応
  ・Key、Value、Tagの登録前サイズチェック周りの修正
    登録可能なKeyおよび、Tagのバイト長を320byteに固定
    PhpTestSock.phpにテストコードを追加
    テスト名はsize-trueとsize-falseとなる
  ・OkuyamaClient.class.phpの前回バージョンの71行目の構文が不要なため削除
  ・OkuyamaClient.class.phpの前回バージョンの113行目unset済み変数への参照の構文を修正
    isset関数に置き換え


・Java用のOkuyamaClientをプーリングするコネクションプールを追加
  本機能を利用すると接続処理、接続済みOkuyamaClientのプール、クローズ処理の管理を行うことが出来る
  接続済みのコネクションを即利用可能なため、接続処理のコスト削減、接続処理の共通化を行うことが可能
  該当クラスは以下
  ・okuyama.imdst.client.OkuyamaClientFactory
  (利用方法)
   -------------------------------------------------------------------------------------------------------
   // MasterNodeの接続情報を作成して、OkuyamaClientFactory.getFactoryに渡すことでコネクションプールを取得
   // ここで取得されるインスタンスはシングルトンとなり全てのスレッドで唯一となる
   // スレッド毎に新しいFactoryを利用したい場合はgetNewFactoryメソッドを利用する
   String[] masterNodes = {"192.168.1.1:8888","192.168.1.2:8888"};
   OkuyamaClientFactory factory = OkuyamaClientFactory.getFactory(masterNodes, 20);

   // プールからClientを取得
   OkuyamaClient okuyamaClient = factory.getClient();

   // 以降は通常のOkuyamaClientの利用方法と同様
   String[] getResult = okuyamaClient.getValue("Key-XXXX");
   if (getResult[0].equals("true")) {
       System.out.println(getResult[1]);
   }

   // close()を呼び出すことでコネクションプールに返却される
   okuyamaClient.close();

   // アプリケーションそのものを終了する際は以下でFactoryを終了させて全てのコネクションを破棄する
   // 終了後も再度getFactoryを呼び出せば新たにfactoryが再生成される
   factory.shutdown();
   -------------------------------------------------------------------------------------------------------


・UtilClientにadddatanodeを追加
  DataNodeを追加する際に従来はWebの管理画面からしか追加できなかったが、
  UtilClientから追加する機能を追加
  ※本機能によるokuyamaのサーバ側の変更は必要ありません。
  使い方)
  $ java -classpath ./:./lib/javamail-1.4.1.jar:./okuyama-0.9.1.jar okuyama.imdst.client.UtilClient adddatanode masternode:8888 datanode02:5555 slavedatanode:02:6555 thirddatanode:7555
  引数説明
  1)adddatanode : 追加命令
  2)masternode:8888 : 追加を依頼するMasterNodeのアドレスとPort番号
  3)datanode02:5555 : 追加を依頼するDataNodeのアドレスとPort番号(MasterNode.propertiesのKeyMapNodesInfoの設定に該当)
  4)slavedatanode:6555 : 追加を依頼するDataNodeのアドレスとPort番号(MasterNode.propertiesのSubKeyMapNodesInfoの設定に該当。設定を行っていない場合は省略)
  5)thirddatanode:7555 : 追加を依頼するDataNodeのアドレスとPort番号(MasterNode.propertiesのThirdKeyMapNodesInfoの設定に該当。設定を行っていない場合は省略)


・DataNodeの完全ディスクモードの性能を向上
  ファイルへの書き出しにメモリでのバッファリング領域を設けそちらへの書き出しが完了した時点でユーザ処理を
  完了とすることで応答速度向上。実際の書き出し処理は別スレッドで順次行われる。


・MasterNodeのIsolation機能(パーティション機能)利用時にgetTagKeysResult、getMultiTagKeysResultで発生する
  不具合に対応


・起動パラメータを以下の通り追加
  DataNode用の起動パラメータ
   -vidf 記述：true/false
         説明：有効期限切れのデータのクリーニングを行うかどうかの設定 true=行う false=行わない 
               有効期限付きのデータを登録しない場合はfalseにすることでクリーニングデータのチェックで
               発生する負荷を減らすことが出来る
               true=リーニングを行う、false=リーニングを行わない
               デフィルトはValueがメモリの場合はtrue、Valueがディスクの場合はfalse
               ※trueを指定するとファイルをストレージに使っている場合も実行される
         設定例： -vidf false

   -svic 記述：整数(分/単位)
         説明：有効期限切れのデータのクリーニングを行う時間間隔。短い間隔を指定すると、クリーニングデータのチェックが
               頻繁に発生するため負荷がかかる。大きすぎる値を指定すると有効期限切れのデータがいつまでもアクセス
               不可の状態で保持されるためストレージ領域を無駄にしてしまいます。               
               デフィルトは30分
         設定例： -svic 60

   -csf 記述：true/false
        説明：保存データの合計サイズを計算するかどうかの指定
              保存容量を計算する必要がない場合はfalseにすることで登録・削除時の負荷を軽減できる。
              保存容量とはMasterNodeのKeyNodeWatchHelper.logなどで出力されている、"Save Data Size"項目などに当たります。
              true=計算する、false=計算しない
              デフィルトはtrue
        設定例： -csf false

   -rdvp 記述：true/false
         説明：Key、Value共にディスクを利用した際にValueの更新時に更新前のデータファイル上のValueの場所を再利用して
               再保存するかの設定。
               再利用する場合は登録済みValueの再保存を行うと更新前のディスク上の場所を利用するため、更新によるディスク使用量の
               増加はありません。しかしディスクへのアクセスは既存のValueの位置へディスクをシークしてから保存を行うためディスクへの
               負荷が高くなります。
               再利用しない場合は既存のValueであっても常に最新のValueをディスクの最後尾に書き足すため、ディスクへのアクセスは
               シーケンシャル(一方向)なアクセスになりディスク負荷は少なくなります。しかし既存のValueへの更新であっても
               ディスク使用量が増加します。
               デフィルトはtrue
        設定例： -rdvp false

   -dwmqs 記述：整数(バッファリング件数/単位)
         説明：DataNodeの完全ディスクモード時にメモリのバッファリング空間の蓄積できる件数。最大値は20億。
         最大ここで設定した件数分のKey長+10byte分のメモリ容量を利用するため、大きな値にする場合は注意が
         必要となる。Defaultの値は7000。
        設定例： -dwmqs 15000


・DataNode.propertiesのDataSaveTransactionFileEveryCommit=false時の不具合を対応

========================================================================================================
[New - 新機能追加、不具合対応]
[[リリース Ver 0.9.0 - (2011/10/21)]]
■Hadoop対応
  !!! [注意] 本機能はベータ機能である !!!
  Hadoopの持つFormatインターフェースを利用してMapReduce内でokuyamaに登録されているデータを取り出して
  利用できるようにしました。
  詳しくはhadoopディレクトリ配下のReadMe.txtを参照してください。


■OkuyamaClientへのgetMultiTagKeysメソッドの追加
  複数のTagを指定して、該当するKeyのみ取得可能なメソッドを追加しました。
  Keyのみ取得できるので、クライアント側のメモリ利用量の削減が可能です。
  またgetMultiTagValuesに比べて高速に応答を返します。
  [OkuyamaClientでのメソッド]
  public String[] getMultiTagKeys(String[] tagList) throws OkuyamaClientException;
  public String[] getMultiTagKeys(String[] tagList, boolean margeType) throws OkuyamaClientException;
  public String[] getMultiTagKeys(String[] tagList, boolean margeType, boolean noExistsData) throws OkuyamaClientException;


■OkuyamaClientへのgetTagKeysResultメソッドの追加
  本メソッドの挙動はgetTagKeysと同様にTagを指定することで、紐付く全てのKeyを取得する。しかし、getTagKeysが
  一度にString型の配列で紐付く全てのKey値を返してくるのに対して、本メソッドはokuyama.imdst.client.OkuyamaResultSetを
  返却してくる。OkuyamaResultSetはjava.sql.ResultSetのように順次データを取り出せるようになっており、
  従来のgetTagKeysでは扱えないような大量のKey値を処理する場合に利用する。
  以下の実装例はTagに紐付く全てのKeyとValueを出力している例である。
  例) "Tag1"に紐付く全てのKeyとValueを出力
  ----------------------------------------------------------------------
	OkuyamaResultSet resultSet = client.getTagKeysResult("Tag1"); // OkuyamaResultSetインターフェースで結果を受け取る

	while(resultSet.next()) {  // カーソルを移動しながら値の有無を確認 trueを返して来た場合は終端ではない、falseは終端
		System.out.println("Key=" + (Object)resultSet.getKey());     // getKeyメソッドにて現在のカーソル位置のKey値を取得
		System.out.println("Value=" + (Object)resultSet.getValue()); // getValueメソッドにて現在のカーソル位置のValue値を取得
	}
	resultSet.close();
  ----------------------------------------------------------------------
  なお、getTagKeysResultは取得するKeyとValueに対してフィルタリングを設定することが可能である。
  フィルタリングは
   1.数値でのKeyとValueに対しての範囲指定
   2.正規表現でのKeyとValueに対しての一致指定
   3.ユーザ実行クラスによる独自フィルタリング(フィルタリングクラスは、okuyama.imdst.client.UserDataFilterインターフェースを実装する)
  が可能である。
  [OkuyamaClientでのメソッド]
  public OkuyamaResultSet getTagKeysResult(String tagStr) throws OkuyamaClientException;
  public OkuyamaResultSet getTagKeysResult(String tagStr, String encoding) throws OkuyamaClientException;
  public OkuyamaResultSet getTagKeysResult(String tagStr, String matchPattern, int cehckType) throws OkuyamaClientException;
  public OkuyamaResultSet getTagKeysResult(String tagStr, String matchPattern, int cehckType, String encoding) throws OkuyamaClientException;
  public OkuyamaResultSet getTagKeysResult(String tagStr, double[] targetRange, int cehckType) throws OkuyamaClientException;
  public OkuyamaResultSet getTagKeysResult(String tagStr, double[] targetRange, int cehckType, String encoding) throws OkuyamaClientException;
  public OkuyamaResultSet getTagKeysResult(String tagStr, UserDataFilter filter) throws OkuyamaClientException;
  public OkuyamaResultSet getTagKeysResult(String tagStr, UserDataFilter filter, String encoding) throws OkuyamaClientException;
  ※!!注意!! 1.PHPのクライアントは未対応
             2.本メソッドを利用するためには、okuyamaのMasterNode、DataNode共に、Version-0.9.0以上である必要がある
             3.OkuyamaResultSetの詳しい利用方法は、JavaDocのokuyama.imdst.client.OkuyamaResultSetの部分を参照してください


■OkuyamaClientへのgetMultiTagKeysResultメソッドの追加
  本メソッドの挙動はgetMultiTagKeysと同様に複数Tagを指定することで、紐付く全てのKeyを取得することが可能であるが、
  getTagKeysResultと同様に、okuyama.imdst.client.OkuyamaResultSetからデータを取り出すことが可能であるため、大量の
  データ取得に利用する。getTagKeysResultと違い、値のフィルタリングをすることは出来ない。
  [OkuyamaClientでのメソッド]
  public OkuyamaResultSet getMultiTagKeysResult(String[] tagList) throws OkuyamaClientException;
  public OkuyamaResultSet getMultiTagKeysResult(String[] tagList, boolean margeType) throws OkuyamaClientException;
  ※!!注意!! 1.PHPのクライアントは未対応
             2.本メソッドを利用するためには、okuyamaのMasterNode、DataNode共に、Version-0.9.0以上である必要がある
             3.OkuyamaResultSetの詳しい利用方法は、JavaDocのokuyama.imdst.client.OkuyamaResultSetの部分を参照してください


■PHP版のOkuyamaClient.class.phpで未実装だった以下のメソッドを実装。挙動はJava版と同様である
  1.getMultiValue
  2.removeSearchIndex
  3.getValueAndUpdateExpireTime
  4.setValueとsetNewValueへの有効期限(ExpireTime)対応
  5.setObjectValue
  6.getObjectValue
  7.getObjectValueAndUpdateExpireTime
  8.getOkuyamaVersion

  ※以下のメソッドは未実装
    getTagValues
    getMultiTagValues
    getMultiTagKeys
    getTagKeysResult
    getMultiTagKeysResult


■OkuyamaQueueClientの実装
  !!! [注意] 本機能はベータ機能である !!!
  okuyamaをキューとして利用できる専用のJavaクライアントを追加
  クライアント名：okuyama.imdst.client.OkuyamaQueueClient
  キューとして利用する場合もokuyamaのサーバ側は特に設定は必要なく、通常と同じように起動するだけで良い。
  OkuyamaQueueClientの利用手順は
   1.MasterNodeへ接続
   2.createQueueSpaceメソッドで任意の名前でQueue領域を作成(既に作成済みのQueue領域を利用する場合は作成不要)
   3.putメソッドにてデータを登録、もしくはtakeメソッドにて取り出し
   4.利用終了後closeを呼び出す
  となる。詳しくはJavaDocのokuyama.imdst.client.OkuyamaQueueClientの部分を参照してください。


■OkuyamaClientへのgetObjectValueAndUpdateExpireTimeメソッドの追加
  getValueAndUpdateExpireTimeのObject版になる。取得と同時に有効期限が設定されている場合は設定した有効期限
  秒数だけ有効期限が延長される。Webでのsessionオブジェクトなどの、アクセスする度に有効期限を延ばしたい
  値に利用すると便利である。
  [OkuyamaClientでのメソッド]
  public Object[] getObjectValueAndUpdateExpireTime(String keyStr) throws OkuyamaClientException;
  ※PHP版のクライアントも対応済み。


■いくつかの処理性能向上と不具合の修正


========================================================================================================
[New - 新機能追加、不具合対応]
[[リリース Ver 0.8.9 - (2011/08/31)]]

■OkuyamaClientにsetObjectValueとgetObjectValueメソッドを追加
  JavaでクラスでSerializableをimplementsしているクラスのオブジェクトをそのまま登録、
  取得出来るメソッドを追加
  メソッドは以下
   ・Setメソッド
     boolean setObjectValue(String keyStr, Object objValue)
     boolean setObjectValue(String keyStr, String[] tagStrs, Object objValue)
     boolean setObjectValue(String keyStr, Object objValue, Integer expireTime)
     boolean setObjectValue(String keyStr, String[] tagStrs, Object objValue, Integer expireTime) 
    返却値のbooleanはtrueの場合は登録成功、falseの場合は登録失敗

   ・Getメソッド
     Object[] getObjectValue(String keyStr)
    返却値のObject配列の要素は
    Object[0] = 要素1 データ有無(String型) : "true"=データ有 or "false"=データ無
    Object[1] = 要素2 取得データ(Object型) : データ有無が"false"の場合のみエラーメッセージ文字列(String型固定))それ以外は、登録したObject


■トランザクションログ(WALログ)のディスクへのfsyncの頻度を調整可能に
  トランザクションログのディスクへのfsyncは従来OS任せだったが、頻度を調整可能に変更
  ImdstDefine.transactionLogFsyncTypeの値を変更するか、DataNodeの起動引数に"tlft"付加し係数を
  指定することで変更可能。係数の説明は以下
   0=OS任せ
   1=fsync頻度小
   2=fsync頻度中
   3=fsync頻度高
   4=トランザクションログ書き込み毎にfsync(データへの変更毎に)

   DataNode起動例)
    java -cp ./classes;./lib/log4j-1.2.14.jar;./lib/javamail-1.4.1.jar;./lib/commons-codec-1.4.jar -Xmx128m -Xms128m okuyama.base.JavaMain /Main.properties /DataNode.properties -tlft 4

■memcachedでいうところのflush_allに対応
  UtilClientによりデータの全削除を行うことができたが、これをmemcachedクライアントのflush_allへ紐付け
  memcahcedクライアントが接続しているMasterNodeの担当するIsolationのデータを全削除できる。
  Isolationを持っていないMasterNodeの場合は失敗する


■Dataファイルへのアクセスを効率化。
  既存データのUpdateに対する効率化を実施


■いくつかの処理性能向上と不具合の修正
  ・内部コード見直しによる処理速度向上
  ・memcachedモードでMasterNodeを起動した場合にdeleteメソッドで特定のクライアントがkey以外にパラメータを
    転送してくるため、その際エラーになっていたため修正
  ・ノード動的追加時にMainMasterNode以外でMasterNodeで発生するデータが取得出来ない不具合に対応
  ・全文検索時に、Isolation利用したMasterNodeに対して、2文字などの短い文字列で検索Indexを作成した場合に
    検索対象にならない不具合に対応


========================================================================================================
[New - 新機能追加、不具合対応]
[[リリース Ver 0.8.8 - (2011/07/3)]]

■ストレージ機能にSerializeMapを追加
  データ格納時にメモリ空間を有効利用するSerializeMapという機能を追加。
  データを格納するMapに登録するKeyとValueを(デ)シリアライザ外部から自由に指定できるように機能追加
  詳しくは以下のBlogを参照
  http://d.hatena.ne.jp/okuyamaoo/20110616
  http://d.hatena.ne.jp/okuyamaoo/20110623

  設定はDataNode.propertiesに以下の項目が追加された
  "DataSaveMapType"
  "SerializerClassName"

  "DataSaveMapType"はSerializeMapの利用を指定
  "SerializerClassName"は(デ)シリアライザのクラスを指定
  SerializerClassNameで指定するクラスはokuyama.imdst.util.serializemap.ISerializerをインプリメンツする
  設定なしは、ConcurrentHashMapを利用する。DefaultはConcurrentHashMap

  設定方法)
  DataSaveMapType=serialize
  SerializerClassName=okuyama.imdst.util.serializemap.ObjectStreamSerializer
  ObjectStreamSerializerは現在実装済みの(デ)シリアライザクラス。リリース物に同梱。
  ※上記でSerializeMapを内部で利用

  DataSaveMapType=
  SerializerClassName=
  ※上記で通常のConcurrentHashMapを内部で利用



■okuyamaクライアントからも有効期限を設定可能に
  OkuyamaClientのsetValue及び、setNewValueにexpireTimeを渡すことで有効期限(単位は秒)を設定可能
  上限時間は、Integerの限界値
  例)
  okuyamaClient.setValue("Key_XXX", "Value_YYY", new Integer(300));
  上記の場合有効期限は300秒


■データ取得と同時にそのデータに設定されている有効期限を登録時に設定した期限分延長するメソッドを
  okuyamaクライアントに追加。
  メソッド名はgetValueAndUpdateExpireTime
  ※有効期限が設定させていないデータは何も起こらない
  例)
  okuyamaClient.setValue("Key_XXX", "Value_YYY", new Integer(300));
  String[] getResult = okuyamaClient.getValueAndUpdateExpireTime("Key_XXX");
  ※上記のsetValue時に設定された300秒という有効期限が、getValueAndUpdateExpireTime呼び出し時に再度300秒で
    自動的に延長される。


■複数Tagを指定して紐付くKeyとValueを取得する機能を追加
  okuyamaクライアントでは、getMultiTagValues

  取得方法を複数のTagが全てに紐付いているANDと、どれかにだけ紐付くORを指定できる。
  返却される形式はMapとなり、KeyとValueのセットになる。
  そのため、複数のTagに紐付いているKeyは束ねられる。
  例)
   String[] getTags = {"Tag1","Tag2","Tag3"};
   Map retAndMap = okuyamaClient.getMultiTagValues(getTags, true) //<=AND指定
   Map retOrMap = okuyamaClient.getMultiTagValues(getTags, false) //<=OR指定


■データ一括削除機能を追加
  Isolation単位もしくは全てのデータを一括で削除する機能をokuyama.imdst.client.UtilClientに追加
  利用方法は以下
  利用方法)
  java -classpath ./:./classes okuyama.imdst.client.UtilClient truncatedata 192.168.1.1 8888 all
  第1引数 = 'truncatedata' <=固定
  第2引数 = '192.168.1.1'  <=MainMasterNodeのIPアドレス
  第3引数 = '8888'         <=MainMasterNodeの起動ポート番号
  第4引数 = 'all'          <=全ての削除を指定する'all'もしくは削除するIsolationPrefix名


■現在のMasterNodeを指定してそのMasterNodeが現在どのような設定情報で稼働しているかを取得する機能をUtilClientに追加
   利用方法)
   java -classpath ./:./classes okuyama.imdst.client.UtilClient masterconfig 192.168.1.1 8888
   第1引数 = 'masterconfig' <=固定
   第2引数 = '192.168.1.1'  <=MainMasterNodeのIPアドレス
   第3引数 = '8888'         <=MainMasterNodeの起動ポート番号
   ※出力例)
   998,true,MainMasterNode=[true]- MyInfo=[127.0.0.1:8888]- MainMasterNodeInfo=[127.0.0.1:8888]- AllMasterNodeInfo=[127.0.0.1:8888 127.0.0.1:8889 127.0.0.1:11211]- 
   CheckMasterNodeTargetInfo=[]- Algorithm [0]:mod [1]:consistenthash=[1]- AllDataNodeInfo=[{third=[localhost:7553  localhost:7554]  sub=[localhost:6553  localhost:6554]  
   main=[localhost:5553  localhost:5554]}]


■いくつかの処理性能向上と不具合の修正
   1.Key-Valueの両方もしくはどちらかをメモリに展開する場合にデータを登録し続けるとメモリを使いすぎる現象を改善。
   2.MasterNodeの不正な呼び出し番号(プロトコルの先頭)を渡した場合に応答がなくなってしまう問題を解決
   
========================================================================================================
[New - 新機能追加、不具合対応]
[[リリース Ver 0.8.8 - (2011/07/3)]]

■okuyamaクライアントからも有効期限を設定可能に
  OkuyamaClientのsetValue及び、setNewValueにexpireTimeを渡すことで有効期限(単位は秒)を設定可能
  上限時間は、Integerの限界値
  例)
  okuyamaClient.setValue("Key_XXX", "Value_YYY", new Integer(300));
  上記の場合有効期限は300秒


■データ取得と同時にそのデータに設定されている有効期限を登録時に設定した期限分延長するメソッドを
  okuyamaクライアントに追加。メソッド名はgetValueAndUpdateExpireTime
  ※有効期限が設定させていないデータは何も起こらない
  例)
  okuyamaClient.setValue("Key_XXX", "Value_YYY", new Integer(300));
  String[] getResult = okuyamaClient.getValueAndUpdateExpireTime("Key_XXX");
  ※上記のsetValue時に設定された300秒という有効期限が、getValueAndUpdateExpireTime呼び出し時に再度300秒で
    自動的に延長される。

■ストレージ機能にSerializeMapを追加
  データ格納時にメモリ空間を有効利用するSerializeMapという機能を追加。
  アクセスレスポンスは低下するがメモリ上に格納できるデータ量は向上する。
  詳しくは以下のBlogを参照
  http://d.hatena.ne.jp/okuyamaoo/20110616
  http://d.hatena.ne.jp/okuyamaoo/20110623

  設定はDataNode.propertiesに以下の項目が追加された
  "DataSaveMapType"

  設定なしは、ConcurrentHashMapを利用する。DefaultはConcurrentHashMap

  設定方法)
  DataSaveMapType=serialize
  ※上記でSerializeMapを内部で利用

  DataSaveMapType=
  ※上記で通常のConcurrentHashMapを内部で利用

■いくつかの処理性能向上と不具合の修正

========================================================================================================
[New - リリースファイル不備]
[[リリース Ver 0.8.7.2 - (2011/05/12)]]
■リリース物に空のkeymapfileディレクトリを同梱し忘れたため、追加
  antで実行した場合などにエラーになるため。

■installディレクトリ内のbinディレクトリに配置されている起動スクリプトのokuyama内の記述が誤っているために修正
  修正内容はクラスパスを通しているokuyamaのjarファイルが、okuyama-0.8.6.jarになっていたため、これを
  okuyama-0.8.7.jarに変更

========================================================================================================
[New - 新機能追加、不具合対応]
[[リリース Ver 0.8.7 - (2011/04/20)]]

・改修履歴
  ■メモリへのデータ保存時に圧縮を行う
	この設定はDataNode.propertiesの"dataMemory=true"の場合のみ有効
	true=圧縮、false=非圧縮
	圧縮を行えばCPU資源を利用するため圧縮効果が望めないデータを保存する場合はfalseが有効
	設定しない場合のデフォルトはtrue
	SaveDataCompressTypeは圧縮指定　1 or 9のどちらかを指定
	1=高速で低圧縮
	9=低速で高圧縮
	設定しない場合のデフォルトは1

    DataNode.propertiesでの設定は、以下の項目
	SaveDataCompress=true
	SaveDataCompressType=1


  ■データトランザクションログファイル遅延書き込み機能
	この設定はDataNode.propertiesの"memoryMode=false"の場合のみ有効
	!!falseに設定した場合は常に書き込まれないため、不意の障害時にデータをロストする可能性が上がる!!
    設定値は"true"遅延しない(常に書き込み)
    設定値は"false"遅延する
	設定しない場合のデフォルトはtrue
	この設定は本設定ファイル上で定義されているDataNode全てに反映される

    DataNode.propertiesでの設定は、以下の項目
	DataSaveTransactionFileEveryCommit=true


  ■共有データファイルへの変更書き込みのタイミング設定 ###
	 この設定は"dataMemory=false"の場合のみ有効
	 trueにした場合は共有データファイルへの変更(ディスク書き込み)を即時ディスクに反映するのではなく別スレッドで随時行う
	 書き込みが行われるまでメモリ上に保持されるのでメモリを消費する。その最大書き込みプール数(データ数)を設定するのが、
	 ShareDataFileMaxDelayCount(数値を指定する)であるここで設定した数値の最大12888倍のバイト数分メモリを消費する
	 最大遅延保持数は999999(この数だけ蓄積する前にメモリが足りなくなる場合もある)
	 設定しない場合のデフォルトはfalse

    DataNode.propertiesでの設定は、以下の項目
	ShareDataFileWriteDelayFlg=true
	ShareDataFileMaxDelayCount=


  ■ServerControllerにコマンドの種類追加
     サーバコントロールコマンドを追加

     追加した機能は以下
     "-help" : 全コマンド一覧出力
     "netdebug" : debug出力を現在のコンソールに出力する。改行送信で停止
     "fullgc" : gc指示


  ■仮想メモリの効率化
    仮想メモリの1ブロック当たりのサイズを複数の種類にして、保存されるサイズに合わせて使い分けるように変更


  ■Valueをメモリに保存する場合に設定したサイズ以上のValueを仮想メモリ空間に保存する機能を追加
    大きなValue値をメモリに保持したくない場合に有効
    DataNode.propertiesの以下の設定値(SaveDataMemoryStoreLimitSize)
    デフォルトは"0"無効(サイズ制限なしにメモリに保持する)

    DataNode.propertiesでの設定は、以下の項目
    SaveDataMemoryStoreLimitSize=0

    ※設定するサイズはバイト値
    例)以下の場合は128KB以上
    SaveDataMemoryStoreLimitSize=131072


  ■データバックアップ機能を追加
    okuyama.imdst.client.UtilClientを作成し、実行時点でのDataNodeのデータをバックアップできるように機能追加
    この機能で作成したファイルをDataNode.propertiesのKeyManagerJob1.Option=の2個目の引数のファイルとして
    DataNodeを起動するとデータが復元される
    使い方)
    java -classpath ./:./classes okuyama.imdst.client.UtilClient bkup 127.0.0.1 5554 > bkupFor5554.dump


  ■Key値に紐付くTagを削除するメソッドを追加
    Key値とTag値の両方を指定することでKey値からTagの紐付きを削除する

    OkuyamaClientではremoveTagFromKey(Key, Tag)メソッド


  ■転置インデックス作成機能と全文検索機能を追加(検索Indexの全削除も含む)
    !!! 本機能はベータ機能である !!!
    転置インデックスはN-gram方式とし、OkuyamaClientのsetValueAndCreateIndexでインデックス作成
    (ユニグラム、バイグラム、ヒストグラム方式。もしくはIndexの長さを指定)
    全文検索はOkuyamaClientのsearchValueを利用する。一度に複数の検索Wordを渡してAND検索とOR検索を指定できる。
    登録時に作成するIndexにPrefixを付加することが出来る。
    これにより、同じIndexを登録するデータ単位で別のものとして扱うことが出来る。
    検索時にPrefixを指定することで、同様のPrefixを指定してIndexを作成したデータのみ取得可能となる
    ※Index作成、検索時両方とも文字コードはUTF-8のみ対応

    OkuyamaClientでは以下のメソッドで操作する
    ・Index作成　複数の引数違いのメソッドが存在するので、OkuyamaClientのJavaDocを参照してください。(ant javadoc で作成可能)
      setValueAndCreateIndex 

    ・Index検索メソッド　複数の引数違いのメソッドが存在するので、OkuyamaClientのJavaDocを参照してください。(ant javadoc で作成可能)
      searchValue

    ・作成したIndexのみ削除
      removeSearchIndex　複数の引数違いのメソッドが存在するので、OkuyamaClientのJavaDocを参照してください。(ant javadoc で作成可能)


  ■いくつかの処理性能向上と不具合の修正


========================================================================================================
[New - 新機能追加、不具合対応]
[[リリース Ver 0.8.6 - (2011/02/11)]]
  ■複数Value一括取得機能を追加(memcachedのgetに複数のKey値を並べるのと同等)
     複数の取得したいKeyを配列で渡すことでまとめてValueを取得可能。返却される値はKeyとValueのMapで返される
	 同様のKeyを複数指定した場合は束ねて返される。
	 OkuyamaClientでは<Map>getMultiValue(String[])メソッドになる。
     memcachedプロトコルではgetの後にkey値を並べる
	 !!注意!!:PHPクライアントは未対応。


  ■Tagを指定するとそれに紐付くValueを同時に返却する機能を追加
     Tagを指定することで従来はKeyの配列を取得可能だったが、KeyとValueのMapを取得する機能を追加。
	 Tagを指定する意外は、<Map>getMultiValue(String[])と同様の挙動となる。
	 OkuyamaClientでは、<Map>getTagValues(String tag)メソッドになる。
	 !!注意!!:PHPクライアントは未対応。


  ■Tagを登録、更新する際の処理性能を向上


  ■値の演算機能を追加。演算できる種類はインクリメント、デクリメントであ(memcachedのincr、decr、append相当)
  	  =>インクリメント処理
        =>OkuyamaClientではincrValue(String key, long val)
		=>memcachedプロトコルではincr
		
  	  =>デクリメント処理
        =>OkuyamaClientではdecrValue(String key, long val)
		=>memcachedプロトコルではdecr

	 !!注意!!:PHPクライアントは未対応。


  ■ServerControllerにコマンドの種類追加
    1.1. サーバコントロールコマンドを追加
         追加した機能は以下
         "cname" : okuyamaのDNS機能を設定する => DataNodeの設定上の名前と実際の名前のMappingを変えることが出来る
                          >datanode001=datanodeserver01
                          上記のように指定すると、okuyamaは"datanode001"という名前のDataNode設定を"datanodeserver01"と読み変えてアクセスする
                          ※関係を変更したい場合は再度実行すれば上書きされる

         "rname" : okuyamaのDNSMappingを削除する
		                  rname改行後、現在の設定名を入力
                          >datanode001 

         "jobs" : 時間別の総アクセス数を返す
                       >jobs
					   
         "allsize" : すべてのIsolation別の保存データサイズを返す
                          単位はバイトになる
						  >allsize
 
         "size" : Isolation名を指定することで個別のサイズを返す
                      >size
					  >IsolationPrefix


  ■レプリケーション登録未確認機能追加
    DataNodeのレプリケーション先を設定している場合に、レプリケーションのデータを転送後、データノードで登録が正しく完了しているかを確認
	せずにクラインとには成功として返す。
    レプリケーション先の書き込み速度に依存しないので、書き込み処理が高速化される反面、ノードダウン時にデータ整合性が失われる可能性が高い。
	MasterNode.propertiesの以下の項目を変更する
	--------------------------------
	#書き込み完了確認をしない
	KeyMapDelayWrite=true
	
	#書き込み完了確認をする
	KeyMapDelayWrite=false
　--------------------------------


  ■いくつかの処理性能向上と不具合の修正
  	  
========================================================================================================
[New - 新機能追加、不具合対応]
[[リリース Ver 0.8.5 - (2011/01/18)]]
  ■MasterNode単位でのIsolation機能を追加
    Isolation機能とはokuyamaのマルチテナント化を実現する機能である。
    今までのokuyamaは保存するKeyはすべてのDataNode内でユニークな値であった(レプリケーションは除く)
    そのため、DataNodeを複数のアプリケーションや、複数のユーザが利用する場合は、Key値にプレフィックスを
    付加するなど工夫が必要であった。
    本機能は、DataNodeを共有して、独立したデータ空間を作成する機能となる。
    独立単位は、MasterNodeの設定で指定する。
    MasterNode.propertiesの"IsolationMode"と"IsolationPrefix"を利用する。
    以下の設定をMasterNode.propertiesに設定し、起動すると同様の設定をして
    起動したMasterNodeは値を共有できるが、設定をしていないMasterNodeからは取得出来なくなる。
    ------------ MasterNode.properties --------------------
    IsolationMode=true
    IsolationPrefix=XC45G
    -------------------------------------------------------

	IsolationMode=falseを指定すると、共有的な領域にアクセスすることになる。ただし、別のMasterNodeが
    指定しているIsolation空間にはアクセスできない。


  ■有効期限切れデータ自動削除
    memcachedクライアントによってexpire Timeを指定され、有効期限が切れたデータを自動的に削除する機能を追加
    実行される前提条件は"ImdstDefine.java"の"vacuumInvalidDataFlg"変数が"true"で(デフォルト"true")
    Key、Value両方がメモリの場合のみ実行される。
    ●DataNode.propertiesの設定が以下の場合のみである
    -------------- DataNode.properties --------------
    KeyManagerJob1.memoryMode=true
    KeyManagerJob1.dataMemory=true
    KeyManagerJob1.keyMemory=true

                     or                      

    -------------- DataNode.properties --------------
    KeyManagerJob1.memoryMode=false
    KeyManagerJob1.dataMemory=true
    KeyManagerJob1.keyMemory=true

                     or                      

    -------------- DataNode.properties --------------
    KeyManagerJob1.memoryMode=false
    KeyManagerJob1.dataMemory=true
    KeyManagerJob1.keyMemory=true

    ※有効期限切れ自動パージは30分に1回実行され、かつチェック時に有効期限を5分切れているデータが物理削除対象


  ■保存データサイズ(DataNodeが保存しているサイズ)を取得する機能を追加
    DataNodeが保存している値の合計バイトサイズを取得できる機能を追加
    1.1. DataNodeに接続
         "60,all"と送信:そのDataNodeが保有する全値の合計サイズが取得できる
         "60,"#" + Isolationの5文字Prefix"と送信:Isolation単位で取得できる
     ※試験的に実装しているため、DataNodeに直接接続する必要がある


  ■デバッグオプションを追加。通信ログを標準出力に出力するように機能追加
    DataNdoe、MasterNode共にすべての通信内容を標準出力にダンプする機能を追加
    1.1. 起動方法 
	     DataNode、MasterNodeともに起動時の第3引数に"-debug"を付加して起動すると、標準出力に通信内容が出力される
         例)DataNode
         java -cp ./classes;./lib/log4j-1.2.14.jar;./lib/javamail-1.4.1.jar;./lib/commons-codec-1.4.jar -Xmx128m -Xms128m okuyama.base.JavaMain /Main.properties /DataNode.properties -debug
         例)MasterNode
         java -cp ./classes;./lib/log4j-1.2.14.jar;./lib/javamail-1.4.1.jar;./lib/commons-codec-1.4.jar -Xmx256m -Xms128m okuyama.base.JavaMain /Main.properties /MasterNode.properties  -debug

     ※性能は落ちるので注意


  ■ServerControllerにコマンドの種類追加
    1.1. サーバコントロールコマンドを追加
         従来はMasterNode.propertiesおよび、MasterNode.propertiesの"ServerControllerHelper.Init="で
         定義されているポートに接続すると即Shutdownだったが、変更し種類を追加

         追加した機能は以下
         "shutdown" : サーバ停止
         "debug" : デバッグオプションtrueに動的に変更
         "nodebug" : デバッグオプションfalseに動的に変更


  ■データ全削除機能を追加
    1.1. データ削除機能として、Isolation単位および、全体を一度に消す機能を追加
         DataNodeの通常ポート(添付の設定ファイルでは5553や5554)に接続
        ・"61,#all"と送信:DataNodeのすべてが消える
        ・"61,"#"+IoslationPrefix文字列"と送信:DataNodeからIsolation単位で消える

     ※一度消したデータは復旧できないので注意が必要
     ※試験的に実装しているため、DataNodeに直接接続する必要がある

  ■Tagデータに関係する不具合修正


========================================================================================================
[New - 新機能追加、不具合対応]
[[リリース Ver 0.8.4 - (2010/12/16)]]
  ■データ有効期限を設定できる機能を追加 
    memcachedでいうところのexpireTimeを設定可能に 
    現在この設定はmemcachedクライアントからのみ設定可能である。
    つまりokuyamaのクライアントから登録した値の有効期限は無期限となる
   これによりmemcachedクライアントの登録時のすべてのOptionカラムの値が使用可能となった。

  ■排他的更新機能を追加
    memcachedでいうところのgets、cas操作に対応 
    okuyama専用クライアントではそれぞれ以下のメソッドになります

    >gets => getValueVersionCheck
    >cas  => setValueVersionCheck

  ■メモリを使用してデータを保持するストレージモードのうち 
    Keyをメモリ及び、Valueをメモリのどちらかのモードで稼動した際に 
    あらかじめ設定したメモリ使用量を超えた場合に、自動的にディスクにストアするように改修 
      =>OutOfMemory発生の予防により、より安定して稼動するように変更。
    DataNode.propertiesの以下の項目で許容メモリ使用量の上限を指定

      >KeyManagerJob1.memoryLimitSize=85                             <-使用上限のパーセント
      >KeyManagerJob1.virtualStoreDirs=./keymapfile/virtualdata1/    <-ストアするファイルディレクトリ指定


  ■Valueをディスクに保持するモード時のディスク使用率を効率化 
    =>従来はValueをディスクに保持する場合は固定長として保存していたため、 
      ValueのMaxサイズ以内の値を固定長でデータファイルに保存されていたい、 
      そのためValueのMaxサイズを大きく設定した場合は、小さいなデータであっても 
      デイスクを無駄に消費していた。 
      この部分を最適化し、Maxサイズとは別に良く使用するサイズを設定できるように変更 
      こちらの値を最適な値にしておくと、ディスク使用率の効率化とレスポンスの向上が狙える。

      >okuyama.imdst.util.ImdstDefineのdataFileWriteMaxSize変数の値(単位はバイト)を変更して再コンパイル

  ■完全ディスクモード時にディスクへの書く込み処理を非同期に変更 
    =>非同期書き込みによりデータ登録、削除性能が向上 
    =>書き込み完了まではメモリ領域を使用することで整合性を確保 

  ■サーバ間のデータ復旧処理のバグを修正 
    =>データ復旧対象のデータ数が多い場合(数百万件以上など)に、OutOfMemoryが発生するバグを修正 

  ■ネットワーク越しに停止できるように変更 
    =>従来は起動プロセスをkillコマンド等で停止するしかなかったが、あらかじめ設定したポートにアクセスすることで 
      停止するように機能追加 
      MasterNode.properties、DataNode.propertiesの以下の部分でポートを指定する

      >ServerControllerHelper.Init=15553

  ■Linux環境用のインストール用パッケージ同梱 
    =>簡単に起動できるスクリプトを同梱 
    =>リリース物のinstallディレクトリ配下にある、ReadMe.txtを参照

  ■幾つかのバグ修正と処理の効率化

========================================================================================================
========================================================================================================
[New - 新ストレージモードを追加、package構成変更、TagからKeyを取得する際に存在しないkeyか返さないOption引数を追加、Tag登録時のロックアルゴリズムを見直し、不具合対応]
[[リリース Ver 0.8.3 - (2010/11/5)]]
  ■ストレージモードに完全ディスクモードを追加
    このモードを使用することで、少ないメモリのサーバでもディスクの上限までデータを管理することができる。
    DataNode.propertiesに新たにパラメータが追加された

    DataNode.propertiesファイル内
    ---------------------------------------
    KeyManagerJob1.dataMemory=false
    KeyManagerJob1.keyMemory=false
    KeyManagerJob1.keyStoreDirs=./keymapfile/THdata1/,./keymapfile/THdata2/
    ----------------------------------------

    ※上記の状態で完全ディスクモードになる
      keyStoreDirsは保存するディレクトリになる。場所指定はカンマ区切りで定義する。
      指定ディレクトリをそれぞれ別々のディスクにするとレスポンスが向上する。


  ■パッケージ構造は大幅に変更
    前回まで使用していたパッケージ名は既にURLが別の方が取得済みでしたので、変更。
    従来のパッケージ構造から
    okuyama.imdst
    okuyama.base
    に変更
    申し訳ありませんが、この変更によりImdstKeyValueClientのimport文の変更が必要になります。
    以下となります
      import okuyama.imdst.client.ImdstKeyValueClient


  ■TagからKeyを取得する際にすでに削除されたKey値を返さないOption引数を追加
    ImdstKeyValueClientのgetTagKeysメソッドの第2引数にfalseを指定する
    
    使用例)
      imdstKeyValueClient.getTagKeys("Tag", false);

    ※PHPクライアントも同様です。


  ■Tag登録時のロックアルゴリズムを見直し
    Tag登録時のロックアルゴリズムを見直したことにより複数クライアントからの同時Tag登録のレスポンスを向上

  ■不具合対応
    いくつかのバグを修正

========================================================================================================
[New -リリース不具合、antタスク作成]
[[リリース Ver 0.8.2 - (2010/09/22)]]
  ■Version-0.8.1にてlibフォルダにmemcached.jarを配置せずにリリースしてしまいました。
    正しくlibフォルダにmemcached.jarを配置。
    これによる影響は、testフォルダのテストプログラムです。


  ■antタスクを作成
    antタスクにて、compile、jarファイル作成、サーバ起動、テストコマンド実行等が実行できるように作成
    いかがantコマンドへの引数とそれぞれの実行内容となる。
    引数                      | 内容
   ------------------------------------------------------------------------------------------------
    compile                   | コンパイルを実行
    jar                       | okuyamaのjarファイルを作成
                              |
    datanode                  | DataNodeServer起動
    slavedatanode             | SlaveDataNodeServer起動
    thirddatanode             | ThirdDataNodeServer起動
    masternode                | MasterNodeServer起動
    masternodelock            | MasterNodeServerをロック使用可能な状態で起動
    slavemasternode           | SlaveMasterNodeServer起動
    memcachedmasternode       | MemcachedプロトコルMasterNodeServer起動
    transactionnode           | 分散ロック管理NodeServer起動
    webmgr                    | Web管理コンソール用Webサーバ起動(ポート10088番)
                              |
    serverrun                 | datanode、slavedatanode、thirddatanode、masternode実行(本実行を行うとokuyamaの稼動テストは可能です)
    serverrun-slave           | datanode、slavedatanode、thirddatanode、masternode、slavemasternode実行(MasterNodeを冗長化構成で起動します)
    serverrun-memcached       | datanode、slavedatanode、thirddatanode、masternode、memcachedmasternode実行(MasterNodeをMemcachedプロトコル用のMasterNodeで冗長化構成にします)
    serverrun-transaction     | datanode、slavedatanode、thirddatanode、masternodelock、transactionnode実行(トランザクション管理ノードも起動し分散ロックを使用可能な状態にします)
    serverrun-webmgr          | datanode、slavedatanode、thirddatanode、masternode、webmgr実行(Web管理画面も合わせて起動します)
                              |
    testset                   | setコマンドを1000回実行する
    set                       | 任意のKeyとValueを登録(ant set -Dkey=key123 -Dvalue=value123)
    testget                   | getコマンドを1000回実行する(testsetで登録されたデータを取得する)
    get                       | 任意のKeyでValueを取得(ant get -Dkey=key123)
    testsettag                | Tagをセットするコマンドを500回実行する
    testgettag                | TagからKeyとValueをGetするコマンドをtestsettagで登録したTagの種類全てに実行する(tag1、tag2、tag3、tag4の4種類)
    testremove                | removeコマンドをtestsetで登録したKeyのうち500件に実行する
    testadd                   | addコマンドをテストする(Key1=Value1という組み合わせで登録する)
    testlock                  | ロック機構を使用してデータのロック、ロック中の登録、削除、ロック開放を実行する(serverrun-transactionを別コンソールで実行している場合のみ利用可能)

    ●例1(2つのコンソールをbuild.xmlと同じ場所で開く)
      コンソール1:ant serverrun
      コンソール2:ant testset
                  ant testget
      ※上記でコンソール1でokuyamaのサーバ構成を起動し、コンソール2でsetコマンド実行し、完了後getコマンドを実行している。
        実際には、コンソール1でokuyamaが起動完了まで環境によるが20秒から30秒ほどかかります。
        そのため、30秒ほど経過後、コンソール2でのテスト実行してください。

    ●例2(2つのコンソールをbuild.xmlと同じ場所で開く)
      コンソール1:ant serverrun-transaction
      コンソール2:ant testlock
      ※上記でコンソール1でokuyamaのロック使用可能状態でサーバ構成を起動し、コンソール2でLock機能のテストを実行している
        実際には、コンソール1でokuyamaが起動完了まで環境によるが20秒から30秒ほどかかります。
        そのため、30秒ほど経過後、コンソール2でのテスト実行してください。

    ●例3(2つのコンソールをbuild.xmlと同じ場所で開く)
      コンソール1:ant serverrun-webmgr
      コンソール2:ant testset
                  ant set -Dkey=key123 -Dvalue=value123
                  ant testget
                  ant get -Dkey=key123
                  ant testsettag
                  ant testgettag
                  ant testremove
      ※上記でコンソール1でokuyamaのサーバ構成とWeb管理コンソールを起動し、コンソール2でsetコマンド実行し、任意のKey=key123,Value=value123を登録、
        完了後getコマンドを実行、完了後任意のKey=key123でValueを取得、完了後Tagを使ってKeyとValueをset、完了後TagでKeyとValueを取得、完了後データを削除している。
        実際には、コンソール1でokuyamaが起動完了まで環境によるが20秒から30秒ほどかかります。
        そのため、30秒ほど経過後、コンソール2でのテスト実行してください。
        一連の動作中同サーバの以下のURLにアクセスするとWeb画面で状況を確認できる
        http://実行サーバIP:10088/okuyamamgr


  ■SlaveDataNode.propertiesでのDataNode起動ポート番号を6553と6554に変更

  ■直下のtestディレクトリ配下のファイルのJavaファイルの文字コードが一部Shift-JisだったためUTF-8に変更(同ディレクトリのclassファイルも修正後コンパイル済み)

  ■次回リリース以降、直下の大半のbatファイルや、shファイルはexecutecommandディレクトリに移動します。


========================================================================================================
========================================================================================================
[New - 機能改善]
[[リリース Ver 0.8.1 - (2010/09/21)]]
  ■トランザクションログファイルを一定サイズで自動的にローテーションするように変更
    DataNode.propertiesの"memoryMode=false"としている場合に作成される、トランザクションログファイル
    (データ操作履歴ファイル)が従来は永遠に追記されるため、いずれ肥大化し問題になるためサイズが1.8GBに
    達した時点で自動的に新しいファイルを作成し旧ファイルはファイル名の末尾に数字を付加しリネームするように
    変更。再起動時は末尾の数字が小さいもの(基本的には0から)から取り込んで最後に数字の付かないファイルを
    読み込むように変更。



  ■JavaScriptでデータ更新も行えるように変更
    従来JavaScriptはValueへの値検索や、返却時の加工にしか使えなかったが、新たにJavaScriptでデータノードに実際に
    保存されている値も更新出来るように機能拡張。
    値を更新するには、JavaScript本文中の変数値"execRet"の値を2にする。
    こうすることで最終的にJavaScript本文中の変数値"retValue"の値が実際にデータノードに再保存される。
    また、新たに"dataKey"というJavaScript変数が追加され、これにはKey値が格納されてJavaScript内から参照出来るように
    なった。
    !!注意!!:値を更新する場合はImdstKeyValueClientのgetValueScriptForUpdateメソッドを使用すること。

    JavaScript 例) 以下はKey値が"key1"の場合は文字列置換したValue値を更新後、クライアントへValueを返却し、"key1"ではない場合は更新せずに返却している。
                  "var dataValue; var dataKey; var retValue = dataValue.replace('value', 'dummy'); if(dataKey == 'key1') {var execRet = '2'} else {var execRet = '1'}";

    ●以下のテストコードで試せる
    java -cp ./;./classes;./lib\mail.jar TestSock 1.1 127.0.0.1 8888 key1 value1
    java -cp ./;./classes;./lib\mail.jar TestSock 2.4 127.0.0.1 8888 key1 "var dataValue; var dataKey; var retValue = dataValue.replace('value', 'dummy'); if(dataKey == 'key1') {var execRet = '2'} else {var execRet = '1'}"
    java -cp ./;./classes;./lib\mail.jar TestSock 2.1 127.0.0.1 8888 key1
    ↑クライアントに返却されるValue値で更新されている

    java -cp ./;./classes;./lib\mail.jar TestSock 1.1 127.0.0.1 8888 key2 value2
    java -cp ./;./classes;./lib\mail.jar TestSock 2.4 127.0.0.1 8888 key2 "var dataValue; var dataKey; var retValue = dataValue.replace('value', 'dummy'); if(dataKey == 'key1') {var execRet = '2'} else {var execRet = '1'}"
    java -cp ./;./classes;./lib\mail.jar TestSock 2.1 127.0.0.1 8888 key2
    ↑クライアントに返却されるValue値で更新されていない



  ■完全ファイルモード時にデータの一定量をメモリにキャッシュするように機能追加
    完全ファイルモード(Keyがメモリ、Valueはデータファイル)時にデータファイルがメモリに乗らないサイズになった際に発生する
    レスポンス低下を抑制するために、一定量(JVMに割り当てているメモリの10%に相当)をメモリにキャッシュする機能を追加。
    このキャッシュ機構はLRUアルゴリズムを採用しており、利用頻度の高いデータほどキャッシュされるようになり、利用頻度の
    低いデータは自動的にパージされる。
    ※現在このキャッシュをOffにすることは出来ない。



  ■データノードのメモリ活用方法を効率化し、従来よりも大量のデータをメモリ保持できるように修正
    従来のKeyとValueのメモリ上での扱い方を変更、最適化しメモリ利用効率を向上した。
    これにより従来よりも1.3～1.4倍程度のデータが扱えるように向上。



  ■データノード内でのKey値探索方式に工夫を行い、取得効率を向上
    特定のパターンでのKey値探索時の効率を向上。
    Key値の先頭や、末尾をプレフィックス的に変更してデータを保存するような場合に性能向上が期待できる。


========================================================================================================
========================================================================================================
[New - 機能改善]
[[リリース Ver 0.8.0 - (2010/09/07)]]
  ■振り分けモードにConsistentHashを追加
    データ分散アルゴリズムを従来はModのみだったが、新たにConsistentHashを追加。
    ノード追加時の自動データ移行も実装
    ConsistentHashアルゴリズム時はexecOkuyamaManager.batを起動しhttp://localhost:10088/okuyamamgrにアクセスし、
    "Add Main DataNode"に追加したいノードのIP:PORTを記述しUPDATEボタンを押下すると自動的にデータ移行が行われる
    ※Subデータノード、Thirdデータノードも運用している場合は一度に"Add Sub DataNode"、"Add Third DataNode"も
      IP:PORTを記述してUPDATEボタンを押下しないと更新に失敗する
      つまり、MainDataNodeだけ増やすとかは出来ない。
    ※MasterNodeの設定は全ノードModもしくはConsistentHashのどちらかに統一されている必要がある。
      従来のModアルゴリズムで保存したデータはConsistentHashに移行は出来ない。
    ※Modアルゴリズム時は従来通りの各ノードテキストBoxの最後に追加したいノードの"IP:PORT"を記述しUPDATEボタンを押下する

    MasterNode.propertiesの以下の設定項目で制御可能
    ●DistributionAlgorithm
        設定値) "mod"=Modアルゴリズム
                "consistenthash"=ConsistentHashアルゴリズム
        記述例)
             DistributionAlgorithm=mod


  ■DataNodeのレプリケーション先を2ノードに変更
    従来はKeyMapNodesInfoに対してSubKeyMapNodesInfoがレプリケーション先となり2ノードでデータをレプリケーション
    していたが、新たにThirdKeyMapNodesInfoを設けた。
    ThirdKeyMapNodesInfoを記述すると、レプリケーションが行われ3ノードで1組のDataNodeとして機能する。
    3ノード全てが停止しなければ稼動可能である。
    ※3つ目のノードに対するget系のアクセスはMain、Subどちらかのノードが停止しない限りは行わない。
      get系のアクセスは正しく稼動している2ノードに限定される。

    MasterNode.propertiesの以下の設定項目で制御可能
    ●ThirdKeyMapNodesInfo
        設定値) "IP:PORT"

        記述例)
             ThirdKeyMapNodesInfo=localhost:7553,localhost:7554


  ■データ取得時の一貫性モードを追加
    データ取得時にレプリケーション先の状態に合わせて取得データの一貫性を意識した取得が可能。
    モードは3種類となる。
    ・弱一貫性:ランダムにメイン、レプリケーション先のどこかから取得する(同じClient接続を使用している間は1ノードに固定される)
    ・中一貫性:必ず最後に保存されるレプリケーションノードから取得する
    ・強一貫性:メイン、レプリケーションの値を検証し、新しいデータを返す(片側が削除されていた場合はデータ有りが返る)

    MasterNode.propertiesの以下の設定項目で制御可能
    ※3つ目のノードに対するget系のアクセスはMain、Subどちらかのノードが停止しない限りは行わない。
      get系のアクセスは正しく稼動している2ノードに限定される。
    ●DataConsistencyMode
        設定値) "0"
                "1"
                "2"
 
        記述例)
             DataConsistencyMode=1


  ■ロードバランス時の振る分けの割合を設定可能に
    ロードバランス設定がtrueの場合に従来は交互にメインとレプリケーションノードにアクセスするように
    振り分けていたが、振り分ける割合を設定できるように変更

    MasterNode.propertiesの以下の設定項目で制御可能
    ※3つ目のノードに対するget系のアクセスはMain、Subどちらかのノードが停止しない限りは行わない。
      get系のアクセスは正しく稼動している2ノードに限定される。
    ●BalanceRatio
        設定値) "7:3"=振り分ける割合(メインノード:レプリケーションノード)
                ※上記の場合は7対3の割合

        記述例)
             BalanceRatio=7:3


  ■通信部分を大幅見直し
    クライアント<->MasterNode、MasterNode<->DataNode間の通信処理を改修
    Xeon3430(2.4GHz)×1、メモリ4GB程度のマシン(CentOS5.4 64bit)で10秒程度に間に順次
    10000クライアントまで接続し接続完了コネクションset,get処理を開始する同時接続テストで確認。
    (C10K問題に対応)
    ※クライアントは無操作の場合は60秒で自動的に切断される
    これに伴い以下の設定項目で通信部分のパラメータを変更しチューニング可能

    MasterNode.propertiesの以下の設定項目で制御可能
    ●MasterNodeMaxConnectParallelExecution
        設定値) 数値=同時接続時に接続直後に行うSocketラップ処理の並列数

        記述例)
             MasterNodeMaxConnectParallelExecution=10


    ●MasterNodeMaxConnectParallelQueue
        設定値) 数値=MasterNodeMaxConnectParallelExecutionで設定した並列処理への引数が設定されるキュー数
 
        記述例)
             MasterNodeMaxConnectParallelQueue=5


    ●MasterNodeMaxAcceptParallelExecution
        設定値) 数値=クライアントからデータ転送が始っていないかを確認する。並列数

        記述例)
             MasterNodeMaxAcceptParallelExecution=15

    ●MasterNodeMaxAcceptParallelQueue
        設定値) 数値=MasterNodeMaxAcceptParallelExecutionで設定した並列処理への引数を設定するキュー数

        記述例)
             MasterNodeMaxAcceptParallelQueue=5


    ●MasterNodeMaxWorkerParallelExecution
        設定値) 数値=データ転送開始状態のSocket登録に対して処理する並列処理数。

        記述例)
             MasterNodeMaxWorkerParallelExecution=15


    ●MasterNodeMaxWorkerParallelQueue
        設定値) 数値=MasterNodeMaxWorkerParallelExecutionで設定した並列処理への引数を設定するキュー数

        記述例)
             MasterNodeMaxWorkerParallelQueue=5


    DataNode.propertiesの以下の設定項目で制御可能
    ●KeyNodeMaxConnectParallelExecution
        MasterNodeMaxConnectParallelExecutionと同様

    ●KeyNodeMaxConnectParallelQueue=5
        MasterNodeMaxConnectParallelQueueと同様

    ●KeyNodeMaxAcceptParallelExecution=20
        MasterNodeMaxAcceptParallelExecutionと同様

    ●KeyNodeMaxAcceptParallelQueue=5
        MasterNodeMaxAcceptParallelQueueと同様

    ●KeyNodeMaxWorkerParallelExecution=15
        MasterNodeMaxWorkerParallelExecutionと同様

    ●KeyNodeMaxWorkerParallelQueue=5
        MasterNodeMaxWorkerParallelQueueと同様


  ■テストケースを追加
    リリース物直下のexecTest.batを実行するとget、setのメソッドテストと、DataNode再起動のテストが実行される
    このツールはWindowsにCygwinがインストールされ、CygwinのbinにPATHが通っている想定である。
    特殊な環境ですいません。
    リリース物内のclasses\Test.propertiesないのパスを適当な値に書き換えてください。

========================================================================================================
========================================================================================================
[New - 機能改善]
[[リリース Ver 0.7.0 - (2010/06/27)]]
  ■MasterNodeを複数起動し冗長化した場合の自動エスカレーション機能を追加
    従来からMasterNodeを複数で冗長化は出来たが、その場合MasterNode内にメインとなるノードが存在し、
    残りのマスターノードはスレーブという扱いだった。
    メインのノードがダウンした場合は、スレーブノードにアクセスすればデータの取得、登録、削除等全ての
    クライアント操作は実行できたが、DataNodeの監視、データノードダウン後の起動時のデータリカバーは
    スレーブMasterNodeだけでは実行されなかった。
    その場合は、スレーブMasterNodeの内の1インスタンスのMasterNode.properties内の設定値
    "MainMasterNodeMode"を"true"に変える必要があった。
    今回の改修でメインMasterNodeがダウンした場合はスレーブMasterNode内から自動的に1インスタンスが
    メインMasterNodeに昇格するように改修。
    この改修によりMasterNode.propertiesに設定項目が追加され、従来の設定項目が使用可能ではあるが、推奨されなくなった。

    ●追加された項目は以下
      ・SystemConfigMode
        説明) 設定情報を取得する場所(file or node)
              設定情報を本ファイルを起動後も参照し続けるか、起動後は本ファイルを一度だけ参照し、
              以後は、DataNodeに登録されている設定情報を参照するかを決定する
              "file"の場合は本ファイルを参照する
              "node"の場合はDataNodeを参照する
              設定をしない場合は"node"となる
        記述例)
             SystemConfigMode=node


      ・MyNodeInfo=127.0.0.1:8888
        説明) 自身の情報
              自身のIPと起動ポート番号を":"区切りで記述
              ※使用を推奨
              ※この設定がない場合はメインMasterNodeの自動昇格機能が機能しない
        記述例)
             MyNodeInfo=127.0.0.1:8888


      ・MainMasterNodeInfo
        説明) メインマスターノードの情報
              起動時にメインMasterNodeとして認識するノードのIPとポート番号
              自身がメインMasterNodeの場合は自身の情報を記述
              ※使用を推奨
        記述例)
             MainMasterNodeInfo=127.0.0.1:8888


      ・AllMasterNodeInfo
        説明) 全てのマスターノードの情報
              全てのマスターノードの情報"IP:PORT番号"フォーマットで","区切りで記述 
              自身の情報はMyNodeInfo設定の内容と同じであること
              ここでの記述順でメインMasterNodeとして機能する
              ※使用を推奨
              ※この設定がない場合はメインMasterNodeの自動昇格機能が機能しない
        記述例)
             AllMasterNodeInfo=127.0.0.1:8888,127.0.0.1:8889,127.0.0.1:11211

    ●使用が推奨されなくなった項目
      ・MainMasterNodeMode
      ・SlaveMasterNodes


  ■MasterNodeが使用する設定情報をMasterNode.propertiesから参照するだけでなく、
    DataNodeに設定情報を格納しそちらか参照するように改修
    従来設定情報はMasterNode.propertiesからつねに参照していたが、設定情報をDataNodeに格納する
    ように改修し、全MasterNodeが情報を共有するよに改修
    ただし、起動時はDataNodeの情報が分からないため、MasterNode.propertiesから参照する
    従来通り、MasterNode.propertiesのみで運用することも可能
    この設定を変更するにはMasterNode.propertiesの以下の設定項目で変更出来る。
    ●追加された項目
      ・SystemConfigMode
        説明) 設定情報を取得する場所(file or node)
              設定情報を本ファイルを起動後も参照し続けるか、起動後は本ファイルを一度だけ参照し、
              以後は、DataNodeに登録されている設定情報を参照するかを決定する
              "file"の場合は本ファイルを参照する
              "node"の場合はDataNodeを参照する
              設定をしない場合は"node"となる
        記述例)
             SystemConfigMode=node



  ■okuyama管理Webコンソールアプリケーションを追加
    稼働中のokuyamaの状況確認と設定の変更が出来るできるWebアプリを作成。
    リリース物のexecOkuyamaManager.batを実行すると管理Webアプリが起動する。

    URL : http://起動マシンのIP:10088/okuyamamgr
    でアクセスできる。

    ※execOkuyamaManager.bat内で起動ポート番号(10088番)とWebアプリが情報を参照するMasterNodeのIP:PORTを","区切りで渡しています。
      MasterNodeの情報はMasterNode.propertiesの設定情報"AllMasterNodeInfo"の内容と同様にしてください。


========================================================================================================
========================================================================================================
[New - 機能改善]
[[リリース Ver 0.6.6 - (2010/06/08)]]
  ■データノードリカバー中の取得、登録、削除の処理の待ち時間を軽減
    データノード、スレーブデータノードの構成で稼動している場合、片方のノードがダウンし、
    起動してくると、片側のノードのデータから復元するリカバー処理が行われる。従来この処理中はクライアントから
    当該ノードのデータにアクセスするとキューイングされていたため、待ちに状態になっていた。
    この部分を見直し、リカバー処理中の待ちとなるタイミングを大幅に軽減した。
    このことによりokuyamaの総使用時間に対する、スループットが向上された。
    同時に、従来復旧後のノードのデータはokuyamaが内部で使用しているデータMapをシリアライズして
    書き出していたが、この部分をトランザクションログと統合しログに書き出すように変更した。
    このことでリカバー処理の総所要時間を低減された。
    ※この改善はデータノード、スレーブデータノード両方を使用して稼動している場合に有効である。

========================================================================================================
========================================================================================================
[New - 機能改善]
[[リリース Ver 0.6.5 - (2010/05/30)]]
  ■Vacuum処理中の取得、登録、削除の処理を継続できる用に改修
    従来Vacuum中は取得、登録、削除処理は処理がブロックされるようになっていた(処理は待ち状態になる)が
    このブロック時間を大幅に削減するように改修。
    従来ならVacuumが始まると終始ブロックされていたが、処理を継続出来るように(待ちが発生しない)なり
    okuyamaの総使用時間に対する、スループットが向上される。
    ※VacuumはDataNode.propertiesの"KeyManagerJob1.dataMemory=true"の用にvalueをファイルに保存している
      場合のみ有効となる。

  ■各propertiesファイルにコメントを追加
    MasterNode.properties、DataNode.properties、TransactionNode.propertiesにコメントを大幅に追記
    ※今までコメントが少なくて申し訳ありませんでした。

  ■ReadMe.txt、ReadMe-UTF.txtの[■機能説明とサンプルの実行方法]、[サンプルの実行方法]部分を追記
    TransactionNodeの使用方法、Memcached互換での起動方法、クライアントの使用可能メソッド部分を修正
    分散Lock、setNewValue(memcacheのadd相当)の使用サンプルを追記

========================================================================================================
========================================================================================================
[New - 機能改善]
[[リリース Ver 0.6.4 - (2010/05/21)]]
  ■データの保存時にKey値も保存するように変更
    従来はKey値から生成したHash値を保存していたが限りなく低い可能性ではあるが衝突を起こす可能性がある為、
    大量のデータ保存に向かないので、Key値も文字列として保存するように変更。
    同時にKey値にも長さの制限を付加。(2048byte)
    ※この変更により大変申し訳ありませんが、従来バージョンでのデータは使用できなくなります。

  ■同期処理部分を見直し、処理効率を向上。
    

========================================================================================================
========================================================================================================
[New - 機能追加]
[[リリース Ver 0.6.3 - (2010/05/19)]]
  ■データノードをmemcacheのノードとして利用可能に
    マスターノードを起動せずに直接データノードをmemcacheのノードとして利用可能。
    以下のように設定を変更しexecDataNode.batを実行するとmemcacheクライアントでアクセスできる。

    設定ファイルDataNode.propertiesの25行目
    ----------------------------------
    KeyManagerHelper.Init=
              ↓上記を下記内容に変更
    KeyManagerHelper.Init=memcache
    ----------------------------------
    として起動するとmemcacheプロトコルで会話が可能となる。
    対応メソッドはマスターノードをmemcacheモードとして起動した場合と同様となる。
    (・set, ・get, ・add, ・delete)(flagに対応)

    ファイルへのデータ永続化が可能
    設定ファイルDataNode.propertiesの30行目、31行目
    ----------------------------------
    KeyManagerJob1.memoryMode=false       
    KeyManagerJob1.dataMemory=true
    ----------------------------------
    上記の設定でトランザクションログは残し、登録されたデータはメモリに保持する
    両方はtrueにすると完全メモリモード(最も高速に稼動)(単体でmemcacheとほぼ同程度の処理速度が出る)
    両方はfalseにすると完全ファイルモード(最も大量のデータ(Valueのサイズ)を保持可能)

    デフォルトでは2560バイトがvalueサイズの最大値となるので、src\org\imdst\util\ImdstDefine.javaの150行目を
    変更しcompile.batを実行しコンパイルすると許容できるデータサイズが変更できる。


  ■Key値からHash値を求めるロジックを変更
    okuyamaでは登録されたKey値はハッシュ値を求めてその値を実際の登録に使用しているが、
    その値の生成ロジックを見直し、よりハッシュ値が分散するように変更
    この変更により、今まで登録したデータは全て破棄する必要があります。
    この変更が受け入れられない場合はsrc\org\imdst\helper\MasterManagerHelper.javaの2660行目、2661行目を
    以下のように変更し、compile.batを実行し再コンパイルを実行。
    --------------------------------------------------------------------
    private int hashCodeCnv(String str) {
        return new HashCodeBuilder(17,37).append(str).toHashCode();
        //return str.hashCode();
    }
               ↓↓↓↓変更(コメントアウトを入れ替え)
    private int hashCodeCnv(String str) {
        //return new HashCodeBuilder(17,37).append(str).toHashCode();
        return str.hashCode();
    }
    --------------------------------------------------------------------


  ■データ登録メソッドsetValue時の処理速度を20%向上
    データノード、スレーブデータノード起動時にsetValueを実行した際の処理速度を
    20%向上。データノードへの登録リクエスト送信中にスレーブデータノードへの送信準備をするように修正。


  ■バグFix
    

========================================================================================================
========================================================================================================
[New - 機能追加]
[[リリース Ver 0.6.2 - (2010/05/09)]]
  ■Memcaheプロトコルモード時の以下の処理を対応
    1.memcacheのメソッドであるaddに対応
      未登録データの場合のみ登録可能なmemcacheのaddコマンドに対応

    2.memcacheのメソッドであるdeleteに対応
      memcacheコマンドであるデータ削除用コマンドdeleteに対応

    3.memcacheのflag登録に対応
      memcacheコマンドでset、add時に指定するflagに対応
      get時に登録flagを返却

  ■データノード間のデータリカバー時のデータ転送方式を一部変更
    従来はノードダウンからのリカバー時にレプリケーションノードからの1通信で全ての登録データ取得していたいが、
    これでは大きなデータが登録されている場合に、送信側、受信側でメモリにのりきらずにリカバーに失敗する場合が
    あったため、使用可能なメモリの残量を確認しながら、データを分割して転送しリカバリーするように変更
    ※データの保存方式をメモリではなくファイルにしている場合は、特にこの問題は発生する可能性があった。

  ■PHP用クライアント(OkuyamaClient.class.php)にgetByteValueメソッドを追加
    Java用クライアントで登録したバイトデータ(setByteValueで登録したデータ)を取得する際に使用

  ■バグFix

========================================================================================================
========================================================================================================
[New - 機能追加]
[[リリース Ver 0.6.1 - (2010/04/21)]]
  ■データが存在しない場合のみ保存できるメソッドを追加
    +未登録のキー値である場合のみ登録可能となり、既に登録済みの場合は登録できない。

     *未登録の場合のみ登録可能なメソッドは以下である。
      ・クライアントのメソッド名:setNewValue
      ・引数1:Key値
      ・引数2:Value値
      ・戻り値:String[] 要素1(データ有無):"true" or "false",要素2(失敗時はメッセージ):"メッセージ"

      ・クライアントのメソッド名:setNewValue
      ・引数1:String Key値
      ・引数2:String[] tag値配列
      ・引数3:String Value値
      ・戻り値:String[] 要素1(データ有無):"true" or "false",要素2(失敗時はメッセージ):"メッセージ"

  ■クライアントから接続時に保存出来る最大データサイズをMasterNodeから取得するように変更

  ■バグFix

========================================================================================================
========================================================================================================
[New - 機能追加]
[[リリース Ver 0.6.0 - (2010/04/08)]]
  ■分散ロック機能を追加
    +任意のデータをロックする機能を追加。
    +分散ロック機能はマスターノード用設定ファイルである、MasterNode.propertiesの9行目の"TransactionMode=true"で
     ロック機能が使用可能となる。
     また、72行目の"TransactionManagerInfo=127.0.0.1:6655"でTransactionManagerノードを指定する必要がある
     そして、TransactionManagerノードが起動している必要があるため、同梱のexecTransactionNode.batで起動する。
     分散ロック機能を使用する場合は、全てのマスターノードが"TransactionMode=true"で起動している必要がある。
     同梱の設定ファイルは全て分散ロック機能で起動する設定となる。
     ※execMasterNodeMemcached.batは分散ロック機能あり、memcacheプロトコルモードで起動する。
     また、従来の分散ロック機能なしで起動する場合は、"TransactionMode=false"としてexecMasterNode.batを実行する。

    +仕組みとしては、Clientからロック取得依頼を行った場合、TransactionManagerノードに指定したKey値で
     ロック情報を作り上げる。この際、すでに別Clientから同一のKey値でロックが取得されている場合は、
     指定した時間の間、ロックが解除されるのを待ち、取得を試みる。
     ロックされた値に対して、set,remove系のアクセスを行った場合は、TransactionManagerノードに対して該当の
     Key値が、リクエストを発行したClient以外からロックされているかを問い合わせて、別クライアントがロック
     している場合は、ロックが解除されるのを待ち続ける。
     同クライアントがロックしているもしくは、ロックがない場合は、そのまま処理を続行する。
     ロックのリリースも同じ動きである。
     なお、分散ロック機能を有効にした場合は、無効時と比べ1回通信が多く発生するため、処理速度は落ちる。
     また、TransactionManagerノードがSPOFとなるが、機能していない場合は無視して稼動するが、
     処理速度は極端に劣化する。
     今後、SPOFとならないように改善予定である。

    +以下は説明となる
     *ロックを実施したデータの挙動は以下となる。
      ・ロック可能なKey値(データ)は現在登録済みであっても、登録されていなくても可能である。
      ・1クライアントから同時に複数のデータをロック可能である
      ・ロックしたデータはロックを実施したクライアントからのみロック解除可能である。
      ・ロック中のデータはロックを実施したクライアントからのみ登録可能である。
      ・ロック中のデータはロックを実施したクライアントからのみ変更可能である。
      ・ロック中のデータはロックを実施したクライアントからのみ削除可能である。
      ・ロック中のデータは全クライアントから参照可能である。
 
     *ロック機能使用開始メソッドは以下である。
      ・クライアントのメソッド名:startTransaction
      ・引数なし
      ・戻り値:boolean true:スタート成功 false:スタート失敗
        ※ロック機能有りでTransactionManagerノードを起動していない場合は、スタートに失敗する。
          
     *ロックメソッドへの引数と戻り値は以下である。
      ・クライアントのメソッド名:lockData
      ・引数1:ロック対象Key値
        引数2:ロック継続時間
              (ロック解除を行わない場合でも、ここでの設定時間が経過すると自動的に解除される。
               単位は秒。
               0を設定するとロックを実施したクライアントが解除するまで永久にロックされる。
               ※0指定は推奨しない)
        引数3:ロック取得待ち時間
              (既に別クライアントがロック中のデータへロックを実施した場合に、設定時間の間ロック取得をリトライする。
               単位は秒。
               0を設定すると1回ロックを試みる)
 
      ・戻り値:String配列
               String配列[0]:Lock成否 "true"=Lock成功 or "false"=Lock失敗
 
     *ロック開放への引数と戻り値は以下である。
      ・クライアントのメソッド名:releaseLockData
      ・引数1:ロック対象Key値
 
      ・戻り値:String配列
               String配列[0]:開放成否 "true"=開放成功 or "false"=開放失敗

     *ロック機構使用終了メソッドは以下である。
      ・クライアントのメソッド名:endTransaction
      ・引数なし
      ・戻り値なし

    +Java版、PHP版のクライアントからは、ロック、リリース両方が可能
     Memchacheクライアントはロック、リリース機能は利用できないが、Lock中のデータにsetを実行した場合は"待ち状態"に入る。

   │※ImdstKeyValueClientを使用した実装例)─────────────────────────────────┐
   │                                                                                                        │
   │ // クライアントインスタンス作成                                                                        │
   │ ImdstKeyValueClient client = new ImdstKeyValueClient();                                                │
   │ // 接続                                                                                                │
   │ imdstKeyValueClient.connect("127.0.0.1", 8888);                                                        │
   │ // Transactionを開始してデータをLock後、データを更新、取得し、Lockを解除                               │
   │                                                                                                        │
   │ // 引数はLock対象のKey値, Lock維持時間(秒)(0は無制限), Lockが既に取得されている場合の                  │
   │ // 取得リトライし続ける時間(秒)(0は1回取得を試みる)                                                    │
   │ ImdstKeyValueClient imdstKeyValueClient = new ImdstKeyValueClient();                                   │
   │ imdstKeyValueClient.connect(args[1], port);                                                            │
   │ String[] ret = null;                                                                                   │
   │                                                                                                        │
   │ // Lock準備                                                                                            │
   │ if(!imdstKeyValueClient.startTransaction()) throw new Exception("Transaction Start Error!!");          │
   │                                                                                                        │
   │ long start = new Date().getTime();                                                                     │
   │                                                                                                        │
   │ // Lock実行                                                                                            │
   │ // "DataKey"というKey値で10秒間維持するロックを作成。もし既にロックされている場合は、5秒間ロック取得を │
   │ // 繰り返す                                                                                            │
   │ ret = imdstKeyValueClient.lockData("DataKey", 10, 5);                                                  │
   │ if (ret[0].equals("true")) {                                                                           │
   │     System.out.println("Lock成功");                                                                    │
   │ } else if (ret[0].equals("false")) {                                                                   │
   │     System.out.println("Lock失敗");                                                                    │
   │ }                                                                                                      │
   │                                                                                                        │
   │                                                                                                        │
   │ // 以下のコメントアウトをはずして、コンパイルし、                                                      │
   │ // 別のクライアントから更新を実行すると、更新できないのがわかる                                        │
   │ //Thread.sleep(5000);                                                                                  │
   │                                                                                                        │
   │ // 自身でロックしているので更新可能                                                                    │
   │ if (!imdstKeyValueClient.setValue(args[3], "LockDataValue")) {                                         │
   │   System.out.println("登録失敗");                                                                      │
   │ }                                                                                                      │
   │                                                                                                        │
   │ // 取得                                                                                                │
   │ ret = imdstKeyValueClient.getValue(args[3]);                                                           │
   │ if (ret[0].equals("true")) {                                                                           │
   │     // データ有り                                                                                      │
   │     System.out.println("Lock中に登録したデータ[" + ret[1] + "]");                                      │
   │ } else if (ret[0].equals("false")) {                                                                   │
   │     System.out.println("データなし");                                                                  │
   │ } else if (ret[0].equals("error")) {                                                                   │
   │     System.out.println(ret[1]);                                                                        │
   │ }                                                                                                      │
   │                                                                                                        │
   │ // 自身でロックしているので削除可能                                                                    │
   │ ret = imdstKeyValueClient.removeValue(args[3]);                                                        │
   │                                                                                                        │
   │ if (ret[0].equals("true")) {                                                                           │
   │     // データ有り                                                                                      │
   │     System.out.println("Lock中に削除したデータ[" + ret[1] + "]");                                      │
   │ } else if (ret[0].equals("false")) {                                                                   │
   │     System.out.println("データなし");                                                                  │
   │ } else if (ret[0].equals("error")) {                                                                   │
   │     System.out.println(ret[1]);                                                                        │
   │ }                                                                                                      │
   │                                                                                                        │
   │ // Lock開放                                                                                            │
   │ ret = imdstKeyValueClient.releaseLockData(args[3]);                                                    │
   │ if (ret[0].equals("true")) {                                                                           │
   │     System.out.println("Lock開放成功");                                                                │
   │ } else if (ret[0].equals("false")) {                                                                   │
   │     System.out.println("Lock開放失敗");                                                                │
   │ }                                                                                                      │
   │                                                                                                        │
   │ long end = new Date().getTime();                                                                       │
   │ System.out.println((end - start) + "milli second");                                                    │
   │                                                                                                        │
   │ // トランザクション開放                                                                                │
   │ imdstKeyValueClient.endTransaction();                                                                  │
   │ // 接続切断                                                                                            │
   │ imdstKeyValueClient.close();                                                                           │
   └────────────────────────────────────────────────────┘

  ■いくつかのバグを修正

  ※今後は、分散トランザクションを実現するように実装を進める。
========================================================================================================
========================================================================================================
[New - 機能追加]
[[リリース Ver 0.5.2 - (2010/03/28)]]
  ■Memcacheプロトコルに一部対応
    KVSの標準プロトコルになりつつある、memcacheのプロトコルに対応するモードを追加
    MasterNode.propertiesの14行目"MasterManagerJob.Option="を"MasterManagerJob.Option=memcache"とすると
    memcacheプロトコルでアクセス可能である。
    MasterNode2.propertiesがmemcache用の設定ファイルになっている。
    execMasterNodeMemcached.batを実行するとmemcacheプロトコルで立ち上がる。
    対応メソッドはsetとgetである。またset,getのflagは0のみ対応している。
    今後対応範囲を増やす予定。

  ■データ保存形式をファイル(DataNode.properties30行目、33行目をfalseとした場合)にした場合に、
    追記型で記憶しているため、ファイルが永遠に肥大化するため、vacuum機能を追加。
    自動的に実行される。

  ■documetディレクトリを追加
    性能評価を実施した資料と、構成図を配置

========================================================================================================
========================================================================================================
[New - 機能追加]
[[リリース Ver 0.5.1 - (2010/03/17)]]
  ■PHP用のクライアントを作成
    PHPでMasterServerへアクセス出来るようにクライアントを作成。
    Javaのコードを焼きなおしました。
    バイトデータを登録(setByteValue)、取得(getByteValue)するメソッドのみ未実装。
    リリース物etc_client\OkuyamaClient.class.phpになります。
    サンプル実行コードetc_client\PhpTestSock.phpと、実行用batファイルetc_client\PhpAutoTest.batを同梱しました。

  ■ReadMe.txt、ReadMe-UTF.txtを最新の状態に更新

  ■ReadMe.txt、ReadMe-UTF.txtの"[[リリース Ver 0.5.0 - (2010/03/17)]]"の記述ミスを訂正
    訂正箇所は以下
    -------------------------------------------------------------------------------------------------------------------------------------------
    ■TestSockサンプルにScript実行モードのバージョンを追加(引数 "2.3" Script実行)
    ・取得、実行サンプル起動方法
    java -cp ./;./classes;./lib/javamail-1.4.1.jar TestSock 2.3 127.0.0.1:8888 20000 "var dataValue; var retValue = dataValue.replace('data', 'dummy'); var execRet = '1';"
                                                                        ^^^
                                                               正しくは 127.0.0.1 8888
    -------------------------------------------------------------------------------------------------------------------------------------------

========================================================================================================
========================================================================================================
[New - 機能追加]
[[リリース Ver 0.5.0 - (2010/03/17)]]
  ■データ取得時にJavaScriptを実行可能なインターフェースを追加
    ImdstKeyValueClientのgetValueScriptメソッドで実行可能。
    データ取得時にJavaScriptで記述したスクリプトをKey値と同時に渡し、Key値でValue値が取得出来た場合、
    その値にスクリプトを実行しその結果を取得できる。
    スクリプト内で、返却有無の決定及び、返却値(Value)を設定することが出来る。
    スクリプトはデータノードで実行されるため、今まで取得したデータに対して何だかの処理で加工or値の妥当性
    検証などを行っていた場合は、スクリプトで処理を代行し、取得マシンのリソースの節約や、取得マシンの
    スペックを越えるような大規模なデータもデータノードのパワーを使用して処理可能である。

    【スクリプト記述制約】
     スクリプトの制約は以下の名前の変数を宣言する必要がある。
    ・ "dataValue" = key値で取得出来たvalue値が設定される。スクリプト内ではこの変数がvalue値となる。
    ・ "execRet" = 実行結果(retValue変数)をクライアントに返すことを指定
                   (1を代入すると返却される 0を代入すると返却されない)
    ・ "retValue" = 実行結果を格納する。クライアントに返される値

   │※ImdstKeyValueClientを使用した実装例)─────────────────────────────────┐
   │                                                                                                        │
   │ StringBuffer scriptBuf = new StringBuffer();                                                           │
   │ // スクリプトを作成                                                                                    │
   │ scriptBuf.append("var dataValue;");                                                                    │
   │ scriptBuf.append("var execRet;");                                                                      │
   │ scriptBuf.append("var retValue;");                                                                     │
   │ // 取得したValue値に"data"という文字がある場合は"dummy"に置換する                                      │
   │ scriptBuf.append("retValue = dataValue.replace('data', 'dummy');");                                    │
   │ // 返却指定                                                                                            │
   │ scriptBuf.append("execRet = '1';");                                                                     │
   │                                                                                                        │
   │ // クライアントインスタンス作成                                                                        │
   │ ImdstKeyValueClient client = new ImdstKeyValueClient();                                                │
   │ // 接続                                                                                                │
   │ imdstKeyValueClient.connect("127.0.0.1", 8888);                                                        │
   │ // Value取得及び、スクリプト実行を依頼                                                                 │
   │ String[] retValue = imdstKeyValueClient.getValueScript("key1", scriptBuf.toString());                  │
   │                                                                                                        │
   │ // 結果を表示                                                                                          │
   │ // 実行結果が存在する場合は"true"が存在しない場合は"false"が、エラーの場合は"error"が返却される        │
   │ System.out.println(retValue[0]);                                                                       │
   │ // retValue[0]が"true"の場合はスクリプトからの返却値が返却される。"error"の場合はエラーメッセージが返却│
   │ System.out.println(retValue[1]);                                                                       │
   └────────────────────────────────────────────────────┘

  ■TestSockサンプルにScript実行モードのバージョンを追加(引数 "2.3" Script実行)
    ・取得、実行サンプル起動方法
    java -cp ./;./classes;./lib/javamail-1.4.1.jar TestSock 2.3 127.0.0.1 8888 20000 "var dataValue; var retValue = dataValue.replace('data', 'dummy'); var execRet = '1';"

  ■クライアントにマスターノードの自動バラランシングモード及び、ダウン時の再接続機能を追加
    ImdstKeyValueClientのsetConnectionInfosメソッドに接続対象となるマスターノードの接続文字列を配列で
    セット(フォーマット"IP:PORT番号"のString配列)し、autoConnectメソッドで接続すると、ノードへの接続が
    出来ない場合、接続後処理途中で切断された場合なども、自動的に再接続し稼動し続けることが出来る。


  ■TestSockサンプルに自動接続モードのバージョンを追加(引数 "1.2"自動接続で登録  "2.2"自動接続で取得)
    ・登録サンプル起動方法
    java -cp ./;./classes;./lib/javamail-1.4.1.jar TestSock 1.2 "127.0.0.1:8888,127.0.0.1:8889" 20000
    ・取得サンプル起動方法
    java -cp ./;./classes;./lib/javamail-1.4.1.jar TestSock 2.2 "127.0.0.1:8888,127.0.0.1:8889" 20000

    ※execMasterNode.batとexecMasterNode2.batを同時に実行した状態で上記を実行して、片側ずつ停止しては、
      再実行を繰り返しても、正しく稼動し続けることが確認できます。

========================================================================================================
========================================================================================================
[New - 機能追加]
[[リリース Ver 0.4.0 - (2010/03/15)]]
 ■データノードの動的追加をサポート
   マスターノード、データノード起動中にMasterNode.propertiesのKeyMapNodesRule、KeyMapNodesInfo、
   SubKeyMapNodesInfoに新たなノードの記述を追記し保存すると、自動的にファイルが再読み込みされ、
   データ、スレーブ両ノードが追加される。
   設定ファイルは再保存されるとほぼリアルタイムに反映されるため、保存前に該当ノードを起動しておく必要がある。
    ※元設定                                                ※ノード追加
   ┌─ MasterNode.properties─────────────┐  ┌─ MasterNode.properties────────────────────────────┐
   │KeyMapNodesRule=2                                 │  │KeyMapNodesRule=4,2                                                             │
   │                                                  │  │                                                                                │
   │KeyMapNodesInfo=localhost:5553,localhost:5554     │=>│KeyMapNodesInfo=localhost:5553,localhost:5554,localhost:6553,localhost:6554     │
   │                                                  │保│                                                                                │
   │SubKeyMapNodesInfo=localhost:5556,localhost:5557  │存│SubKeyMapNodesInfo=localhost:5556,localhost:5557,localhost:6556,localhost:6557  │                        │
   │                                                  │  │                                                                                │
   └─────────────────────────┘  └────────────────────────────────────────┘

 ■データノード追加後に新しいノードへデータの移行を行う機能を追加
   データノード追加後に過去データノード台数運用時のデータにアクセスしたタイミングで追加後ノードの
   メインデータノード、スレーブデータノードへデータを自動的に保存するようにし、以後過去のデータ保存ノードへ
   アクセスを行わないように機能を追加。
   ※ノード追加を行うと自動的にデータアクセス時に行われる。

 ■データノードへのアクセスをメインデータノード、スレーブデータノード間でバランシング出来るモードを追加
   MasterNode.propertiesのLoadBalanceModeの設定をtrueにするとバランシングを行う。
   メインと、スレーブで性能が大きく異なる場合はバランシングを行わないほうが良い場合もある。
   振り分けは単純なラウンドロビン方式である。

 ■マスターノードを複数台稼動させ、負荷分散、冗長化出来る機能を追加
   今までは、マスターノードは1台構成だったが、SPOFとなっていた為、複数台起動出来るように機能追加。
   マスターノードは1～n台での構成が可能だが、1台は必ずマスターノード内でのメインにならなければならない。
   理由は、データノードの生存監視と復旧時のリカバリー処理の為である。
   リカバリー処理時は、全てのマスターノードが同調して稼動するため、不整合は発生しない構成となっている。
   MasterNode.propertiesのMainMasterNodeModeをメインの場合はtrueとし、スレーブの場合はfalseとする。
   また、スレーブのマスターノードのネットワーク上の名前と稼動ポート番号をSlaveMasterNodesにカンマ区切りで列挙する。
   ※冗長化しない場合はMainMasterNodeMode=trueとするだけでよい。

  リリース物\(src or classes)\MasterNode.properties(メイン用) リリース物\(src or classes)\MasterNode2.properties(スレーブ用)
 ┌─ MasterNode.properties─────────────┐      ┌─ MasterNode2.properties ────────────┐
 │MainMasterNodeMode=true                           │      │MainMasterNodeMode=false                          │
 │                                                  │      │                                                  │
 │SlaveMasterNodes=127.0.0.1:8889                   │      │SlaveMasterNodes=                                 │
 │                                                  │      │                                                  │
 └─────────────────────────┘      └─────────────────────────┘

   メインのマスターノードで、データノードの監視、復旧を行うが、メインのマスターノードが稼動出来ない状態に
   なった場合は、スレーブのマスターノードの設定ファイルを以下のように書き換えて再保存すると、
   スレーブのマスターノードがメインのマスターノードに変更されて稼動し始める。
 ┌─ MasterNode2.properties   ─────────────┐
 │MainMasterNodeMode=true                               │
 │                                                      │
 │SlaveMasterNodes=(別のマスターノードがある場合は記述) │
 │                                                      │
 └───────────────────────────┘
   ※SlaveMasterNodesに列挙したノードが稼動していなくても、メインノード正しく稼動する。
   ※自動的にスレーブがメインに昇格するように後ほど実装予定。
   ※ImdstKeyValueClientに複数のマスターノードを設定できるようにし、
     バランシングや、接続できない場合の自動別ノード再接続機能などを後ほど実装予定。

 ■起動batファイル追加
   execMasterNode2.bat <=スレーブマスターノード起動コマンド
   execMasterNode.batのみでの稼動は従来と同じように可能
========================================================================================================
========================================================================================================
[New - 機能改善]
[[リリース Ver 0.3.3 - (2010/03/12)]]
 ■データノード同士のデータリカバリ時に従来は起動中のノードのデータを再起動してきたノードに
   無条件でリカバリしていたいが、データの登録、削除に実施時刻の要素を追加し、リカバリ時に実施時刻を
   確認し、新しいノードのデータを適応するように改善。

========================================================================================================
========================================================================================================
[New - 不具合修正&サンプルコード追加]
[[リリース Ver 0.3.2 - (2010/03/10)]]
 ■一定数のKey-Valueを同じTagに紐付けて保存すると正しく取り出せない不具合を修正

 ■TestSockにキー値を指定して削除するモードを追加(引数"8")
   java -cp ./;./classes;./lib/javamail-1.4.1.jar TestSock 8 127.0.0.1 8888 KeyName1
   上記で127.0.0.1のポート8888番で起動しているマスターノードに接続し、"keyName1"というKey値で保存されて
  いるデータを削除する。
========================================================================================================
========================================================================================================
[New - 削除メソッドを実装&データ保存をメモリ上とファイルを選択できるように機能追加]
[[リリース Ver 0.3.0 - (2010/03/4)]]
 ■削除メソッドを追加
   ImdstKeyValueClientのremoveValueメソッドにて呼び出し可能
   リターン値はgetValueと同様で結果文字列("true" or "false")と削除できた場合は対象の値が格納された配列
   TestSockの"7"番指定で呼び出し可能
   ---------------------------------------------------------------------------------------------------
   java -cp ./;./classes;./lib/javamail-1.4.1.jar TestSock 1 127.0.0.1 8888 100         <= 100件登録
   java -cp ./;./classes;./lib/javamail-1.4.1.jar TestSock 7 127.0.0.1 8888 50         <= 50件削除

 ■データ保存形式をメモリとファイルを選択可能
   今までのバージョンでは稼動中はデータは常にKeyとValueの関係でメモリ上に保持されていた。
   登録毎のトランザクション記録ファイルと、定期的なメモリ情報のファイル書き出しで永続性を保っていたが、
   ファイル書き出しモードではKeyのみメモリ上に保持しデータはファイルにレストアすることを実現。
   これによりメモリ情報上での情報を少なくすることが可能であり、テストではJVMオプションで-Xmx256mとした
   DataNodeで400万件以上のデータを格納出来た。
   (※Key値の長さはDataNode格納時は影響がないが、参考に"datasavekey_1"～"datasavekey_4000000"というKey値)
   しかし、今まで制約を設けていなかった格納データ長に制約が出来た。
   現在格納できるValueのサイズは、512byteである。
   これ以上のデータ長を格納する場合は、ImdstKeyValueClientのsetByteValueメソッドを使用することとなる。
   ※512の指定を変更する場合は一度全てのデータファイル(サンプルでは.\keymapfileディレクトリのファイル)を
     全て削除してから、ImdstDefineのsaveDataMaxSizeを変更することで対応可能。
   ※データファイル保存方法は追記型となるので、今後バキュームメソッドを実装予定。
   ※メモリとファイルの切り替えはDataNode.propertiesの
     "KeyManagerJob1.dataMemory=false" <=ファイル
     "KeyManagerJob1.dataMemory=true"  <=メモリ
     で切り替え可能

========================================================================================================
========================================================================================================
[New - MasterNode処理部分を最適化&性能評価のテキストを添付]
[[リリース Ver 0.2.2 - (2010/02/24)]]
 ■MasterNodeのロジックを最適化。
   最適化残箇所はまだ残っている。

 ■最適化前と後で、簡単に性能を測定。測定結果をテキストとして添付
========================================================================================================
========================================================================================================
[New - 不具合修正]
[[リリース Ver 0.2.1 - (2010/02/11)]]
 ■自動リカバー時の挙動を修正。
   停止ノード起動時のタイミングによって正しくデータがリカバリーされない不具合を修正。

 ■src\MasterNode.propertiesにコメントを追加。
========================================================================================================
========================================================================================================
[New - 機能追加]
[[リリース Ver 0.2.0 - (2010/02/08)]]
 ■自動レプリケーション及び、自動リカバリー機能を追加
   データノードクラッシュ時もシステムの機能停止を防止。

 [追加機能詳細]
  1. 1つのデータを複数のデータノードに登録するよう機能を追加(自動レプリケーション)
     分散登録を行うことで、自動的にデータの複製が行われ、より安全性の高い分散KVSへと進化しました。

  2. 自動リカバリー機能
     1.の機能を使用している場合、メインデータノードがクラッシュした場合も、メインデータノード復帰後、
     スレーブノード(レプリケーションノード)から自動的にデータを復元します。
     ※スレーブノードがクラッシュした場合も、復帰後自動的にメインデータノードから復元されます。

  3. 上記2つの機能を使用している場合はノード停止時もシステムの停止なしに使用可能
     データノードクラッシュ時もスレーブノード(レプリケーションノード)への自動移行が行われるため、
     使用システムの停止がありません。

  ※上記の使用方法は、src\MasterNode.propertiesを参照してください。
  ※execMasterNode.batはマスターノードを起動します。
  ※execDataNode.batはメインデータノードを起動します。
  ※execSlaveDataNode.batはスレーブデータノードを起動します。
========================================================================================================


スペック
 実装言語:Java(jdk1.6にて開発)
 ソースエンコーディング:UTF-8
 動作検証OS:WinsowsXp SP3、CentOS 5.3(final)
 必要ライブラリ:log4j-1.2.14.jar、javamail-1.4.1.jar(JavaMail Ver1.4.1)
 Version:0.6.5(2010/05/30)


■機能説明とサンプルの実行方法
[機能説明]
1.Key-Valueストア
  Key-Valueストアを実現します。
  Keyは文字列、Valueは文字列と、byteデータの両方を登録可能です。

2.Tag機能
  Keyの他にTagを登録できます。
  Tagは文字列となります。
  ストアではKeyはユニークな値として扱われますが、Tagは複数のデータに紐付ける
  ことが出来ます。
  複数データにあらかじめ任意のTagを付けることで、Tag指定により
  一度に関連データを取得可能となります。
  ※現在はTag指定で関連するデータのKey配列が取得できます。


4.オンメモリであり、永続化されたデータ
  データの登録をクライアントが命令し、完了するとそのデータは2台のデータノードに登録されます
  登録のされかたは、Key値はメモリ(※1)とファイル(※2)に、Value値はファイル(※3)にのみ登録されます。
  Value値をメモリ(※1,4)にのみ登録することも可能です。
  上記2つ以外にトランザクションログも同時にファイルに登録しています。
  データノードがダウンしても正しく保存されたKey値をファイル情報から復元するか、
  Key値のファイルへの反映は定期的であるため、その間で保存前にダウンしたもしくは破損している場合は、
  トランザクションログから復旧されます。

  ※1.登録データは各データノード上で1つのConcurrentHashMapに格納されます。
      データの登録、取り出しは全てここから行われます。
  ※2.ファイルシステムに保存されるデータは、定期的に保存されるConcurrentHashMapを
      シリアライズ化したデータと、データ登録時のログ情報となります。
      シリアライズデータの登録はデータ登録、取得処理とは非同期にて実行されます。
  ※3.Value値は固定長でLF改行の1ファイルに書き込まれます。
      記録方式は追記型となります。
      Key値はこのValue値の最新の位置を持っています。
  ※4.DataNode.propertiesの"KeyManagerJob1.dataMemory"の値で変更可能
      trueでメモリ保持、falseでファイル保存
      どちらの場合もトランザクションログは保存されるので、不慮のダウンによるデータの復元には影響はありません。
      ※KeyManagerJob1.memoryMode=trueの場合は復元されません


5.分散型
  「okuyama」はマスタノード、データノード、トランザクションノード、クライアントの4つで構成されます。
  それぞれの役目は以下です。
  マスタノード:・設定されたアルゴリズム(※1)に従って、クライアントからのデータ操作依頼を適切な
                 データノードに依頼します。
               ・1つのデータを2台のデータノードにレプリケーションします
                 取得時に該当データノードがダウンしてる場合も、レプリケーション先のデータノードから取得します。
                 また、データノードが2台とも稼動している場合は、処理を分散し負荷分散を行います。
                 データ登録時に1台のデータノードがダウンしている場合ももう一台のノードに保存し処理を続行します。
               ・複数台での冗長化が可能である。
                 複数台で稼動する場合は、MasterNode内でのMainノードを決定する必要がある。
               ・停止なしでの動的なデータノードの追加を実現します。
                 データノードを追加した場合も、それまでに登録したデータへのアクセスは同じように可能です。
               ・常にDataNodeの生死を監視し、ダウンからの復旧時にデータを稼動ノードから自動リカバーさせます。
                 リカバー中はデータの不整合が発生しないように同期化を実現します。
                 ※1.管理するデータノードの数に依存する簡単なアルゴリズムです。
               ・設定ファイルはsrc\MasterNode.properties


  データノード:・複数台での構成が可能
               ・キーとデータの組み合わせでデータを保存します。
                 データの登録、抽出、削除インターフェースを持ちます。
               ・自身では他ノードへのデータの振り分けなどは行ないません。
               ・設定ファイルはsrc\DataNode.properties

  トランザクションノード:・分散Lock(TransactionMode)を使用する場合にLockを保持、管理します。
                           TransactionModeを使用していて、このノードがダウンすると極端にスループットがダウンします。
                           今後改修予定。
                         ・設定ファイルはsrc\TransactionNode.properties

  クライアント:・マスタノードへの通信を行う実際のプログラムインターフェースです。
               ・マスターノードの情報を複数セットすることで自動分散や、マスターノードダウン時の
                 別ノードへの自動再接続をおこないます。
               ・JavaとPHPそれぞれのクラインプログラムがあります。
                 使用方法は以下の項もしくはリリース物のサンプルプログラムTestSock.javaもしくは、
                 etc_clietn\PhpTestSock.phpを参照してください。
                 クライアントのソースファイルは
                 Javaはsrc\org\imdst\client\ImdstKeyValueClient.java
                 PHPはetc_client\OkuyamaClient.class.php

               インターフェースとしては、
               1.setValue(Key値, Value値)                 :[Key(文字列)とValue(文字列)の組み合わせでのデータ登録]
               2.setValue(Key値, Tag値配列 Value値)       :[Key(文字列)とTag(文字列(配列))とValue(文字列)の組み合わせでのデータ登録]
               3.getValue(Key値)                          :[Key(文字列)でのValue(文字列)取得]
               4.getTagKeys(Tag値)                        :[Tag(文字列)でのKey値群(Key値の配列)取得]
               5.setByteValue(Key値, byte値)              :[Key(文字列)とbyte配列の組み合わせでのデータ登録](PHPは未実装)
               6.setByteValue(Key値, Tag値配列 byte値)    :[Key(文字列)とTag(文字列(配列))とbyte配列の組み合わせでのデータ登録](PHPは未実装)
               7.getByteValue(Key値)                      :[Key(文字列)とValue値をByte配列で取得する.setByteValueで登録した値のみ取得できる]
               8.removeValue(Key値)                       :[Key(文字列)でデータを削除]
               9.getValueScript(Key値,JavaScriptコード)   :[Key(文字列)とJavaScriptコードを渡し、取得されたvalue値にJavaScriptを実行し値を返す]
              10.startTransaction()                       :[Transactionモード時のみ。Transactionを開始する(分散Lockが使用可能になる)]
              11.lockData(Key値,Lock時間,Lock取得待ち時間):[Transactionモード時のみ。Key値でLockを行う。Lock時間で指定した時間維持される(0は無制限)、別のクライアントがLockしている場合はLock取得待ち時間の間リトライする]
              12.releaseLockData(Key値)                   :[Transactionモード時のみ。自身の取得したLockを開放する]
              13.setNewValue(Key値, Value値)              :[未登録のKey値の場合のみ登録できる]
              14.setNewValue(Key値, Tag値配列 Value値)    :[未登録のKey値の場合のみ登録できる]

  それぞれのノード間の通信はTCP/IPでの通信となります。
  また、クライアントとマスタノード間の通信は試験的にBase64にてエンコーディングした文字列を使用しています。


[インストール方法]
[起動方法]
 ※Windows環境

   前提条件:1.構成
              1台のマシン上で稼動するようなサンプル設定ファイルが同梱されています。
              それぞれのノード台数
              マスタノード:1台
              データノード:2台(2インスタンス×3(マスター、スレーブ、サード))

            2.各ノードの使用ポートは以下となります。
              マスタノード:8888
              用途:クライアントからの要求待ち受け
              変更する場合:srcディレクトリ配下のMasterNode.propertiesの7行目を変更
                           7行目=MasterManagerJob.Init=8888<=この番号

              データノード:5553、5554　5556、5557
              用途:マスタノードからの要求待ち受け
              変更する場合:メインデータノード
                           srcディレクトリ配下のDataNode.propertiesの7行目、13行目を変更
                           7行目=KeyManagerJob1.Init=5553<=この番号
                           13行目=KeyManagerJob2.Init=5554<=この番号
                           スレーブデータノード
                           srcディレクトリ配下のSlaveDataNode.propertiesの7行目、13行目を変更
                           7行目=KeyManagerJob1.Init=5556<=この番号
                           13行目=KeyManagerJob2.Init=5557<=この番号

 1.コンパイル
   簡易的なコンパイル用バッチファイルを用意しています。
   本ファイルと同一ディレクトリにある、compile.batを実行してください。
   前提:javac.exeにPATHが通っている

 2.MasterNode起動
   簡易的なMasterNode起動用バッチファイルを用意しています。
   本ファイルと同一ディレクトリにある、execMasterNode.batを実行してください。
   設定ファイルはclasses\MasterNode.propertiesを参照しています。
   停止方法はCtrl+Cをプロンプトで実行
   ※ServerStopファイルが存在するとサーバは起動しません。
   ※execMasterNode2.batはスレーブMasterNodeを起動します。
   ※execMasterNodeMemcached.batはスレーブMasterNodeをmemcache互換プロトコルで起動します。
   前提:1.java.exeにPATHが通っている
        2.メモリ上限を128MBとしています

 3.DataNode起動
   簡易的なDataNode起動用バッチファイルを用意しています。
   本ファイルと同一ディレクトリにある、execDataNode.batを実行してください。
   2つのデータノードが同時に起動します。
   設定ファイルはclasses\DataNode.propertiesを参照しています。
   停止方法はCtrl+Cをプロンプトで実行
   ※ServerStopファイルが存在するとサーバは起動しません。
   前提:1.java.exeにPATHが通っている
        2.メモリ上限を256MBとしています

 3.SlaveDataNode起動
   簡易的なスレーブ用SlaveDataNode起動用バッチファイルを用意しています。
   本ファイルと同一ディレクトリにある、execSlaveDataNode.batを実行してください。
   2つのデータノードが同時に起動します。
   設定ファイルはclasses\SlaveDataNode.propertiesを参照しています。
   停止方法はCtrl+Cをプロンプトで実行
   ※ServerStopファイルが存在するとサーバはDataNodeは起動しません。
   前提:1.java.exeにPATHが通っている
        2.メモリ上限を256MBとしています


 3.ThirdDataNode起動
   簡易的なスレーブ用ThirdDataNode起動用バッチファイルを用意しています。
   本ファイルと同一ディレクトリにある、execThirdDataNode.batを実行してください。
   2つのデータノードが同時に起動します。
   設定ファイルはclasses\SlaveDataNode.propertiesを参照しています。
   停止方法はCtrl+Cをプロンプトで実行
   ※ServerStopファイルが存在するとサーバはDataNodeは起動しません。
   前提:1.java.exeにPATHが通っている
        2.メモリ上限を256MBとしています


 4.TransactionNode起動
   簡易的な分散Lock用TransactionNode起動用バッチファイルを用意しています。
   本ファイルと同一ディレクトリにある、execTransactionNode.batを実行してください。
   設定ファイルはclasses\TransactionNode.propertiesを参照しています。
   停止方法はCtrl+Cをプロンプトで実行
   ※ServerStopファイルが存在するとサーバは起動しません。
   前提:1.java.exeにPATHが通っている
        2.メモリ上限を256MBとしています

 ※execMasterNode2.batを実行すると、スレーブMasterNodeが起動します。
   ポート番号は8889を使用します。
   execMasterNodeMemcached.batはポート11211でプロトコルはmemcacheになります。
   設定ファイルはclasses\MasterNode2.propertiesを参照しています。

   ・起動サンプルでの構成図
                ┌──────┐      ┌──────┐
                │ マスター   │      │ スレーブ   │
                │ ノード     │      │ マスター   │
                │ Port:8888  │      │ ノード     │
                │            │      │ Port:8889  │
                └───┬──┘      └───┬──┘
                        │┌─────────┘
            ┌─────┴┴───┐
            │                    │
      ┌───────┐ ┌───────┐
      │┌─────┐│ │┌─────┐│
      ││データ    ││ ││データ    ││
      ││ノード    ││ ││ノード    ││
      ││Port:5553 ││ ││Port:5554 ││
      │└─────┘│ │└─────┘│
      │┌─────┐│ │┌─────┐│
      ││スレーブ  ││ ││スレーブ  ││
      ││データ    ││ ││データ    ││
      ││ノード    ││ ││ノード    ││
      ││Port:5556 ││ ││Port:5557 ││
      │└─────┘│ │└─────┘│
      └───────┘ └───────┘

 4.サンプルの実行方法
   簡易的な接続、登録、取得、削除サンプルを用意しています。
   本ファイルと同一ディレクトリにある、TestSock.classを実行してください(jdk1.6にてコンパイル済み)。
   引数なしで実行すると使用方法が出力されます。
   例)
     # 以下の例は自動的にインクリメントするKey値でValue文字列を1000回登録している
     java -cp ./;./classes;./lib/javamail-1.4.1.jar TestSock 1 127.0.0.1 8888 1000

     # 以下の例はキー値をkey_aでバリュー値value_bを登録
     java -cp ./;./classes;./lib/javamail-1.4.1.jar TestSock 1.1 127.0.0.1 key_a value_b

     # 以下の例は自動的にインクリメントするKey値でValue文字列を1000回取得している
     java -cp ./;./classes;./lib/javamail-1.4.1.jar TestSock 2 127.0.0.1 8888 1000

     # 以下の例はキー値をkey_aでvalueを取得
     java -cp ./;./classes;./lib/javamail-1.4.1.jar TestSock 2.1 127.0.0.1 8888 key_a

     # 以下の例はマスターノードIP127.0.0.1,ポート8888とIP127.0.0.1,ポート8889に接続をバランシングして
     # 自動的にインクリメントするKey値でValue文字列を1000回取得している
     # execMasterNode2.batを起動していると、元のexecMasterNode.batのプロセスを終了しても正しく稼動し続ける
     java -cp ./;./classes;./lib/javamail-1.4.1.jar TestSock 2.2 "127.0.0.1:8888,127.0.0.1:8889" 100

     # 以下の例はキー値をkey_aで取得したValue値にJavaScriptを実行し結果を取得
     java -cp ./;./classes;./lib/javamail-1.4.1.jar TestSock 2.3 127.0.0.1 8888 key_a "var dataValue; var retValue = dataValue.replace('b', 'scritpChangeRet'); var execRet = '1';"

     # 以下の例は自動的にインクリメントするKey値と適当な4パターンのTag値でValue文字列を100回登録している
     java -cp ./;./classes;./lib/javamail-1.4.1.jar TestSock 3 127.0.0.1 8888 100

     # 以下の例はTag値「tag1」に紐付くKey値とValue値を1回取得している
     java -cp ./;./classes;./lib/javamail-1.4.1.jar TestSock 4 127.0.0.1 8888 1 tag1

     # 以下の例はKey値「wordfile」で「C:\temp\SampleWord.doc」ファイルを1回登録している
     java -cp ./;./classes;./lib/javamail-1.4.1.jar TestSock 5 127.0.0.1 8888 1 C:\temp\SampleWord.doc wordfile

     # 以下の例はKey値「wordfile」のバイトデータを取得し「C:\SampleWord.doc」ファイルとして1回作成している
     java -cp ./;./classes;./lib/javamail-1.4.1.jar TestSock 6 127.0.0.1 8888 1 C:\SampleWord.doc wordfile

     # 以下の例はKey値「key_a」のデータを削除して、Valueを取得している
     java -cp ./;./classes;./lib/javamail-1.4.1.jar TestSock 8 127.0.0.1 8888 key_a

     # 以下の例はTransactionを開始してデータをLock後、データを更新、取得し、Lockを解除
     java -cp ./;./classes;./lib/javamail-1.4.1.jar TestSock 10 127.0.0.1 8888 key_a 5 10

     # 以下の例は1度だけデータを登録する場合に使用する呼び出し
     # "key_abc"というKeyは1度しか登録しないようにしたい場合
     # 2度実行すると2回目はエラーとなる。(memcacheのaddに相当する)
     java -cp ./;./classes;./lib/javamail-1.4.1.jar TestSock 11 127.0.0.1 8888 key_abc value_abc

     PHPに関しては、etc_client\PhpAutoTest.batを参照してください。

[今後]
 今後はバグFixと分散トランザクション(ロック機構)を実現していきます。

