■概要
1.hadoopとの連携はフォーマットでのInput連携となっています。
   構成としては以下となります。
   hadoop ==>   org.okuyama.hadoop.format.OkuyamaInputFormat ==>  org.okuyama.hadoop.format.OkuyamaRecordReader ==> hadoop(Map)
2.1つのTagに紐付くデータが1つのMapに割り当てられます
3.設定情報を登録するOkuyamaHadoopFormatConfigureというクラスが存在しそこに設定情報を登録し実行
4.利用出来るデータは現在Tagに紐付くTextデータのみ。HadoopのTextInputFormatとほぼ同様

■利用方法
1.hadoopがシングルモードで動くところまで準備してください。
2.org.okuyama.hadoop.test.WordCountがサンプルプログラムです。
    以下抜粋にて説明
        OkuyamaHadoopFormatConfigure.init();               <== OkuyamaInputFormatが利用する情報を設定するクラスを初期化。再利用する場合は都度初期化が必要
        String[] masterNodes = {"127.0.0.1:8888"};      <== データを参照するokuyamaの接続情報を代入(複数指定の場合は、ノード数分配列の後端に追加)
        OkuyamaHadoopFormatConfigure.setMasterNodeInfoList(masterNodes);    <== MasterNodeの情報を登録
        OkuyamaHadoopFormatConfigure.addTag("tag1");    <==  処理対象とするTagを全て登録
        OkuyamaHadoopFormatConfigure.addTag("tag2");    <== 同様
        OkuyamaHadoopFormatConfigure.addTag("tag3");    <== 同様
        OkuyamaHadoopFormatConfigure.addTag("tag4");    <== 同様

        job.setInputFormatClass(OkuyamaInputFormat.class);  <== HadoopにInputFormatクラスを指定
3.残りの箇所は通常のHadoopプログラムとして実行してください。
