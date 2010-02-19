package org.imdst.util;

/**
 * 定数をまとめる.<br>
 *
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class ImdstDefine {


    /* -- KeyMapファイルに関係する定数 ----------                        */
    // KeyNodeのWorkファイルでのセパレータ
    //public static final String keyWorkFileSep = "#imdst7386#";
    public static final String keyWorkFileSep = ",";

    // KeyNodeのWorkファイルの文字コード
    public static final String keyWorkFileEncoding = "UTF-8";

    // KeyNodeのWorkKeyファイルの終点文字列
    //public static final String keyWorkFileEndPointStr = "#imdstEndWFP8574";
    public static final String keyWorkFileEndPointStr = ";";


    /* --  クライアントとの転送内容に使用する定数 ----------              */
    // クライアントとの文字コード
    public static final String keyHelperClientParamEncoding = "UTF-8";

    // クライアントからのリクエスト文字列のセパレータ
    public static final String keyHelperClientParamSep = "#imdst3674#";

    // クラインととの連携文字列でのブランクを表す代行文字列
    public static final String imdstBlankStrData = "(imdst5397)";

    // Tag値の文字列の前方カッコ
    public static final String imdstTagStartStr = "{imdst_tag#9641";

    // Tag値の文字列の後方カッコ
    public static final String imdstTagEndStr = "1469#tag_imdst}";

    // Tag値でキーを登録する際にKey値を連結するのでその際のセパレータ文字列
    //public static final String imdstTagKeyAppendSep = "#imdst8417#";
    public static final String imdstTagKeyAppendSep = ":";

    // データノードに対するKeyデータ登録時に成功した場合の返却文字列の先頭部分
    public static final String keyNodeKeyRegistSuccessStr = "1#imdst3674#true";

    // データノードに対するTagデータ登録時に成功した場合の返却文字列の先頭部分
    public static final String keyNodeTagRegistSuccessStr = "3#imdst3674#true";

    /* --  通信時の固定文字列系定数  ----------                           */
    // クラインが接続を切断する際に通知する文字列
    public static final String imdstConnectExitRequest = "(&imdst9999&)";

    // 全てのKeyMapObjectファイルをKey=Valueの形式で接続した場合のデータ区切り文字
    public static final String imdstConnectAllDataSendDataSep = ";";

    /* --  設定ファイルの固定文字列系定数  ----------                     */
    public static final String Prop_KeyMapNodesRule = "KeyMapNodesRule";
    public static final String Prop_KeyMapNodesInfo = "KeyMapNodesInfo";
    public static final String Prop_SubKeyMapNodesInfo = "SubKeyMapNodesInfo";

    public static final String dataNodeParamKey_1 = "dataNodeNameList";
    public static final String dataNodeParamKey_2 = "dataNodePortList";
    public static final String dataNodeParamKey_3 = "dataSubNodeNameList";
    public static final String dataNodeParamKey_4 = "dataSubNodePortList";
    public static final String dataNodeParamKey_5 = "keyMapNodeInfo";


	/* -- ここからプログラム内固定文字列系(Mapのキーとか)  ----------      */
    public static final String keyNodeSocketKey = "socket";
    public static final String keyNodeStreamWriterKey = "stream_writer";
    public static final String keyNodeStreamReaderKey = "stream_reader";
    public static final String keyNodeWriterKey = "writer";
    public static final String keyNodeReaderKey = "reader";
    public static final String keyNodeConnectionMapKey = "map";
    public static final String keyNodeConnectionMapTime = "time";

}