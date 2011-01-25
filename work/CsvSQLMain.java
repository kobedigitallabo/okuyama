import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;

//import org.csvdb.lang.CsvDbException;
//import org.csvdb.io.DataDefineConfig;

public class CsvSQLMain {

    public static void main(String[] args) {
        CsvSQLMain me = new CsvSQLMain();
        me.exec(args);
    }

    public void exec(String[] args) {
        try {
            String[] work = args[0].split(";");

            StatementCompileData statementCompileData = new StatementCompileData(work[0]);
            statementCompileData.compile();
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

private class StatementCompileData {

    // ステートメントタイプ番号（SELECT)
    private static final int ST_TYPE_SELECT = 1;
    // ステートメントタイプ番号（ERROR)
    private static final int ST_TYPE_ERR = 2;

    // ステートメントタイプの判断用
    private static final String ST_TYPE_SELECT_STR = "select";

    // ステートメント分解用のデリミタ
    private static final String ST_TOKEN_DELIM = " ";


    // 条件指定の「=」
    public static final String WHERE_TYPE_EQUAL = "1";

    // 条件指定の「<>」
    public static final String WHERE_TYPE_NOT_EQUAL = "2";

    // 条件指定の「like」
    public static final String WHERE_TYPE_LIKE = "3";

    // 条件指定の「<」
    public static final String WHERE_TYPE_RIGHT_BIG = "4";

    // 条件指定の「>」
    public static final String WHERE_TYPE_LEFT_BIG = "5";

    // 条件指定の「in」
    public static final String WHERE_TYPE_IN = "6";


    // WHERE句の両辺の型「文字列」
    public static final String WHERE_VALUE_TYPE_STRING = "STRING";

    // WHERE句の両辺の型「数値」
    public static final String WHERE_VALUE_TYPE_NUMBER = "NUMBER";

    // WHERE句の両辺の型「テーブル」
    public static final String WHERE_VALUE_TYPE_TABLE = "TABLE";


    // コンパイル中のエラー有無を格納
    private boolean compErrFlg;

    // コンパイル中のエラー文字列
    private String compErrMsg;


    // 自身が持つステートメント文字列
    private String statement;

    // 自身がコンパイルしているステートメントのタイプ 1=SELECT 2=UPDATE 3=INSERT 4=DELETE -1=エラー
    public static final int ST_TYPE_NUMBER_SELECT = 1;
    public static final int ST_TYPE_NUMBER_UPDATE = 2;
    public static final int ST_TYPE_NUMBER_INSERT = 3;
    public static final int ST_TYPE_NUMBER_DELETE = 4;
    public static final int ST_TYPE_NUMBER_ERROR = -1;
    private int stTypeNumber = ST_TYPE_NUMBER_ERROR;


    /**
     * SELECT句のカラムリスト.<br>
     * SELECT句のカラムの詳細情報を格納.<br>
     * 内容はHashMap<br>
     * ArrayList{(0) HashMap{"COLUMN_NAME", "COL_A"},<br>
     *           (1) HashMap{"COLUMN_NAME", "COL_B"}<br>
     *          }<br>
     */
    private ArrayList selectColsDtList;


    /**
     * FROM句のテーブルリスト.<br>
     * FROM句のテーブルの詳細情報を格納.<br>
     * 内容はHashMap<br>
     * ArrayList{(0) HashMap{"TABLE_NAME", "TABLE_A"},<br>
     *           (1) HashMap{"TABLE_NAME", "TABLE_B"}<br>
     *          }<br>
     *
     */
    private ArrayList fromTablesDtList;

    /**
     * WHERE句の詳細リスト(内容はOR句出てくるまでを一つの集合として、<br>
     * ArrayListにして格納.<br>
     * 内部のArrayListの中身はHashMap<br>
     * ArrayList{(0) ArrayList{HashMap{["LEFT", "COLUMN_A"],<br>
     *                                 ["LEFT_TYPE", "COLUMN"], <br>
     *                                 ["RIGHT", "'A'"], <br>
     *                                 ["RIGHT_TYPE", "STRING"], <br>
     *                                 ["TYPE", "="]<br>
     *                                },<br>
     *                        },<br>
     *                        {HashMap{["LEFT", "COLUMN_B"],<br>
     *                                 ["LEFT_TYPE", "COLUMN"], <br>
     *                                 ["RIGHT", "1"], <br>
     *                                 ["RIGHT_TYPE", "NUMBER"], <br>
     *                                 ["TYPE", "<>"]<br>
     *                                },<br>
     *                        },<br>
     *           (1) ArrayList{HashMap{["LEFT", "COLUMN_A"],<br>
     *                                 ["LEFT_TYPE", "COLUMN"], <br>
     *                                 ["RIGHT", "'B'"], <br>
     *                                 ["RIGHT_TYPE", "STRING"], <br>
     *                                 ["TYPE", "="]<br>
     *                                },<br>
     *                        },<br>
     *                        {HashMap{["LEFT", "COLUMN_B"],<br>
     *                                 ["LEFT_TYPE", "COLUMN"], <br>
     *                                 ["RIGHT", "2"], <br>
     *                                 ["RIGHT_TYPE", "NUMBER"], <br>
     *                                 ["TYPE", "<>"]<br>
     *                                },<br>
     *                        }<br>
     */ 
    private ArrayList whereDtList;


    /**
     * オプションコマンドを格納.<br>
     * HashMap{["OPTION_CMD", "OUT"],<br>
     *         ["OPTION_STR", "STR"]<br>
     *        }<br>
     *
     * OPTION_CMDにオプション指定文字列を格納<br>
     * 現在サポートしているのは下記<br>
     * "OUT" => 結果をファイルに出力<br>
     *
     * OPTION_STRにオプション引数文字列を格納<br>
     * 現在サポートしているのは下記<br>
     * "OUT"に対する引数文字列 => /etc/CSV_RET.csv<br>
     *
     */
    private HashMap optionCmdMap;


    /**
     * コストラクタ
     * @param statement 問い合わせ文字列
     */
    public StatementCompileData(String statement) {
        this.statement = statement;
        this.compErrFlg = false;
        this.compErrMsg = null;
        this.selectColsDtList = new ArrayList();
        this.fromTablesDtList = new ArrayList();
        this.whereDtList = new ArrayList();
        this.optionCmdMap = new HashMap();
    }

    /**
     * Statementをコンパイルする.<br>
     *
     */
    public void compile() throws Exception {
        String targetStr = null;
        try {
            switch (this.checkStatementType()) {
                case ST_TYPE_SELECT:
                    // SELECTステートメント

                    // SELECT句分解
                    targetStr = this.parseSelectColumn();

                    if (this.compErrFlg) break;

                    // FROM句分解
                    targetStr = this.parseFrom(targetStr);

                    if (this.compErrFlg) break;

                    // WHERE句分解
                    targetStr = this.parseWhere(targetStr);
                    if (this.compErrFlg) break;

                    // 仮チェック
                    //this.checkAll();
                    //if (this.compErrFlg) break;

                    this.stTypeNumber = ST_TYPE_NUMBER_SELECT;
                    break;
                case ST_TYPE_ERR:
                    // それ以外 今のところなし
                    this.compErrFlg = true;
                    this.compErrMsg = "対応していないステートメントです =[" + this.statement + "]";
                    this.stTypeNumber = ST_TYPE_NUMBER_ERROR;
                break;
            }
            
        
        } catch (Exception e) {
            throw e;
        }
        System.out.println("selectColsDtList = [" + selectColsDtList + "]");
        System.out.println("fromTablesDtList = [" + fromTablesDtList + "]");
        System.out.println("whereDtList = [" + whereDtList + "]");
    }

    public ArrayList getSelectColsDtList() {
        return this.selectColsDtList;
    }

    public ArrayList getFromTablesDtList() {
        return this.fromTablesDtList;
    }

    public ArrayList getWhereDtList() {
        return this.whereDtList;
    }

    /**
     * ステートメントのタイプを調べる.<br>
     * 1=SELECT<br>
     * 2=それ以外 いまのところなし<br>
     * @return int 1=SELECT 2=それ以外 いまのところなし
     */
    public int checkStatementType() throws Exception {

        int ret = ST_TYPE_ERR;
        String firstToken = null;

        // トリムする
        String workSt = this.statement.trim();

        // トークン初期化
        StringTokenizer token = new StringTokenizer(workSt, ST_TOKEN_DELIM);

        try {
            if (token.hasMoreTokens()) {
                firstToken = token.nextToken(ST_TOKEN_DELIM);
            }

            if (firstToken != null && firstToken.trim().toLowerCase().equals(ST_TYPE_SELECT_STR)) {
                ret = ST_TYPE_SELECT;
            }
        } catch (Exception e) {
            throw e;
        }
        return ret;
    }


    /**
     * SELECT句を分解する.<br>
     * ","で分解して一区切りにする.<br>
     * 但し、"'"もしくは"("が出てきた場合は次の"'"もしくは")"が出てくるまでを一つの区切りにする.<br>
     * 本メソットは解析後の残り文字列を返す.<br>
     *
     */
    private String parseSelectColumn() {
        HashMap colsDtMap = null;
        ArrayList addStrList = new ArrayList(50);
        boolean singleQuotationFlg = false;
        boolean bracketsFlg = false;
        int bracketsFlgCount = 0;
        String workStr = "";

        String targetSt = null;
        char[] selectChars = null;
        String[] fromSplitList = null;

        // 「select」を削除
        targetSt = this.deleteSelectStr(this.statement);

        if (targetSt.trim().length() == 0) {
            // SELECT句が存在しない
            this.compErrFlg = true;
            this.compErrMsg = "SELECT句が存在しません:" + this.statement;
            return null;
        } 

        // 「from」で分解
        fromSplitList = this.splitSqlFrom(targetSt);

        // FROM句以前格納
        targetSt = fromSplitList[0];

        // SELECT句の最後が","で終わっている場合用に一文字プラスする
        // ループ時に最後まで動かす為
        targetSt = targetSt + " ";
        selectChars = targetSt.toCharArray();

        // 「 , 」が出てくるまで移動
        for(int charIndex = 0;charIndex < selectChars.length;charIndex++){
            // 現在「'」の中か判断
            if(singleQuotationFlg == false && bracketsFlg == false){
                if(charIndex + 1 < selectChars.length){
                    if(selectChars[charIndex] == ','){
                        // 「,」なのでここまでの文字列を格納
                        addStrList.add(workStr);
                        workStr = "";
                        charIndex = charIndex + 1;
                    }
                }
            }

            if(selectChars[charIndex] == '\''){
                if(singleQuotationFlg == false){
                    singleQuotationFlg = true;
                }else{
                    singleQuotationFlg = false;
                }
            }

            if(singleQuotationFlg == false){
                if(selectChars[charIndex] == '('){
                    bracketsFlgCount++;
                    bracketsFlg = true;
                }

                if(selectChars[charIndex] == ')'){
                    bracketsFlgCount--;
                    if(bracketsFlgCount == 0)  bracketsFlg = false;
                }
            }

            workStr = workStr + String.valueOf(selectChars[charIndex]);
        }

        addStrList.add(workStr);

        for(int i = 0;i < addStrList.size();i++){
            colsDtMap = new HashMap();

            // 今のところカラム名のみ格納
            // 必要ならここで再度解析してもいいかも
            colsDtMap.put("COLUMN_NAME", ((String)addStrList.get(i)).trim());

            if (((String)addStrList.get(i)).trim().equals("")) {
                // SELECT句に不正文字列
                this.compErrFlg = true;
                this.compErrMsg = "SELECT句が不正です。:" + this.statement;
                return null;
            }

            selectColsDtList.add(colsDtMap);
        }

        // "'"や"("が終了せずに終わってないかチェック
        if (singleQuotationFlg == true || bracketsFlg == true) {
                this.compErrFlg = true;
                this.compErrMsg = "ステートメントが正しく終了していません。:" + this.statement;
        }

        return fromSplitList[1];
    }


    /**
     * FROM句をテーブル単位で分解する.<br>
     * ","で分解して一区切りにする.<br>
     * 但し、"'"もしくは"("が出てきた場合は次の"'"もしくは")"が出てくるまでを一つの区切りにする.<br>
     * 本メソットは解析後の残り文字列を返す.<br>
     * 
     */
    private String parseFrom(String targetSt) {
        HashMap tableDtMap = null;
        ArrayList addStrList = new ArrayList(50);
        boolean singleQuotationFlg = false;
        boolean bracketsFlg = false;
        int bracketsFlgCount = 0;
        String workStr = "";

        char[] fromChars = null;
        String[] whereSplitList = null;

        // FROM句存在確認
        if (targetSt == null || targetSt.trim().length() == 0) {
            // FROM句が存在しない
            this.compErrFlg = true;
            this.compErrMsg = "FROM句が存在しません:" + this.statement;
            return null;
        } 

        // 「where」で分解
        whereSplitList = this.splitSqlWhere(targetSt);

        // FROM句以前格納
        targetSt = whereSplitList[0];

        // FROM句の最後が","で終わっている場合用に一文字プラスする
        // ループ時に最後まで動かす為
        targetSt = targetSt + " ";
        fromChars = targetSt.toCharArray();

        // 「 , 」が出てくるまで移動
        for(int charIndex = 0;charIndex < fromChars.length;charIndex++){
            // 現在「'」の中か判断
            if(singleQuotationFlg == false && bracketsFlg == false){
                if(charIndex + 1 < fromChars.length){
                    if(fromChars[charIndex] == ','){
                        // 「,」なのでここまでの文字列を格納
                        addStrList.add(workStr);
                        workStr = "";
                        charIndex = charIndex + 1;
                    }
                }
            }

            if(fromChars[charIndex] == '\''){
                if(singleQuotationFlg == false){
                    singleQuotationFlg = true;
                }else{
                    singleQuotationFlg = false;
                }
            }

            if(singleQuotationFlg == false){
                if(fromChars[charIndex] == '('){
                    bracketsFlgCount++;
                    bracketsFlg = true;
                }

                if(fromChars[charIndex] == ')'){
                    bracketsFlgCount--;
                    if(bracketsFlgCount == 0)  bracketsFlg = false;
                }
            }

            workStr = workStr + String.valueOf(fromChars[charIndex]);
        }

        addStrList.add(workStr);

        for(int i = 0;i < addStrList.size();i++){
            tableDtMap = new HashMap();

            // 今のところテーブル名のみ格納
            // 必要ならここで再度解析してもいいかも
            tableDtMap.put("TABLE_NAME", ((String)addStrList.get(i)).trim());

            if (((String)addStrList.get(i)).trim().equals("")) {
                // FROM句に不正文字列
                this.compErrFlg = true;
                this.compErrMsg = "FROM句が不正です。:" + this.statement;
                return null;
            }

            this.fromTablesDtList.add(tableDtMap);
        }

        // "'"や"("が終了せずに終わってないかチェック
        if (singleQuotationFlg == true || bracketsFlg == true) {
            this.compErrFlg = true;
            this.compErrMsg = "ステートメントが正しく終了していません。:" + this.statement;
            return null;
        }

        return whereSplitList[1];
    }


    /**
     * where句を分析
     *
     */
    public String parseWhere(String targetSt) {
        HashMap whereDtMap = null;
        ArrayList whereAndList = null;

        String[] orSplitList = null;
        String[] andSplitList = null;
        String[] groupBySplitList = null;

        // WHERE句以降存在確認
        if (targetSt == null || targetSt.trim().length() == 0) {
           // WHERE句以降が存在しない
           return null;
        } 

        // 「order by」で分解 
        // TODO:後で実装
        //groupBySplitList = this.splitSqlOrderBy(targetSt);
        groupBySplitList = new String[2];

        // FROM句以前格納
        // TODO:後で実装
        //targetSt = groupBySplitList[0];
        // TODO:仮
        targetSt = targetSt;

        // OR句で分解
        orSplitList = this.splitSqlOr(targetSt);

        // OR句単位で分析
        for (int orIndex = 0; orIndex < orSplitList.length; orIndex++) {

            // 一つのAND句のまとまりリスト
            whereAndList = new ArrayList();

            // AND句で分解
            andSplitList = this.splitSqlAnd(orSplitList[orIndex]);

            // AND単位で分析
            for (int andIndex = 0; andIndex < andSplitList.length; andIndex++) {
                // 一つの条件句を分析
                whereDtMap = this.parseWhereOneDt(andSplitList[andIndex]);
                // nullだった場合分析失敗
                if (whereDtMap == null) break;
                whereAndList.add(whereDtMap);
            }
            this.whereDtList.add(whereAndList);
        }

        return groupBySplitList[1];
    }


    /**
     * SQL文のWhere句一つの条件を解析する
     * RIGHT = 右辺の項目を格納
     * RIGHT_TYPE = 右辺の項目のタイプ(STRING, NUMBER, COLUMN)
     * LEFT = 左辺の項目を格納
     * LEFT_TYPE = 左辺の項目のタイプ(STRING, NUMBER, COLUMN)
     * TYPE = 条件
     *
     * 例) 「table1.Acolumn = '2'」
     *                               ↓
     *      [RIGHT='2', RIGHT_TYPE='STRING', LEFT='table1.Acolumn', LEFT_TYPE='COLUMN', TYPE='='}
     *
     * @param oneStr 文字列
     * @return HashMap 解析後のマップ
     */
    private HashMap parseWhereOneDt(String str) {
        boolean singleQuotationFlg = false;
        boolean bracketsFlg = false;
        int bracketsFlgCount = 0;
        String[] retList;
        str = str.trim();
        char[] lineChars = str.toCharArray();
        String type = "";
        StringBuffer workStrBuf = new StringBuffer();
        StringBuffer rightStrBuf = null;
        String leftStr = null;

        HashMap retMap = new HashMap();

        // 「 = 」が出てくるまで移動
        if(str.indexOf("=") != -1){
            for(int charIndex = 0;charIndex < lineChars.length;charIndex++){
                // 現在「'」の中か判断
                if(singleQuotationFlg == false && bracketsFlg == false){
                    if(lineChars[charIndex] == '='){
                        // 「=」条件が出てきたのでここまでの文字列を連結
                        leftStr = workStrBuf.toString();
                        rightStrBuf = new StringBuffer();
                        for(int lastIndex = charIndex + 1;lastIndex < lineChars.length;lastIndex++){
                            rightStrBuf.append(lineChars[lastIndex]);
                        }
                        type = WHERE_TYPE_EQUAL;
                        break;
                    }
                }

                if(lineChars[charIndex] == '\''){
                    if(singleQuotationFlg == false){
                        singleQuotationFlg = true;
                    }else{
                        singleQuotationFlg = false;
                    }
                }

                if(singleQuotationFlg == false){
                    if(lineChars[charIndex] == '('){
                        bracketsFlgCount++;
                        bracketsFlg = true;
                    }

                    if(lineChars[charIndex] == ')'){
                        bracketsFlgCount--;
                        if(bracketsFlgCount == 0)  bracketsFlg = false;
                    }
                }

                workStrBuf.append(String.valueOf(lineChars[charIndex]));
            }
        }

        // 「 <> 」が出てくるまで移動
        if(type.equals("")){
            workStrBuf = new StringBuffer();
            if(str.indexOf("<>") != -1){
                for(int charIndex = 0;charIndex < lineChars.length;charIndex++){
                // 現在「'」の中か判断
                if(singleQuotationFlg == false && bracketsFlg == false){

                    if(lineChars[charIndex] == '<' && 
                        lineChars[charIndex + 1] == '>'){
                        // 「<>」条件が出てきたのでここまでの文字列を連結
                        leftStr = workStrBuf.toString();
                        rightStrBuf = new StringBuffer();
                        for(int lastIndex = charIndex + 2;lastIndex < lineChars.length;lastIndex++){
                            rightStrBuf.append(lineChars[lastIndex]);

                        }
                        type = WHERE_TYPE_NOT_EQUAL;
                        break;
                    }
                }

                if(lineChars[charIndex] == '\''){
                    if(singleQuotationFlg == false){
                        singleQuotationFlg = true;
                    }else{
                        singleQuotationFlg = false;
                    }
                }

                if(singleQuotationFlg == false){
                    if(lineChars[charIndex] == '('){
                        bracketsFlgCount++;
                        bracketsFlg = true;
                    }

                    if(lineChars[charIndex] == ')'){
                        bracketsFlgCount--;
                        if(bracketsFlgCount == 0)  bracketsFlg = false;
                    }
                }

                workStrBuf.append(lineChars[charIndex]);
                }
            }
        }


        // 「 > 」が出てくるまで移動
        if(type.equals("")){
            workStrBuf = new StringBuffer();
            if(str.indexOf(">") != -1){
                for(int charIndex = 0;charIndex < lineChars.length;charIndex++){
                // 現在「'」の中か判断
                    if(singleQuotationFlg == false && bracketsFlg == false){
                        if(lineChars[charIndex] == '>'){
                            // 「>」条件が出てきたのでここまでの文字列を連結
                            leftStr = workStrBuf.toString();
                            rightStrBuf = new StringBuffer();
                            for(int lastIndex = charIndex + 1;lastIndex < lineChars.length;lastIndex++){
                                rightStrBuf.append(lineChars[lastIndex]);
                            }
                            type = WHERE_TYPE_LEFT_BIG;
                            break;
                        }
                    }

                    if(lineChars[charIndex] == '\''){
                        if(singleQuotationFlg == false){
                            singleQuotationFlg = true;
                        }else{
                            singleQuotationFlg = false;
                        }
                    }

                    if(singleQuotationFlg == false){
                        if(lineChars[charIndex] == '('){
                            bracketsFlgCount++;
                            bracketsFlg = true;
                        }

                        if(lineChars[charIndex] == ')'){
                            bracketsFlgCount--;
                            if(bracketsFlgCount == 0)  bracketsFlg = false;
                        }
                    }

                    workStrBuf.append(lineChars[charIndex]);
                }
            }
        }


        // 「 < 」が出てくるまで移動
        if(type.equals("")){
            workStrBuf = new StringBuffer();
            if(str.indexOf("<") != -1){
                for(int charIndex = 0;charIndex < lineChars.length;charIndex++){
                    // 現在「'」の中か判断
                    if(singleQuotationFlg == false && bracketsFlg == false){
                        if(lineChars[charIndex] == '<'){
                            // 「<」条件が出てきたのでここまでの文字列を連結
                            leftStr = workStrBuf.toString();
                            rightStrBuf = new StringBuffer();
                            for(int lastIndex = charIndex + 1;lastIndex < lineChars.length;lastIndex++){
                                rightStrBuf.append(lineChars[lastIndex]);
                            }
                            type = WHERE_TYPE_RIGHT_BIG;
                            break;
                        }
                    }

                    if(lineChars[charIndex] == '\''){
                        if(singleQuotationFlg == false){
                            singleQuotationFlg = true;
                        }else{
                            singleQuotationFlg = false;
                        }
                    }

                    if(singleQuotationFlg == false){
                        if(lineChars[charIndex] == '('){
                            bracketsFlgCount++;
                            bracketsFlg = true;
                        }

                        if(lineChars[charIndex] == ')'){
                            bracketsFlgCount--;
                            if(bracketsFlgCount == 0)  bracketsFlg = false;
                        }
                    }

                    workStrBuf.append(lineChars[charIndex]);
                }
            }
        }


        // 「 like 」が出てくるまで移動
        if(type.equals("")){
            workStrBuf = new StringBuffer();
            if(str.toLowerCase().indexOf("like") != -1){
                for(int charIndex = 0;charIndex < lineChars.length;charIndex++){
                    // 現在「'」の中か判断
                    if(singleQuotationFlg == false && bracketsFlg == false){

                        if(lineChars[charIndex] == ' ' && 
                            (lineChars[charIndex + 1] == 'l' || lineChars[charIndex + 1] == 'L') && 
                            (lineChars[charIndex + 2] == 'i' || lineChars[charIndex + 2] == 'I') && 
                            (lineChars[charIndex + 3] == 'k' || lineChars[charIndex + 3] == 'K') && 
                            (lineChars[charIndex + 4] == 'e' || lineChars[charIndex + 4] == 'E') && 
                            (lineChars[charIndex + 5] == ' ' || lineChars[charIndex + 5] == '\'')){
                            // 「like」条件が出てきたのでここまでの文字列を連結
                            leftStr = workStrBuf.toString();
                            rightStrBuf = new StringBuffer();

                            for(int lastIndex = charIndex + 6;lastIndex < lineChars.length;lastIndex++){
                                rightStrBuf.append(lineChars[lastIndex]);
                            }
                            type = WHERE_TYPE_LIKE;
                            break;
                        }
                    }

                    if(lineChars[charIndex] == '\''){
                        if(singleQuotationFlg == false){
                            singleQuotationFlg = true;
                        }else{
                            singleQuotationFlg = false;
                        }
                    }

                    if(singleQuotationFlg == false){
                        if(lineChars[charIndex] == '('){
                            bracketsFlgCount++;
                            bracketsFlg = true;
                        }

                        if(lineChars[charIndex] == ')'){
                            bracketsFlgCount--;
                            if(bracketsFlgCount == 0)  bracketsFlg = false;
                        }
                    }

                    workStrBuf.append(lineChars[charIndex]);
                }
            }
        }

        // 「 in 」が出てくるまで移動
        if(type.equals("")){
            workStrBuf = new StringBuffer();
            if(str.toLowerCase().indexOf("in") != -1){
                for(int charIndex = 0;charIndex < lineChars.length;charIndex++){
                // 現在「'」の中か判断
                if(singleQuotationFlg == false && bracketsFlg == false){

                    if(lineChars[charIndex] == ' ' && 
                       (lineChars[charIndex + 1] == 'i' || lineChars[charIndex + 1] == 'I') && 
                       (lineChars[charIndex + 2] == 'n' || lineChars[charIndex + 2] == 'N')){
                        // 「in」条件が出てきたのでここまでの文字列を連結
                        leftStr = workStrBuf.toString();
                        rightStrBuf = new StringBuffer();

                        for(int lastIndex = charIndex + 3;lastIndex < lineChars.length;lastIndex++){
                            rightStrBuf.append(lineChars[lastIndex]);
                        }

                        rightStrBuf = new StringBuffer(rightStrBuf.toString().trim());

                        lineChars = rightStrBuf.toString().toCharArray();

                        // in句が正しく指定されているかをチェック
                        if(!(lineChars[0] == '(' && lineChars[lineChars.length - 1] == ')')) break;

                        type = WHERE_TYPE_IN;
                        break;
                    }
                }

                if(lineChars[charIndex] == '\''){
                    if(singleQuotationFlg == false){
                        singleQuotationFlg = true;
                    }else{
                        singleQuotationFlg = false;
                    }
                }

                if(singleQuotationFlg == false){
                    if(lineChars[charIndex] == '('){
                        bracketsFlgCount++;
                        bracketsFlg = true;
                    }

                    if(lineChars[charIndex] == ')'){
                        bracketsFlgCount--;
                        if(bracketsFlgCount == 0)  bracketsFlg = false;
                    }
                }

                workStrBuf.append(lineChars[charIndex]);
                }
            }
        }

        // 解析に失敗した場合はエラー発行
        if(type.equals("") || 
           (leftStr == null || leftStr.trim().equals("")) || 
           (rightStrBuf == null || rightStrBuf.toString().trim().equals(""))){
            this.compErrFlg = true;
            this.compErrMsg = "WHERE句が不正です:" + str;
            return null;
        }

        // 作成した情報を格納
        retMap.put("TYPE",type);

        retMap.put("LEFT",leftStr.trim());
        // 左辺の型を調べる
        retMap.put("LEFT_TYPE", this.checkStrType(leftStr.trim()));

        retMap.put("RIGHT", rightStrBuf.toString().trim());
        // where句が"in"では無い場合右辺の型を調べる
        if (!type.equals(WHERE_TYPE_IN)) {
            retMap.put("RIGHT_TYPE", this.checkStrType(rightStrBuf.toString().trim()));
        }

        // "'"や"("が終了せずに終わってないかチェック
        if (singleQuotationFlg == true || bracketsFlg == true) {
            this.compErrFlg = true;
            this.compErrMsg = "ステートメントが正しく終了していません。:" + this.statement;
        }

        return retMap;
    }


    /**
     * 仮チェックロジック
     *
     */
    /*private void checkAll() {
        HashMap selectDt = null;
        HashMap fromDt = null;
        ArrayList whereDtList = null;
        HashMap whereDt = null;

        // SELECT句チェック
        // カラム数チェック
        if (this.selectColsDtList.size() < 1) {
            this.compErrFlg = true;
            this.compErrMsg = "SELECT句には最低1つはカラム指定が必要です:" + this.statement;
            return;
        }
        // カラム存在チェック
        for (int i = 0; i < this.selectColsDtList.size(); i++) {
            selectDt = (HashMap)this.selectColsDtList.get(i);
            if (!DataDefineConfig.isExistColumn((String)selectDt.get("COLUMN_NAME"))) {
                this.compErrFlg = true;
                this.compErrMsg = "SELECT句に存在しないカラムが指定されました:" + (String)selectDt.get("COLUMN_NAME");
                return;
            }
        }


        // FROM句チェック
        // テーブル数チェック
        if (this.fromTablesDtList.size() != 1) {
            this.compErrFlg = true;
            this.compErrMsg = "FROM句には1つテーブル指定しか出来ません:" + this.statement;
            return;
        }
        // テーブル存在チェック
        // TODO:取り合えずテーブル名は"csv"固定にする
        for (int i = 0; i < this.fromTablesDtList.size(); i++) {
            fromDt = (HashMap)this.fromTablesDtList.get(i);
            if (!((String)fromDt.get("TABLE_NAME")).toLowerCase().equals("csv")) {
                this.compErrFlg = true;
                this.compErrMsg = "FROM句には「csv」というテーブル名のみ指定できます:" + (String)fromDt.get("TABLE_NAME");
                return;
            }
        }


    }
*/

    /**
     * SQL文をFROM句で分解し、文字列の配列にして返す
     * 例)「select * from Atable,Btable」
     *                    ↓
     *    String[0] = select * , String[1] = Atable,Btable
     * @param String sql SQL文字列
     * @return String[] 分解後の配列
     */
     private String[] splitSqlFrom(String targetStr) {

        String workSt = targetStr.trim();
        boolean checkFromStr = false;
        char[] sqlChars = workSt.toCharArray();
        String[] retList = new String[2];
        boolean singleQuotationFlg = false;
        boolean bracketsFlg = false;
        int bracketsFlgCount = 0;

        String workStr = "";

        // 「 from 」が出てくるまで移動
        for(int charIndex = 0;charIndex < sqlChars.length;charIndex++){
            // 現在「'」の中か判断
            if(singleQuotationFlg == false && bracketsFlg == false){
                if(charIndex + 5 < sqlChars.length){
                    if(sqlChars[charIndex] == ' ' && 
                         (sqlChars[charIndex + 1] == 'f' || sqlChars[charIndex + 1] == 'F') &&
                         (sqlChars[charIndex + 2] == 'r' || sqlChars[charIndex + 2] == 'R')&&
                         (sqlChars[charIndex + 3] == 'o' || sqlChars[charIndex + 3] == 'O')&& 
                         (sqlChars[charIndex + 4] == 'm' || sqlChars[charIndex + 4] == 'M')&& 
                         sqlChars[charIndex + 5] == ' ' ){
                        // 「from」句なのでここまでの文字列を格納
                        retList[0] = workStr;
                        workStr = "";
                        charIndex = charIndex + 5;
                        checkFromStr = true;
                    }
                }
            }

            if(sqlChars[charIndex] == '\''){
                if(singleQuotationFlg == false){
                    singleQuotationFlg = true;
                }else{
                    singleQuotationFlg = false;
                }
            }

            if(singleQuotationFlg == false){
                if(sqlChars[charIndex] == '('){
                    bracketsFlgCount++;
                    bracketsFlg = true;
                }

                if(sqlChars[charIndex] == ')'){
                    bracketsFlgCount--;
                    if(bracketsFlgCount == 0)  bracketsFlg = false;
                }
            }

            workStr = workStr + String.valueOf(sqlChars[charIndex]);
        }

        if(checkFromStr == false) {
            retList[0] = workStr;
            retList[1] = null;
        } else {
            retList[1] = workStr;
        }

        // "'"や"("が終了せずに終わってないかチェック
        if (singleQuotationFlg == true || bracketsFlg == true) {
                this.compErrFlg = true;
                this.compErrMsg = "ステートメントが正しく終了していません。:" + this.statement;
        }

        return retList;
    }


    /**
     * SQL文をwhere句で分解し、文字列の配列にして返す
     * 例)「select * from Atable,Btable where Atable.A = 'a'」
     *                    ↓
     *    String[0] = select * from Atable,Btable, String[1] = Atable.A = 'a'
     * @param String sql SQL文字列
     * @return String[] 分解後の配列
     */
     private String[] splitSqlWhere(String targetStr) {
        targetStr = targetStr.trim();
        char[] sqlChars = targetStr.toCharArray();
        String[] workRet = new String[2];
        String[] retList = null;
        boolean singleQuotationFlg = false;
        boolean bracketsFlg = false;
        boolean whereSplitedFlg = false;
        int bracketsFlgCount = 0;

        String workStr = "";

        // 「 where 」が出てくるまで移動
        for(int charIndex = 0;charIndex < sqlChars.length;charIndex++){
            // 現在「'」の中か判断
            if(singleQuotationFlg == false && bracketsFlg == false && whereSplitedFlg == false){
                if(charIndex + 6 < sqlChars.length){
                    if(sqlChars[charIndex] == ' ' && 
                      (sqlChars[charIndex + 1] == 'w' || sqlChars[charIndex + 1] == 'W') &&
                      (sqlChars[charIndex + 2] == 'h' || sqlChars[charIndex + 2] == 'H') &&
                      (sqlChars[charIndex + 3] == 'e' || sqlChars[charIndex + 3] == 'E') && 
                      (sqlChars[charIndex + 4] == 'r' || sqlChars[charIndex + 4] == 'R') && 
                      (sqlChars[charIndex + 5] == 'e' || sqlChars[charIndex + 5] == 'E') &&
                      sqlChars[charIndex + 6] == ' ' ){
                        // 「where」句なのでここまでの文字列を格納
                        workRet[0] = workStr;
                        workStr = "";
                        charIndex = charIndex + 6;
                        whereSplitedFlg = true;
                    }
                }
            }

            if(sqlChars[charIndex] == '\''){
                if(singleQuotationFlg == false){
                    singleQuotationFlg = true;
                }else{
                    singleQuotationFlg = false;
                }
            }

            if(singleQuotationFlg == false){
                if(sqlChars[charIndex] == '('){
                    bracketsFlgCount++;
                    bracketsFlg = true;
                }

                if(sqlChars[charIndex] == ')'){
                    bracketsFlgCount--;
                    if(bracketsFlgCount == 0)  bracketsFlg = false;
                }
            }

            workStr = workStr + String.valueOf(sqlChars[charIndex]);
        }

        // "where"句の存在を確認
        if(workRet[0] == null){

            // "where句"なし
            retList = new String[2];
            retList[0] = workStr;
            retList[1] = null;
        }else{
            retList = new String[2];
            retList[0] = workRet[0];
            retList[1] = workStr;
        }

        // "'"や"("が終了せずに終わってないかチェック
        if (singleQuotationFlg == true || bracketsFlg == true) {
                this.compErrFlg = true;
                this.compErrMsg = "ステートメントが正しく終了していません。:" + this.statement;
        }

        return retList;
    }


    /**
     * SQL文をOR連結指で分解し、文字列の配列にして返す
     * 例)「t1.ACODE = 'B' or t2.BCODE = 'C' and t2.CCODE = 'D' or t1.NAME = 'AA'」
     *                    ↓
     *    String[0] = t1.ACODE = 'B' , String[1] = t2.BCODE = 'C' and t2.CCODE = 'D' , String[2] = t1.NAME = 'AA'
     * @param String sql SQL文字列
     * @return String[] 分解後の配列
     */
     public String[] splitSqlOr(String sql){

        ArrayList addStrList = new ArrayList(10);
        boolean singleQuotationFlg = false;
        boolean bracketsFlg = false;
        int bracketsFlgCount = 0;

        String workStr = "";
        String[] retList;
        sql = sql.trim();
        char[] whereChars = sql.toCharArray();

        // 「 or 」が出てくるまで移動
        for(int charIndex = 0;charIndex < whereChars.length;charIndex++){
            // 現在「'」の中か判断
            if(singleQuotationFlg == false && bracketsFlg == false){
                if(charIndex + 3 < whereChars.length){
    
                    if(whereChars[charIndex] == ' ' && 
                       (whereChars[charIndex + 1] == 'o' || whereChars[charIndex + 1] == 'O') &&
                       (whereChars[charIndex + 2] == 'r' || whereChars[charIndex + 2] == 'R') &&
                       whereChars[charIndex + 3] == ' ' ){
                        // 「and」句なのでここまでの文字列を格納
                        addStrList.add(workStr);
                        workStr = "";
                        charIndex = charIndex + 3;
                    }
                }
            }
  
            if(whereChars[charIndex] == '\''){
                if(singleQuotationFlg == false){
                    singleQuotationFlg = true;
                }else{
                    singleQuotationFlg = false;
                }
            }

            if(singleQuotationFlg == false){
                if(whereChars[charIndex] == '('){
                    bracketsFlgCount++;
                    bracketsFlg = true;
                }

                if(whereChars[charIndex] == ')'){
                    bracketsFlgCount--;
                    if(bracketsFlgCount == 0)  bracketsFlg = false;
                }
            }

            workStr = workStr + String.valueOf(whereChars[charIndex]);
        }

        addStrList.add(workStr);
        retList = new String[addStrList.size()];
        for(int i = 0;i < addStrList.size();i++){
            retList[i] = (String)addStrList.get(i);
        }

        // "'"や"("が終了せずに終わってないかチェック
        if (singleQuotationFlg == true || bracketsFlg == true) {
                this.compErrFlg = true;
                this.compErrMsg = "ステートメントが正しく終了していません。:" + this.statement;
        }

        return retList;
    }


    /**
     * SQL文をAND連結指で分解し、文字列の配列にして返す.<br>
     * OR連結指は存在しないものとして処理する
     * 例)「t1.ACODE = 'B' AND t2.BCODE = 'C' and t2.CCODE = 'D' AND t1.NAME = 'AA'」
     *                    ↓
     *    String[0] = t1.ACODE = 'B' , String[1] = t2.BCODE = 'C' and t2.CCODE = 'D' , String[2] = t1.NAME = 'AA'
     * @param String sql SQL文字列
     * @return String[] 分解後の配列
     */
     public String[] splitSqlAnd(String whereSql){
        ArrayList addStrList = new ArrayList(10);
        boolean singleQuotationFlg = false;
        boolean bracketsFlg = false;
        int bracketsFlgCount = 0;

        String workStr = "";
        String[] retList;
        whereSql = whereSql.trim();
        char[] whereChars = whereSql.toCharArray();

        // 「 and 」が出てくるまで移動
        for(int charIndex = 0;charIndex < whereChars.length;charIndex++){
            // 現在「'」の中か判断
            if(singleQuotationFlg == false && bracketsFlg == false){
                if(charIndex + 4 < whereChars.length){
                    if(whereChars[charIndex] == ' ' && 
                       (whereChars[charIndex + 1] == 'a' || whereChars[charIndex + 1] == 'A') &&
                       (whereChars[charIndex + 2] == 'n' || whereChars[charIndex + 2] == 'N') &&
                       (whereChars[charIndex + 3] == 'd' || whereChars[charIndex + 3] == 'D') && 
                       whereChars[charIndex + 4] == ' ' ){
                        // 「and」句なのでここまでの文字列を格納
                        addStrList.add(workStr);
                        workStr = "";
                        charIndex = charIndex + 4;
                    }
                }
            }

            if(whereChars[charIndex] == '\''){
                if(singleQuotationFlg == false){
                    singleQuotationFlg = true;
                }else{
                    singleQuotationFlg = false;
                }
            }

            if(singleQuotationFlg == false){
                if(whereChars[charIndex] == '('){
                    bracketsFlgCount++;
                    bracketsFlg = true;
                }

                if(whereChars[charIndex] == ')'){
                    bracketsFlgCount--;
                    if(bracketsFlgCount == 0)  bracketsFlg = false;
                }
            }

            workStr = workStr + String.valueOf(whereChars[charIndex]);
        }

        addStrList.add(workStr);
        retList = new String[addStrList.size()];
        for(int i = 0;i < addStrList.size();i++){
            retList[i] = (String)addStrList.get(i);
        }

        // "'"や"("が終了せずに終わってないかチェック
        if (singleQuotationFlg == true || bracketsFlg == true) {
                this.compErrFlg = true;
                this.compErrMsg = "ステートメントが正しく終了していません。:" + this.statement;
        }

        return retList;
    }


    /**
     * where句の左辺文字列がテーブルを表しているのか、数値を表しているのか、文字列を表しているのかを返す
     * 例) 「tableA.column1」 => "TABLE"
     *     「'AAA'」 => "STRING"
     *     「100」 => "NUMBER"
     *
     * @param String str 判断対象の文字列
     * @return String 判断結果
     */
    public String checkStrType(String str){
        String ret = "";
        str = str.trim();
        int strLen = str.length();

        // 文字列の先頭と末尾を調べる
        if(str.indexOf("'") == 0 && (str.lastIndexOf("'") == (strLen-1))){
            // 文字列指定
            ret = WHERE_VALUE_TYPE_STRING;
        }else{
            // float型にキャストし、キャストできなかった場合はテーブル指定
            try{
                float f = Float.parseFloat(str);
                ret = WHERE_VALUE_TYPE_NUMBER;

            }catch(Exception e){
                // 何もしない
                ret = WHERE_VALUE_TYPE_TABLE;
            }
        }
        return ret;
    }


    /**
     * SELECT文字列を削除.<br>
     */
    private String deleteSelectStr(String str) {
        str = str.trim();

        return str.substring(6, str.length());
        
    }


    /**
     * コンパイルエラーの有無を返す.<br>
     * @return boolean true:エラー false:エラー無し
     */
    public boolean isCompileErr() {
        return this.compErrFlg;
    }

    /**
     * コンパイルエラーのメッセージを返す.<br>
     * @return String メッセージ
     */
    public String getCompileErrMsg() {
        return this.compErrMsg;
    }


    /**
     * 自身がコンパイルしたステートメントのタイプを返す.<br>
     *
     * @return int 種類番号
     */
    public int getStType() {
        return this.stTypeNumber;
    }
}
}