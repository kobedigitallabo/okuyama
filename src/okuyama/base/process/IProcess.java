package okuyama.base.process;

import okuyama.base.lang.BatchException;


/**
 * Processクラスを実装する場合のインターフェース.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)

 */
public interface IProcess {

    /**
     * Process実行部分.<br>
     *
     * @param option 設定ファイルのoption部分
     * @return String 結果(必要な場合のみ、nullでもかまわない)
     *                ここでのリターンの値は、AbstractJobクラスのgetPreProcessメソッド、
     *                getPostProcessメソッドで取得可能
     * @throws BatchException
     */
    public String process(String option) throws BatchException;
}