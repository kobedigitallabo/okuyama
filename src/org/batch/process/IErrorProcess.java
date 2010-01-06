package org.batch.process;

import java.util.Hashtable;

import org.batch.lang.BatchException;

/**
 * ErrorProcessクラスを実装する場合のインターフェース.<br>
 *
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public interface IErrorProcess {


    /**
     * ErrorProcess実行部分.<br>
     *
     * @param jobTable 実行終了後のJobインスタンスがJob名をキーに格納されている
     * @param allJobStatusTable 各Jobのステータスを格納したテーブル
     * @param option 設定ファイルのoption部分
     */
    public void errorProcess(Hashtable jobTable, 
                             Hashtable allJobStatusTable, 
                             String option) throws BatchException;
}