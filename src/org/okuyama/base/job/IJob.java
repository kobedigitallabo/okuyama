package org.okuyama.base.job;

import org.okuyama.base.lang.BatchException;


/**
 * Job実行クラスのインタフェース.<br>
 * 実行順序としては、initJobが呼び出され、<br>
 * その後、executeJobが呼び出される.<br>
 *
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public interface IJob {

    /**
     * 特殊な初期化の処理を行う
     *
     * @param initValue 初期値(設定ファイルの「init」で指定した値になる)
     */
    public void initJob(String initValue) ;


    /**
     * Job実行部分.<br>
     * ユーザ実装部分<br>
     *
     * @param optionParam 初期値(設定ファイルの「option」で指定した値になる)
     * @return String 自身の親クラスの定義文字列 SUCCESS, ERROR
     * @throws BatchException
     */
    public String executeJob(String optionParam) throws BatchException;
}   