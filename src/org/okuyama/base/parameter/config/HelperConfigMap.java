package org.okuyama.base.parameter.config;

import java.util.ArrayList;


/** 
 * 一つのHelperに対する詳細情報を保持する.<br>
 * 
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class HelperConfigMap {

    private String helperName = null;

    private String helperClassName = null;

    private String helperInit = null;

    private String helperOption = null;

    private int helperLimitSize = 0;

    private int maxHelperUse = 0;

    private String[] helperDbgroupList = null;

    private String helperCommit = null;


    /**
     * Helper定義情報を初期化.<br>
     *
     * @param helperName
     * @param helperClassName
     * @param helperInit
     * @param helperOption
     * @param helperLimitSize
     * @param maxHelperUse
     */
    public HelperConfigMap(String helperName, String helperClassName, String helperInit, String helperOption, int helperLimitSize, int maxHelperUse, String[] helperDbgroupList, String helperCommit) {
        this.helperName = helperName;
        this.helperClassName = helperClassName;
        this.helperInit = helperInit;
        this.helperOption = helperOption;
        this.helperLimitSize = helperLimitSize;
        this.maxHelperUse = maxHelperUse;
        this.helperDbgroupList = helperDbgroupList;
        this.helperCommit = helperCommit;
    }


    /**
     * Helper名を返す.<br>
     * 
     * @return String Helper名
     */
    public String getHelperName() {
        return this.helperName;
    }


    /**
     * Helperクラス名を返す.<br>
     *
     * @return String Helperクラス名
     */
    public String getHelperClassName() {
        return this.helperClassName;
    }


    /**
     * Initオプションを返す.<br>
     *
     * @return String Initオプション値
     */
    public String getHelperInit() {
        return this.helperInit;
    }

    /**
     * Helperのオプション値を返す
     * 
     * @return String オプション値
     */
    public String getHelperOption() {
        return this.helperOption;
    }

    /**
     * Helperのインスタンス化可能数を返す
     * 
     * @return int 
     */
    public int getHelperLimitSize() {
        return this.helperLimitSize;
    }

    /**
     * Helperの1インスタンスあたりの使用上限回数を返す.<br>
     * 
     * @return int 
     */
    public int getMaxHelperUse() {
        return this.maxHelperUse;
    }

    /**
     * HelperDbgroup指定が存在するかを返す.<br>
     *
     * @return boolean 真偽値
     */
    public boolean isHelperDbGroup() {
        if (this.helperDbgroupList == null) {
            return false;
        }
        return true;
    }

    /**
     * 自身が使用するデータベースグループ名を返す.<br>
     *
     * @return String[] グループリスト
     */
    public String[] getHelperDbgroupList() {
        return this.helperDbgroupList;
    }

    /**
     * 自身のCommit指定を返す.<br>
     */
    public String getHelperCommit() {
        return this.helperCommit;
    }

}
