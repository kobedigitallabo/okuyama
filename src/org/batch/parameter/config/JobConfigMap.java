package org.batch.parameter.config;

import java.util.ArrayList;


/** 
 * 一つのJobに対する詳細情報を保持する.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class JobConfigMap {

    private String jobName = null;

    private String jobClassName = null;

    private String jobInit = null;

    private String jobOption = null;

    private String[] jobDependList = null;

    private String[] jobDbgroupList = null;

    private String jobCommit = null;

    /**
     * Job定義情報を初期化.<br>
     */
    public JobConfigMap(String jobName, String jobClassName, String jobInit, String jobOption, String[] jobDependList, String[] jobDbgroupList, String jobCommit) {
        this.jobName = jobName;
        this.jobClassName = jobClassName;
        this.jobInit = jobInit;
        this.jobOption = jobOption;
        this.jobDependList = jobDependList;
        this.jobDbgroupList = jobDbgroupList;
        this.jobCommit = jobCommit;
    }


    /**
     * Job名を返す.<br>
     */
    public String getJobName() {
        return this.jobName;
    }


    /**
     * Jobクラス名を返す.<br>
     */
    public String getJobClassName() {
        return this.jobClassName;
    }


    /**
     * Initオプションを返す.<br>
     */
    public String getJobInit() {
        return this.jobInit;
    }

    /**
     * Jobオプションを返す.<br>
     */
    public String getJobOption() {
        return this.jobOption;
    }


    /**
     * 依存関係が存在するかを返す.<br>
     */
    public boolean isDepend() {
        if (this.jobDependList == null) {
            return false;
        }
        return true;
    }

    /**
     * 自身が依存しているJob名を返す.<br>
     */
    public String[] getJobDependList() {
        return this.jobDependList;
    }


    /**
     * Dbgroup指定が存在するかを返す.<br>
     */
    public boolean isDbGroup() {
        if (this.jobDbgroupList == null) {
            return false;
        }
        return true;
    }

    /**
     * 自身が使用するデータベースグループ名を返す.<br>
     */
    public String[] getDbgroupList() {
        return this.jobDbgroupList;
    }

    /**
     * 自身のCommit指定を返す.<br>
     */
    public String getJobCommit() {
        return this.jobCommit;
    }
}
