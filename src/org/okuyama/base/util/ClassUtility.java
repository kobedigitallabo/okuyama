package org.okuyama.base.util;

import java.lang.reflect.Method;

import org.okuyama.base.job.AbstractJob;
import org.okuyama.base.job.AbstractHelper;

/**
 * リフレクションを使用してクラスのインスタンス生成や、<br>
 * そのほかメソッド実行などをまとめる.<br>
 * 
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class ClassUtility {

    /**
     * クラスインスタンス作成.<br>
     * 
     * @param className ターゲットクラス名
     * @return Object ターゲットクラス
     * @throws Exception
     */
    public static Object createInstance(String className) throws Exception {
        Object retObj = null;
        try {
            retObj = Class.forName(className).newInstance();

        } catch (ClassNotFoundException ce) {
            throw new Exception(className + ":そのようなクラスは存在しません", ce);
        } catch (Exception e) {
            throw e;
        }
        return retObj;
    }

    /**
     * Jobクラスインスタンス作成.<br>
     * 
     * @param className ターゲットJob名
     * @return AbstractJob ターゲットJob
     * @throws Exception
     */
    public static AbstractJob createJobInstance(String className) throws Exception {
        Object retObj = null;
        try {
            retObj = Class.forName(className).newInstance();

        } catch (ClassNotFoundException ce) {
            throw new Exception(className + ":そのようなクラスは存在しません", ce);
        } catch (Exception e) {
            throw e;
        }
        return (AbstractJob)retObj;
    }

    /**
     * Helperクラスインスタンス作成.<br>
     * 
     * @param className ターゲットHelper名
     * @return AbstractJob ターゲットHelper
     * @throws Exception
     */
    public static AbstractHelper createHelperInstance(String className) throws Exception {
        Object retObj = null;
        try {
            retObj = Class.forName(className).newInstance();

        } catch (ClassNotFoundException ce) {
            throw new Exception(className + ":そのようなクラスは存在しません", ce);
        } catch (Exception e) {
            throw e;
        }
        return (AbstractHelper)retObj;
    }

}