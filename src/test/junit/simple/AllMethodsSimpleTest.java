package test.junit.simple;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

/**
 * 全メソッドの単純なテストを行うためのクラス。
 * 
 * @author T.Okuyama
 * @license GPL(Lv3)
 *
 */
@RunWith(Suite.class)
@SuiteClasses({SetMethodSimpleTest.class,
				GetMethodSimpleTest.class,
				MultiGetMethodSimpleTest.class,
				RemoveMethodSimpleTest.class,
				SetTagMethodSimpleTest.class,
				GetTagMethodSimpleTest.class,
				RemoveTagMethodSimpleTest.class,
				MultiGetTagMethodSimpleTest.class,
				SetExpireTest.class,
				IncrAndDecrMethodSimpleTest.class,
				CasMethodSimpleTest.class,
				SearchMethodSimpleTest.class,
				SetNewValueMethodSimpleTest.class,
				ScriptMethodSimpleTest.class,
				ListCreateMethodSimpleTest.class,
				LPushMethodSimpleTest.class,
				RPushMethodSimpleTest.class,
				IndexMethodSimpleTest.class,
				LPopMethodSimpleTest.class,
				RPopMethodSimpleTest.class})
public class AllMethodsSimpleTest {

}
