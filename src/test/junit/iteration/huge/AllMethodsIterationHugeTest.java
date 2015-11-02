package test.junit.iteration.huge;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

/**
 * メソッドの巨大データに対する繰り返しテストを全て行うためのクラス。
 * 
 * @author T.Okuyama
 * @license GPL(Lv3)
 *
 */
@RunWith(Suite.class)
@SuiteClasses({SetMethodIterationHugeTest.class,
				GetMethodIterationHugeTest.class,
				SetNewValueMethodIterationHugeTest.class,
				LPushMethodIterationHugeTest.class,
				RPushMethodIterationHugeTest.class,
				LPopMethodIterationHugeTest.class,
				RPopMethodIterationHugeTest.class})
public class AllMethodsIterationHugeTest {

}
