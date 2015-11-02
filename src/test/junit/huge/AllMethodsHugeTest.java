package test.junit.huge;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

/**
 * 巨大でーたに対する簡単なメソッドを全て行うクラス。
 * 
 * @author T.Okuyama
 * @license GPL(Lv3)
 *
 */
@RunWith(Suite.class)
@SuiteClasses({SetMethodHugeTest.class,
				GetMethodHugeTest.class,
				MultiGetMethodHugeTest.class,
				SetNewValueMethodHugeTest.class,
				LPushMethodHugeTest.class,
				RPushMethodHugeTest.class,
				LPopMethodHugeTest.class,
				RPopMethodHugeTest.class})
public class AllMethodsHugeTest {

}
