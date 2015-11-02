package test.junit;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

/**
 * 全メソッドの全テストを行うためのクラス。
 * 
 * @author T.Okuyama
 * @license GPL(Lv3)
 *
 */
@RunWith(Suite.class)
@SuiteClasses({test.junit.simple.AllMethodsSimpleTest.class,
				test.junit.huge.AllMethodsHugeTest.class,
				test.junit.iteration.AllMethodsIterationTest.class,
				test.junit.iteration.huge.AllMethodsIterationHugeTest.class,
				test.junit.parallel.AllMethodsParallelTest.class})
public class AllMethodsAllTests {

}
