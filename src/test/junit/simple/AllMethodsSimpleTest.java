package test.junit.simple;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({SetMethodSimpleTest.class,
				GetMethodSimpleTest.class,
				MultiGetMethodSimpleTest.class,
				RemoveMethodSimpleTest.class,
				SetTagMethodSimpleTest.class,
				GetTagMethodSimpleTest.class})
public class AllMethodsSimpleTest {

}
