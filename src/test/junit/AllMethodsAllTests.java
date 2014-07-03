package test.junit;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({test.junit.simple.AllMethodsSimpleTest.class,
				test.junit.huge.AllMethodsHugeTest.class,
				test.junit.iteration.AllMethodsIterationTest.class,
				test.junit.iteration.huge.AllMethodsIterationHugeTest.class})
public class AllMethodsAllTests {

}
