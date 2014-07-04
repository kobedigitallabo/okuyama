package test.junit.parallel;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({SetMethodParallelTest.class,
				GetMethodParallelTest.class,
				RemoveMethodParallelTest.class,
				SeparateMethodsParallelTest.class})
public class AllMethodsParallelTest {

}
