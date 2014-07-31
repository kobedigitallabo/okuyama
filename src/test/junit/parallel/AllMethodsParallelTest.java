package test.junit.parallel;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({SetMethodParallelTest.class,
				GetMethodParallelTest.class,
				RemoveMethodParallelTest.class,
				SetTagMethodParallelTest.class,
				GetTagMethodParallelTest.class,
				RemoveTagMethodParallelTest.class,
				SeparateMethodsParallelTest.class,
				IncrAndDecrMethodParallelTest.class,
				SetNewValueMethodParallelTest.class,
				ScriptMethodParallelTest.class,
				ListCreateMethodParallelTest.class,
				LPushMethodParallelTest.class,
				RPushMethodParallelTest.class,
				LPopMethodParallelTest.class,
				RPopMethodParallelTest.class})
public class AllMethodsParallelTest {

}
