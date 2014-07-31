package test.junit.iteration;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({SetMethodIterationTest.class,
				GetMethodIterationTest.class,
				RemoveMethodIterationTest.class,
				SetTagMethodIterationTest.class,
				GetTagMethodIterationTest.class,
				RemoveTagMethodIterationTest.class,
				IncrAndDecrMethodIterationTest.class,
				CasMethodIterationTest.class,
				SetNewValueMethodIterationTest.class,
				ScriptMethodIterationTest.class,
				LPushMethodIterationTest.class,
				RPushMethodIterationTest.class,
				LPopMethodIterationTest.class,
				RPopMethodIterationTest.class})
public class AllMethodsIterationTest {

}
