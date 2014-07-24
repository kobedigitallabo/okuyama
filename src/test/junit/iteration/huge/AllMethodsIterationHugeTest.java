package test.junit.iteration.huge;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({SetMethodIterationHugeTest.class,
				GetMethodIterationHugeTest.class,
				SetNewValueMethodIterationHugeTest.class})
public class AllMethodsIterationHugeTest {

}
