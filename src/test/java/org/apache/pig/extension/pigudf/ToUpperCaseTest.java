package org.apache.pig.extension.pigudf;

import org.apache.pig.pigunit.PigTest;


public class ToUpperCaseTest {

	// @Test
	public void testUpper() throws Exception {
		PigTest pigTest = new PigTest("src/main/resources/scripts/uppercase.pig");
		pigTest.assertOutput("C", new String[] { "(TEST)", "(DATA)" });
	}

}
