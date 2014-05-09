package org.apache.pig.extension.pigudfminicluster;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Scanner;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pig.data.Tuple;
import org.apache.pig.pigunit.PigTest;
import org.junit.Test;

import antlr.collections.List;

public class ToUpperCaseTest extends MiniClusterTestBase {
	
	
	@Test
	public void testUpper() throws Exception {
		String IN_DIR = "/tmp";
		String DATA_FILE = "lowercase.data";
		FileSystem fs = FileSystem.get(super.getConfiguration());
		Path inDir = new Path(IN_DIR);
		
		FileReader fileReader = new FileReader("src/main/resources/data/lowercase.data");
        BufferedReader bufferedReader = new BufferedReader(fileReader);
        ArrayList<String> lines = new ArrayList<String>();
        String line = null;
        while ((line = bufferedReader.readLine()) != null) {
            lines.add(line);
        }
        bufferedReader.close();
		super.writeHDFSContent(fs, inDir, DATA_FILE, lines);
		
		
		PigTest pigTest = new PigTest("src/main/resources/scriptsminicluster/uppercase.pig");
		System.out.println("************************************************************************");	
		Iterator<Tuple> tuples = pigTest.getAlias("C");
		while(tuples.hasNext()) {
			System.out.println(tuples.next().toDelimitedString(","));		
		}
		pigTest.assertOutput("C", new String[] { "(TEST)", "(DATA)" });
		
		fs.delete(inDir, true);
	}

}
