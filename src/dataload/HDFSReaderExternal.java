/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dataload;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 
 * Helper class to read files from HDFS. Given the HDFS path it will read the file, and put each text line into an arraylist. The arraylist will then
 * be returned after the class is finished parsing the file. The XML configuration files will need to be present in the src folder so that they can
 * be read by this class. This is necessary so that the code has to information to link to the Hadoop cluster. Necessary XML files typically should
 * include core-site.xml, hdfs-site.xml, and mapred-site.xml. This will only work if the Hadoop cluster allows external access to HDFS, if not then
 * another implementations of HDFSRead needs to be written to access HDFS.
 * 
 * @author stakiar
 */
public class HDFSReaderExternal implements HDFSReader {

	public ArrayList<String> readFile(String fileName) throws FileNotFoundException {
		ArrayList<String> output = new ArrayList<String>();
		try {
			Configuration conf = new Configuration();
			
			FileSystem hdfs = FileSystem.get(java.net.URI.create(fileName), conf);
			FSDataInputStream in = hdfs.open(new Path(fileName));

			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String strLine;
			
			while ((strLine = br.readLine()) != null) {
				System.out.println(strLine);
				output.add(strLine);
			}
			hdfs.close();
		} catch ( IOException e ) {
			e.printStackTrace();
			System.err.println("Error: " + e.getMessage());
		}
		return output;
	}
	
	/**
	 * Main method is used as a testing mechanism. Run this class and change the parameter to readFile() in order to test to see if this is working.
	 * If no file is printed in console then the class is not accessing the Hadoop Grid properly.
	 */
	public static void main(String args[]) {
		HDFSReaderExternal read = new HDFSReaderExternal();
		try {
			for ( String s : read.readFile(args[0]) ) {
				System.out.println(s);
			}			
		} catch ( FileNotFoundException e ) {
			System.err.println(e);
			System.err.println(e.getStackTrace());
		}
	}
}