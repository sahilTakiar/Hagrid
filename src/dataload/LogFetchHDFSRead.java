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

import java.io.FileNotFoundException;
import java.text.NumberFormat;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * Incomplete class that will read a log file stored on a HDFS cluster, load it into the web app, and then send it to the TaskServlet for processing
 * This class implements the LogFetch interface which is the base interface for all classes that load the log files into Hagrid. In order for this class
 * to be complete, the fetchData() method needs to be completed, and the TaskServlet class needs to be changed so that it uses this class to get the log
 * files, and then forwards it to TaskGraphs.jsp
 * 
 * The fetchData() method currently parses through the log files produced by Hadoop 1.0.2, but if the log file format is changed in a different release
 * of Hadoop then the parsing mechanism will need to be changed, and changes to TaskParserKeys.java will also be necessary
 */
public class LogFetchHDFSRead implements LogFetch {

	@Override
	public void fetchData() {
		
		// INITIALIZE VARIABLES
		String line;
		int start = 0, endJob = 3, endTask = 4;
		
		// READ HDFS FILE
		HDFSReaderExternal reader = new HDFSReaderExternal();
		Iterator<String> itr = null;
		try {
			itr = reader.readFile("ENTER PATH TO FILE HERE").iterator();
		} catch ( FileNotFoundException e ) {
			e.printStackTrace();
			System.err.println("Error: " + e.getMessage());
		}
		
		// CREATE A SERIES OF TREEMAPS TO STORE ALL THE DATA FOR THE DIFFERENT METRICS
		TreeMap<String, String>
		
		// COMBINERS
		combinerInputMap = new TreeMap<String, String>(),
		combinerInputReduce = new TreeMap<String, String>(),
		combinerOutputMap = new TreeMap<String, String>(),
		combinerOutputReduce = new TreeMap<String, String>(),
		
		// FILE BYTES
		fileBytesWrittenMap = new TreeMap<String, String>(),
		fileBytesReadMap = new TreeMap<String, String>(),
		fileBytesWrittenReduce = new TreeMap<String, String>(),
		fileBytesReadReduce = new TreeMap<String, String>(),
		
		// HDFS BYTES
		hdfsBytesRead = new TreeMap<String, String>(),
		hdfsBytesWritten = new TreeMap<String, String>(),
		
		// MAPS
		mapInputRecords = new TreeMap<String, String>(),
		mapOutputBytes = new TreeMap<String, String>(),
		mapOutputRecords = new TreeMap<String, String>(),
		
		// REDUCES
		reduceInputRecords = new TreeMap<String, String>(),
		reduceInputGroups = new TreeMap<String, String>(),
		reduceOutputRecords = new TreeMap<String, String>(),
		reduceShuffleBytes = new TreeMap<String, String>(),
		
		// SPILLS
		spilledRecordsMap = new TreeMap<String, String>(),
		spilledRecordsReduce = new TreeMap<String, String>(),
		
		// TIME
		finishTime = new TreeMap<String, String>(),
		startTime = new TreeMap<String, String>(),
		
		// OTHER
		taskType = new TreeMap<String, String>(), 
		jobMap = new TreeMap<String, String>();
		
		// ITERATE THROUGH EACH LINE OF THE LOG FILE
		while ( itr.hasNext() ) {
			
			// INITIALIZE
			String taskId; int i;
			
			// GET NEXT LINE
			line = itr.next();

			// ONLY CONSIDER TASK LEVEL LINES
			if ( line.substring(start, endTask).equals("Task") ) {
				
				// ISOLATE THE PART OF THE STRING THAT CONTAINS THE COUNTERS
				i = line.indexOf("TASKID=\"") + "TASKID=\"".length();
				taskId = line.substring(i, line.indexOf("\"", i));
				
				// TASK MUST BE OF TYPE MAP OR REDUCE
				if ( line.contains("TASK_TYPE=\"MAP\"") ) {
					if ( line.contains("TASK_STATUS=\"SUCCESS\"") ) {
						taskCounterParameters(TaskParserKeys.COMBINER_INPUT, taskId, line, combinerInputMap);
						taskCounterParameters(TaskParserKeys.COMBINER_OUTPUT, taskId, line, combinerOutputMap);

						taskGeneralParameters(TaskParserKeys.FINISH_TIME, taskId, line, finishTime);

						taskCounterParameters(TaskParserKeys.FILE_BYTES_READ, taskId, line, fileBytesReadMap);
						taskCounterParameters(TaskParserKeys.FILE_BYTES_WRITTEN, taskId, line, fileBytesWrittenMap);

						taskCounterParameters(TaskParserKeys.HDFS_BYTES_READ, taskId, line, hdfsBytesRead);
						taskCounterParameters(TaskParserKeys.HDFS_BYTES_WRITTEN, taskId, line, hdfsBytesWritten);

						taskCounterParameters(TaskParserKeys.MAP_INPUT_RECORDS, taskId, line, mapInputRecords);
						taskCounterParameters(TaskParserKeys.MAP_OUTPUT_BYTES, taskId, line, mapOutputBytes);
						taskCounterParameters(TaskParserKeys.MAP_OUTPUT_RECORDS, taskId, line, mapOutputRecords);

						taskCounterParameters(TaskParserKeys.SPILLED_RECORDS, taskId, line, spilledRecordsMap);
						taskGeneralParameters(TaskParserKeys.TASK_TYPE, taskId, line, taskType);
					} else {
						taskGeneralParameters(TaskParserKeys.START_TIME, taskId, line, startTime);
					}
				}
				
				if ( line.contains("TASK_TYPE=\"REDUCE\"") ) {
					if ( line.contains("TASK_STATUS=\"SUCCESS\"") ) {
						taskCounterParameters(TaskParserKeys.COMBINER_INPUT, taskId, line, combinerInputReduce);
						taskCounterParameters(TaskParserKeys.COMBINER_OUTPUT, taskId, line, combinerOutputReduce);

						taskGeneralParameters(TaskParserKeys.FINISH_TIME, taskId, line, finishTime);

						taskCounterParameters(TaskParserKeys.FILE_BYTES_READ, taskId, line, fileBytesReadReduce);
						taskCounterParameters(TaskParserKeys.FILE_BYTES_WRITTEN, taskId, line, fileBytesWrittenReduce);

						taskCounterParameters(TaskParserKeys.HDFS_BYTES_READ, taskId, line, hdfsBytesRead);
						taskCounterParameters(TaskParserKeys.HDFS_BYTES_WRITTEN, taskId, line, hdfsBytesWritten);

						taskCounterParameters(TaskParserKeys.REDUCE_INPUT_RECORDS, taskId, line, reduceInputRecords);
						taskCounterParameters(TaskParserKeys.REDUCE_INPUT_GROUPS, taskId, line, reduceInputGroups);
						taskCounterParameters(TaskParserKeys.REDUCE_OUTPUT_RECORDS, taskId, line, reduceOutputRecords);
						taskCounterParameters(TaskParserKeys.REDUCE_SHUFFLE_BYTES, taskId, line, reduceShuffleBytes);

						taskCounterParameters(TaskParserKeys.SPILLED_RECORDS, taskId, line, spilledRecordsReduce);
						taskGeneralParameters(TaskParserKeys.TASK_TYPE, taskId, line, taskType);
					} else {
						taskGeneralParameters(TaskParserKeys.START_TIME, taskId, line, startTime);
					}
				}
			}
			
			// PROCESS JOB LEVEL LINES
			storeJobParameters(start, endJob, line, jobMap);
		}
		
		// CALCULATE AND STORE TOTALS
		storeTotal(combinerInputMap, jobMap, "Map Combine Input");
		storeTotal(combinerInputReduce, jobMap, "Reduce Combine Input");
		
		storeTotal(combinerOutputMap, jobMap, "Map Combine Output");
		storeTotal(combinerOutputReduce, jobMap, "Reduce Combine Output");
		
		storeTotal(fileBytesReadMap, jobMap, "Map File Bytes Read");
		storeTotal(fileBytesReadReduce, jobMap, "Reduce File Bytes Read");
		
		storeTotal(fileBytesWrittenMap, jobMap, "Map File Bytes Written");
		storeTotal(fileBytesWrittenReduce, jobMap, "Reduce File Bytes Written");
		
		storeMapTotal(hdfsBytesRead, jobMap, "Map HDFS Bytes Read");
		storeReduceTotal(hdfsBytesRead, jobMap, "Reduce HDFS Bytes Read");
		
		storeMapTotal(hdfsBytesWritten, jobMap, "Map HDFS Bytes Written");
		storeReduceTotal(hdfsBytesWritten, jobMap, "Reduce HDFS Bytes Written");
		
		storeMapTotal(mapInputRecords, jobMap, "Map Input Records");
		storeMapTotal(mapOutputBytes, jobMap, "Map Output Bytes");
		storeMapTotal(mapOutputRecords, jobMap, "Map Output Records");
		
		storeReduceTotal(reduceInputRecords, jobMap, "Reduce Input Records");
		storeReduceTotal(reduceInputGroups, jobMap, "Reduce Input Groups");
		storeReduceTotal(reduceOutputRecords, jobMap, "Reduce Output Records");
		storeReduceTotal(reduceShuffleBytes, jobMap, "Reduce Shuffle Bytes");
		
		storeTotal(spilledRecordsMap, jobMap, "Map Spilled Records");
		storeTotal(spilledRecordsReduce, jobMap, "Reduce Spilled Records");
		
		storeTotalTime(finishTime, startTime, jobMap);
	}
	
	/**
	 * Helper method to store the job parameters in the job TreeMap
	 * @param start
	 * @param endJob
	 * @param line
	 * @param jobMap
	 */
	private void storeJobParameters(int start, int endJob, String line, TreeMap<String, String> jobMap) {
		if ( line.substring(start, endJob).equals("Job") ) {
			if ( line.contains("JOBNAME") ) {
				jobGeneralParameters(TaskParserKeys.JOBNAME, "JobName", line, jobMap);
			}
			if ( line.contains("FINISH_TIME") ) {
				jobGeneralParameters(TaskParserKeys.FAILED_MAPS, "Failed Maps", line, jobMap);
				jobGeneralParameters(TaskParserKeys.FAILED_REDUCES, "Failed Reduces", line, jobMap);
				
				jobGeneralParameters(TaskParserKeys.FINISHED_MAPS, "Finished Maps", line, jobMap);
				jobGeneralParameters(TaskParserKeys.FINISHED_REDUCES, "Finished Reduces", line, jobMap);
							
				taskCounterParameters(TaskParserKeys.RACK_LOCAL_MAPS, "Total Rack Local Maps", line, jobMap);
			}
		}
	}
	
	/**
	 * Helper method to store the total time in the job TreeMap
	 * @param map1
	 * @param map2
	 * @param map3
	 */
	private void storeTotalTime(TreeMap<String, String> map1, TreeMap<String, String> map2, TreeMap<String, String> map3) {
		double total = 0;
		for ( Map.Entry<String, String> entry : map1.entrySet() ) {
			total += (Long.parseLong(entry.getValue()) - Long.parseLong(map2.get(entry.getKey())));
		}
		map3.put("Total Time", formatTotals(total/60000));
	}
	
	private void storeTotal(TreeMap<String, String> map1, TreeMap<String, String> map2, String key) {
		long total = 0;
		for ( Map.Entry<String, String> entry : map1.entrySet() ) {
			total += Long.parseLong(entry.getValue());
		}
		map2.put(key, formatTotals(total));
	}
	
	private void storeMapTotal(TreeMap<String, String> map1, TreeMap<String, String> map2, String key) {
		long total = 0;
		for ( Map.Entry<String, String> entry : map1.entrySet() ) {
			if ( entry.getKey().contains("_m_") ) {
				total += Long.parseLong(entry.getValue());
			}
		}
		map2.put(key, formatTotals(total));
	}
	
	private void storeReduceTotal(TreeMap<String, String> map1, TreeMap<String, String> map2, String key) {
		long total = 0;
		for ( Map.Entry<String, String> entry : map1.entrySet() ) {
			if ( entry.getKey().contains("_r_") ) {
				total += Long.parseLong(entry.getValue());
			}
		}
		map2.put(key, formatTotals(total));
	}
	
	/**
	 * Helper method to parse task level parameters
	 * @param param
	 * @param newKey
	 * @param line
	 * @param map
	 */
	private void taskCounterParameters(String param, String newKey, String line, TreeMap<String, String> map) {
		int i;
		String newValue;
		if ( (i = line.indexOf(param)) != -1 ) {
			newValue = line.substring(i + param.length(), line.indexOf(")", i + param.length()));
			map.put(newKey, newValue);
		}
	}
	
	/**
	 * Helper method to parse task level general parameters
	 * @param param
	 * @param newKey1
	 * @param line
	 * @param map
	 */
	private void taskGeneralParameters(String param, String newKey1, String line, TreeMap<String, String> map) {
		int i;
		String newValue;
		if ( (i = line.indexOf(param)) != -1 ) {
			newValue = line.substring(i + param.length(), line.indexOf("\"", i + param.length()));
			map.put(newKey1, newValue);
		}
	}
	
	/**
	 * Helper method to parse job level parameters
	 * @param counter
	 * @param newKey
	 * @param line
	 * @param map
	 */
	private void jobGeneralParameters(String counter, String newKey, String line, TreeMap<String, String> map) {
		int i;
		String newValue;
		if ( (i = line.indexOf(counter)) != -1 ) {
			newValue = line.substring(i + counter.length(), line.indexOf("\"", i + counter.length()));
			map.put(newKey, newValue);
		}
	}

	/**
	 * Helper method to format total long values sent to the dashboard
	 * @param total
	 */
	private String formatTotals(long total) {
		NumberFormat nf = NumberFormat.getInstance();
		return nf.format(total);
	}
	
	/**
	 * Helper method to format total double values sent to the dashboard
	 * @param total
	 */
	private String formatTotals(double total) {
		NumberFormat nf = NumberFormat.getInstance();
		return nf.format(total);
	}
	
	@Override
	public void storeData() {
		/*
		 * Blank because a HDFS reader has no need to store the data in the database
		 * Since the the data is already present on HDFS Hagrid should fetch the data each
		 * time the user requests it. A possible change to this would be to implement a
		 * caching system to decrease the number of requests to HDFS files. If a user wants
		 * to implement a cache create another class that implements LogFetch,
		 * or create a class that extends LogFetchHDFSRead
		*/ 
	}
}