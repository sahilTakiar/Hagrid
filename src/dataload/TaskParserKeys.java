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

/**
 * This class defines a set of strings used for parsing the log files. The log files should match this pattern, however, if they do not
 * these strings can be modified so that they parse the right data. Example: In a typical log file the Spilled Records for a task
 * will be outputed in the following format: "[(SPILLED_RECORDS)(Spilled Records)(19511558)]"
 */
public class TaskParserKeys {

	public static final String COMBINER_INPUT = "(COMBINE_INPUT_RECORDS)(Combine input records)(";
	public static final String COMBINER_OUTPUT = "(COMBINE_OUTPUT_RECORDS)(Combine output records)(";
	
	public static final String FAILED_MAPS = "FAILED_MAPS=\"";
	public static final String FAILED_REDUCES = "FAILED_REDUCES=\"";
	
	public static final String FINISHED_MAPS = "FINISHED_MAPS=\"";
	public static final String FINISHED_REDUCES = "FINISHED_REDUCES=\"";
	
	public static final String FINISH_TIME = "FINISH_TIME=\"";
	
	public static final String FILE_BYTES_READ = "(FILE_BYTES_READ)(FILE_BYTES_READ)(";
	public static final String FILE_BYTES_WRITTEN = "(FILE_BYTES_WRITTEN)(FILE_BYTES_WRITTEN)(";
	
	public static final String HDFS_BYTES_READ = "(HDFS_BYTES_READ)(HDFS_BYTES_READ)(";
	public static final String HDFS_BYTES_WRITTEN = "(HDFS_BYTES_WRITTEN)(HDFS_BYTES_WRITTEN)(";
	
	public static final String JOBID = "JOBID=\"";
	public static final String JOBNAME = "JOBNAME=\"";
	
	public static final String MAP_OUTPUT_BYTES = "(MAP_OUTPUT_BYTES)(Map output bytes)(";
	public static final String MAP_OUTPUT_RECORDS = "(MAP_OUTPUT_RECORDS)(Map output records)(";
	public static final String MAP_INPUT_RECORDS = "(MAP_INPUT_RECORDS)(Map input records)(";
	
	public static final String RACK_LOCAL_MAPS = "(RACK_LOCAL_MAPS)(Rack-local map tasks)(";
	
	public static final String REDUCE_INPUT_RECORDS = "(REDUCE_INPUT_RECORDS)(Reduce input records)(";
	public static final String REDUCE_INPUT_GROUPS = "(REDUCE_INPUT_GROUPS)(Reduce input groups)(";
	public static final String REDUCE_OUTPUT_RECORDS = "(REDUCE_OUTPUT_RECORDS)(Reduce output records)(";
	public static final String REDUCE_SHUFFLE_BYTES = "(REDUCE_SHUFFLE_BYTES)(Reduce shuffle bytes)(";
	
	public static final String START_TIME = "START_TIME=\"";
	public static final String SPILLED_RECORDS = "(SPILLED_RECORDS)(Spilled Records)(";
	public static final String TASK_TYPE = "TASK_TYPE=\"";
}