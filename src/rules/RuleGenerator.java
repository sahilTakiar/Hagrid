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

package rules;

import java.util.HashMap;

/**
 * This class generates all the rules that are present in the rules package. Each rule has access to all the public instance variables in the class constructor. These variables
 * are taken from the SQL database and are passed to RuleGenerator from the TaskServlet
 */
public class RuleGenerator {

	private String[] rules;
	
	public HashMap<String, String> jobParams;
	public String[] bytesRead;
	public String[] bytesWritten;
	public String[] combineInputRecords;
	public String[] combineOutputRecords;
	public String[] committedHeapBytes;
	public String[] cpuMilliseconds;
	public String[] fileBytesRead;
	public String[] fileBytesWritten;
	public String[] hdfsBytesRead;
	public String[] hdfsBytesWritten;
	public String[] mapInputBytes;
	public String[] mapInputRecords;
	public String[] mapOuputBytes;
	public String[] mapOutputMaterializedBytes;
	public String[] mapOutputRecords;
	public String[] physicalMemoryBytes;
	public String[] reduceInputGroups;
	public String[] reduceInputRecords;
	public String[] reduceOuputRecords;
	public String[] reduceShuffleBytes;
	public String[] splitRawBytes;
	public String[] spilledRecords;
	public String[] virtualMemoryBytes;
	public String[] taskTime;
	public String[] taskIds;

	public RuleGenerator(String taskIds, String jobParams, String bytesRead, String bytesWritten, String combineInputRecords, String combineOutputRecords, String committedHeapBytes, String cpuMilliseconds, String fileBytesRead, String fileBytesWritten, String hdfsBytesRead, String hdfsBytesWritten, String mapInputBytes, String mapInputRecords, String mapOuputBytes, String mapOutputMaterializedBytes, String mapOutputRecords, String physicalMemoryBytes, String reduceInputGroups, String reduceInputRecords, String reduceOuputRecords, String reduceShuffleBytes, String splitRawBytes, String spilledRecords, String virtualMemoryBytes, String taskTime) {

		this.jobParams = new HashMap<String, String>();
		String[] param;
		
		for ( String s : jobParams.split(",") ) {
			param = s.split(": ");
			this.jobParams.put(param[0], param[1]);
		}
		
		this.bytesRead = bytesRead.split(",");
		this.bytesWritten = bytesWritten.split(",");
		this.combineInputRecords = combineInputRecords.split(",");
		this.combineOutputRecords = combineOutputRecords.split(",");
		this.committedHeapBytes = committedHeapBytes.split(",");
		this.cpuMilliseconds = cpuMilliseconds.split(",");
		this.fileBytesRead = fileBytesRead.split(",");
		this.fileBytesWritten = fileBytesWritten.split(",");
		this.hdfsBytesRead = hdfsBytesRead.split(",");
		this.hdfsBytesWritten = hdfsBytesWritten.split(",");
		this.mapInputBytes = mapInputBytes.split(",");
		this.mapInputRecords = mapInputRecords.split(",");
		this.mapOuputBytes = mapOuputBytes.split(",");
		this.mapOutputMaterializedBytes = mapOutputMaterializedBytes.split(",");
		this.mapOutputRecords = mapOutputRecords.split(",");
		this.physicalMemoryBytes = physicalMemoryBytes.split(",");
		this.reduceInputGroups = reduceInputGroups.split(",");
		this.reduceInputRecords = reduceInputRecords.split(",");
		this.reduceOuputRecords = reduceOuputRecords.split(",");
		this.reduceShuffleBytes = reduceShuffleBytes.split(",");
		this.splitRawBytes = splitRawBytes.split(",");
		this.spilledRecords = spilledRecords.split(",");
		this.virtualMemoryBytes = virtualMemoryBytes.split(",");
		this.taskTime = taskTime.split(",");
		this.taskIds = taskIds.split(",");
		
		String[] rules = {
				new MapTimeRule(this.taskTime, this.taskIds).getRule()
		};
		this.rules = rules;
	}
	
	public String[] returnAllRules() {
		return rules;
	}
}