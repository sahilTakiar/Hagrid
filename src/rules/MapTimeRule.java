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

/**
 * A rule that checks how fast each map task is running and tells the user to change mapred.min/max.split.size accordingly
 */
public class MapTimeRule implements BaseRule {

	private String[] taskIds;
	private String[] taskTime;
	
	public MapTimeRule(String[] taskTime, String[] taskIds) {
		this.taskIds = taskIds;
		this.taskTime = taskTime;
	}
	
	@Override
	public String getRule() {
		double numLow = 0, numHigh = 0, numIdeal = 0;
		int totalMaps = 0;
		
		for ( int i = 0; i < taskIds.length; i++ ) {
			if ( taskIds[i].contains("m") ) { totalMaps++; }
		}

		for ( int i = 0; i < totalMaps; i++ ) {
			long value = Integer.parseInt(taskTime[i]);
			if ( value < (360000) && value > (240000) ) {
				numIdeal++;
			}
			if ( value < (2400000) ) {
				numLow++;
			}
			if ( value > (360000) ) {
				numHigh++;
			}
		}

		String feedback = "Maps: " + (numIdeal/totalMaps*100) + "% have an ideal time, " + (numLow/totalMaps*100) + "% are too slow, " + (numHigh/totalMaps*100) + "% are too long.";
		String advice = " Consider changing mapred.min.split.size or mapred.max.split.size to change the split size / number of maps.";
		
		if ( numLow/totalMaps > 0.25 ) {
			advice = " Consider using mapred.min.split.size or mapred.max.split.size to decrease the number of maps.";
		}

		if ( numHigh/totalMaps > 0.25 ) {
			advice = " Consider using mapred.min.split.size or mapred.max.split.size to increase the number of maps";
		}
				
		feedback = feedback + advice;
		return feedback;
	}
}