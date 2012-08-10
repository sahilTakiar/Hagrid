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

package servlets;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import rules.RuleGenerator;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;

@WebServlet("/StatsServlet")
public class TaskServlet extends HttpServlet {
	
    public TaskServlet() {
        super();
    }
    
    /**
     * Main method that processes the HTTP request and accessed the data from the database based on the specified jobid
     */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		String jobId = request.getParameter("jobId");
		
		String url = "jdbc:mysql://stakiar-ld.linkedin.biz:3306/LogStatsAppDatabase";
		String user = "stakiar";
		String pwd = "test123";
		
		String taskIds = "", jobParams = "", bytesRead = "", bytesWritten = "", combineInputRecords = "", combineOutputRecords = "", committedHeapBytes = "", cpuMilliseconds = "", fileBytesRead = "", fileBytesWritten = "", hdfsBytesRead = "", hdfsBytesWritten = "", mapInputBytes = "", mapInputRecords = "", mapOuputBytes = "", mapOutputMaterializedBytes = "", mapOutputRecords = "", physicalMemoryBytes = "", reduceInputGroups = "", reduceInputRecords = "", reduceOuputRecords = "", reduceShuffleBytes = "", splitRawBytes = "", spilledRecords = "", virtualMemoryBytes = "", taskTime = ""; 
		
		try {
			Class.forName("com.mysql.jdbc.Driver");
			Connection connection = DriverManager.getConnection(url, user, pwd);
			
			bytesRead = getData(request, connection, "Bytes_Read", jobId, "_");
			bytesWritten = getData(request, connection, "Bytes_Written", jobId, "_");
			combineInputRecords = getData(request, connection, "Combine_Input_Records", jobId, "_");
			combineOutputRecords = getData(request, connection, "Combine_Output_Records", jobId, "_");
			committedHeapBytes = getData(request, connection, "Committed_Heap_Bytes", jobId, "_");
			cpuMilliseconds = getData(request, connection, "CPU_Milliseconds", jobId, "_");
			fileBytesRead = getData(request, connection, "File_Bytes_Read", jobId, "_");
			fileBytesWritten = getData(request, connection, "File_Bytes_Written", jobId, "_");
			hdfsBytesRead = getData(request, connection, "HDFS_Bytes_Read", jobId, "_");
			hdfsBytesWritten = getData(request, connection, "HDFS_Bytes_Written", jobId, "_");
			mapInputBytes = getData(request, connection, "Map_Input_Bytes", jobId, "_");
			mapInputRecords = getData(request, connection, "Map_Input_Records", jobId, "_");
			mapOuputBytes = getData(request, connection, "Map_Output_Bytes", jobId, "_");
			mapOutputMaterializedBytes = getData(request, connection, "Map_Output_Materialized_Bytes", jobId, "_");
			mapOutputRecords = getData(request, connection, "Map_Output_Records", jobId, "_");
			physicalMemoryBytes = getData(request, connection, "Physical_Memory_Bytes", jobId, "_");
			reduceInputGroups = getData(request, connection, "Reduce_Input_Groups", jobId, "_");
			reduceInputRecords = getData(request, connection, "Reduce_Input_Records", jobId, "_");
			reduceOuputRecords = getData(request, connection, "Reduce_Output_Records", jobId, "_");
			reduceShuffleBytes = getData(request, connection, "Reduce_Shuffle_Bytes", jobId, "_");
			splitRawBytes = getData(request, connection, "Split_Raw_Bytes", jobId, "_");
			spilledRecords = getData(request, connection, "Spilled_Records", jobId, "_");
			virtualMemoryBytes = getData(request, connection, "Virtual_Memory_Bytes", jobId, "_");
			taskTime = getData(request, connection, "Task_Time", jobId, "_");
			taskIds = getTaskIds(request, connection, "TaskID", jobId, "_");
			
			jobParams = getJobParams(request, connection, jobId);
			
		} catch ( SQLException e ) {
			e.printStackTrace();
			System.err.println("Error: " + e.getMessage());
		} catch ( ClassNotFoundException e ) {
			e.printStackTrace();
			System.err.println("Error: " + e.getMessage());
		}
		
		request.setAttribute("Feedback", new RuleGenerator(taskIds, jobParams, bytesRead, bytesWritten, combineInputRecords, combineOutputRecords, committedHeapBytes, cpuMilliseconds, fileBytesRead, fileBytesWritten, hdfsBytesRead, hdfsBytesWritten, mapInputBytes, mapInputRecords, mapOuputBytes, mapOutputMaterializedBytes, mapOutputRecords, physicalMemoryBytes, reduceInputGroups, reduceInputRecords, reduceOuputRecords, reduceShuffleBytes, splitRawBytes, spilledRecords, virtualMemoryBytes, taskTime).returnAllRules());
		
		request.getRequestDispatcher("TaskGraphs.jsp").forward(request, response);
	}
	
	/**
	 * Helper method to get a column of data from the table identified by the jobId
	 * @param request
	 * @param connection
	 * @param col
	 * @param jobId
	 * @param taskType
	 * @return
	 * @throws SQLException
	 */
	private String getData(HttpServletRequest request, Connection connection, String col, String jobId, String taskType) throws SQLException {
		PreparedStatement p = connection.prepareStatement("SELECT " + col + " FROM " + jobId + " ORDER BY TaskId");
		ResultSet rs = p.executeQuery();
		
		String text = "";
		while ( rs.next() ) {
			text = text + rs.getLong(col) + ",";
		}
		request.setAttribute(col, text);
		text = text.substring(0, text.length()-1);
		return text;
	}
	
	/**
	 * Helper method to get the taskId data
	 * @param request
	 * @param connection
	 * @param col
	 * @param jobId
	 * @param taskType
	 * @return
	 * @throws SQLException
	 */
	private String getTaskIds(HttpServletRequest request, Connection connection, String col, String jobId, String taskType) throws SQLException {
		PreparedStatement p = connection.prepareStatement("SELECT " + col + " FROM " + jobId + " ORDER BY TaskId");
		ResultSet rs = p.executeQuery();
		
		String text = "";
		String id;
		int index;
		while ( rs.next() ) {
			id = rs.getString(col);
			if ( id.indexOf("_r_") == -1 ) { index = id.indexOf("_m_"); } else { index = id.indexOf("_r_"); }
			text = text + id.substring(index + 1, id.length()) + ",";
		}
		request.setAttribute(col, text);
		return text;
	}
	
	/**
	 * Helper method to get the job level parameters from the jobParameters table
	 * @param request
	 * @param connection
	 * @param jobId
	 * @return
	 * @throws SQLException
	 */
	private String getJobParams(HttpServletRequest request, Connection connection, String jobId) throws SQLException {
		PreparedStatement p = connection.prepareStatement("SELECT * FROM jobParameters WHERE JobID = \"" + jobId + "\"");
		ResultSet rs = p.executeQuery();
		ResultSetMetaData md = rs.getMetaData();
		rs.next();

		String text = "";
		for ( int i = 1; i <= md.getColumnCount(); i++ ) {
			text = text + md.getColumnName(i) + ": " + rs.getString(md.getColumnName(i)) + ",";
		}
		request.setAttribute("JobParams", text);
		return text;
	}
}