<!--
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
-->

<html>
<head>
<title>HAGRID</title>

<!-- IMPORT STYLESHEETS -->
<link href="CSS/bootstrap.css" rel="stylesheet" type="text/css" />

</head>

<!-- IMPORT THE JQUERY AND D3.JS LIBRARIES -->
<script src="Javascript/d3.v2.min.js"></script>
<script src="Javascript/jquery-1.7.2.min.js"></script>
<script src="Javascript/bootstrap.min.js"></script>

<!-- IMPORT EXTERNAL JS FILES -->
<script src="BarChartFunctions.js"></script>

<style>

/* STYLE FOR THE RECTANGLES IN THE GRAPH */
.chart rect {
  padding-top: 25px;
}

/* STYLE FOR THE TEXT IN THE GRAPH */
.chart text {
  shape-rendering: crispEdges;
  fill: black;
  font-size: 13px;
}

/* STYLE ADVICE TEXT */
.adviceText {
  padding: 0px;
  margin: 0px;
}

</style>

<!-- CREATE TABLES FOR JOB PARAMETERS AND ADVICE SECTION -->
<body>
	<div class="container-fluid">
 		<h2 style="text-align:center">Job Parameters</h2>
		<div class="accordion" id="accordion2">
			<div class="accordion-group">
				<div class="accordion-heading">
					<a class="accordion-toggle" data-toggle="collapse" data-parent="#accordion2" href="#collapseOne">Expand</a>
				</div>
				<div id="collapseOne" class="accordion-body in collapse" style="height: 0px;">
					<div class="accordion-inner">
						<table class="table table-bordered table-striped" style="font-size: 12.5px;">
							<thead>
								<tr>
									<th colspan="7" style="text-align: center;">Job Level Parameters and Totals</th>
								</tr>
							</thead>
							<tbody>
								<tr id="tr1"></tr>
								<tr id="tr2"></tr>
								<tr id="tr3"></tr>
								<tr id="tr4"></tr>
								<tr id="tr5"></tr>
							</tbody>
						</table>
					</div>
				</div>
			</div>
		</div>
	</div>

	<div class="container-fluid">
		<h2 style="text-align:center">Advice</h2>
		<div class="accordion" id="accordion3">
			<div class="accordion-group">
				<div class="accordion-heading">
					<a class="accordion-toggle" data-toggle="collapse" data-parent="#accordion3" href="#collapseTwo">Expand</a>
				</div>
				<div id="collapseTwo" class="accordion-body collapse" style="height: 0px;">
					<div class="accordion-inner">
						<p class="adviceText" id="mapTaskTimeAdvice">Sorry could not provide any feedback :(</p>
					</div>
				</div>
			</div>
		</div>
	</div>

<!-- POPULATE THE ENTRIES OF THE TABLE -->
<script>
	// LOAD JOB PARAMETERS
	data = new String('${JobParams}').split(",");
	
	$('<td>' + data[0] + '</td>').appendTo("#tr1");
	$('<td>' + data[1] + '</td>').appendTo("#tr1");
	$('<td>' + data[2] + '</td>').appendTo("#tr1");
	$('<td>' + data[3] + '</td>').appendTo("#tr1");
	$('<td>' + data[4] + '</td>').appendTo("#tr1");
	$('<td>' + data[5] + '</td>').appendTo("#tr1");
	$('<td>' + data[6] + '</td>').appendTo("#tr1");
	$('<td>' + data[7] + '</td>').appendTo("#tr2");
	$('<td>' + data[8] + '</td>').appendTo("#tr2");
	$('<td>' + data[9] + '</td>').appendTo("#tr2");
	$('<td>' + data[10] + '</td>').appendTo("#tr2");
	$('<td>' + data[11] + '</td>').appendTo("#tr2");
	$('<td>' + data[12] + '</td>').appendTo("#tr2");
	$('<td>' + data[13] + '</td>').appendTo("#tr2");
	$('<td>' + data[14] + '</td>').appendTo("#tr3");
	$('<td>' + data[15] + '</td>').appendTo("#tr3");
	$('<td>' + data[16] + '</td>').appendTo("#tr3");
	$('<td>' + data[17] + '</td>').appendTo("#tr3");
	$('<td>' + data[18] + '</td>').appendTo("#tr3");
	$('<td>' + data[19] + '</td>').appendTo("#tr3");
	$('<td>' + data[20] + '</td>').appendTo("#tr3");
	$('<td>' + data[21] + '</td>').appendTo("#tr4");
	$('<td>' + data[22] + '</td>').appendTo("#tr4");
	$('<td>' + data[23] + '</td>').appendTo("#tr4");
	$('<td>' + data[24] + '</td>').appendTo("#tr4");
	$('<td>' + data[25] + '</td>').appendTo("#tr4");
	$('<td>' + data[26] + '</td>').appendTo("#tr4");
	$('<td>' + data[27] + '</td>').appendTo("#tr4");	
	$('<td>' + data[28] + '</td>').appendTo("#tr5");
	$('<td>' + data[29] + '</td>').appendTo("#tr5");
	$('<td>' + data[30] + '</td>').appendTo("#tr5");
	$('<td>' + data[31] + '</td>').appendTo("#tr5");
	$('<td>' + data[32] + '</td>').appendTo("#tr5");
	$('<td></td>').appendTo("#tr5");
	$('<td></td>').appendTo("#tr5");
</script>

<script>

// WIDTH AND HEIGHT OF ALL GRAPHS
var allWidth = 260;
var allHeight = 120;
var barType = "TaskID:";

/**************************************************************************************/
/* CALL THE FUNCTION DRAWGRAPH WITH APPROPRIATE PARAMETERS FOR EACH GRAPH TO BE MADE */
/*************************************************************************************/

var dataTask = new String('${TaskID}').split(",");

drawGraph('${Bytes_Read}', 0, 0, allWidth, allHeight, 100, null, "Bytes Read");
drawGraph('${Bytes_Written}', 0, 0, allWidth, allHeight, 100, null, "Bytes Written");

drawGraph('${Combine_Input_Records}', 0, 0, allWidth, allHeight, 100, null, "Combine Input Records");
drawGraph('${Combine_Output_Records}', 0, 0, allWidth, allHeight, 100, null, "Combine Output Records");

drawGraph('${Committed_Heap_Bytes}', 0, 0, allWidth, allHeight, 100, null, "Committed Heap Bytes");
drawGraph('${CPU_Milliseconds}', 0, 0, allWidth, allHeight, 100, null, "CPU Milliseconds");

drawGraph('${File_Bytes_Read}', 0, 0, allWidth, allHeight, 100, null, "File Bytes Read");
drawGraph('${File_Bytes_Written}', 0, 0, allWidth, allHeight, 100, null, "File Bytes Written");

drawGraph('${HDFS_Bytes_Read}', 0, 0, allWidth, allHeight, 100, null, "HDFS Bytes Read");
drawGraph('${HDFS_Bytes_Written}', 0, 0, allWidth, allHeight, 100, null, "HDFS Bytes Written");

drawGraph('${Map_Input_Bytes}', 0, 0, allWidth, allHeight, 100, null, "Map Input Bytes");
drawGraph('${Map_Input_Records}', 0, 0, allWidth, allHeight, 100, null, "Map Input Records");
drawGraph('${Map_Output_Bytes}', 0, 0, allWidth, allHeight, 100, null, "Map Output Bytes");
drawGraph('${Map_Output_Materialized_Bytes}', 0, 0, allWidth, allHeight, 100, null, "Map Output Materialized Bytes");
drawGraph('${Map_Output_Records}', 0, 0, allWidth, allHeight, 100, null, "Map Output Records");

drawGraph('${Physical_Memory_Bytes}', 0, 0, allWidth, allHeight, 100, null, "Physical Memory Bytes");

drawGraph('${Reduce_Input_Groups}', 0, 0, allWidth, allHeight, 100, null, "Reduce Input Groups");
drawGraph('${Reduce_Input_Records}', 0, 0, allWidth, allHeight, 100, null, "Reduce Input Records");
drawGraph('${Reduce_Output_Records}', 0, 0, allWidth, allHeight, 100, null, "Reduce Output Records");
drawGraph('${Reduce_Shuffle_Bytes}', 0, 0, allWidth, allHeight, 100, null, "Reduce Shuffle Bytes");

drawGraph('${Spilled_Records}', 0, 0, allWidth, allHeight, 100, null, "Spilled Records");
drawGraph('${Split_Raw_Bytes}', 0, 0, allWidth, allHeight, 100, null, "Split Raw Bytes");

drawGraph('${Virtual_Memory_Bytes}', 0, 0, allWidth, allHeight, 100, null, "Virtual Memory Bytes");
drawGraph('${Task_Time}', 0, 0, allWidth, allHeight, 100, null, "Task Time");

// LOAD IN THE FEEDBACK TEXT
"<% String[] data25 = (String[]) request.getAttribute("Feedback");  %>"
document.getElementById("mapTaskTimeAdvice").innerHTML = "<%= data25[0] %>";

// EXPAND BOTH JOB PARAMETERS AND ADVICE WINDOWS
$("#collapseOne").collapse("show");
$("#collapseTwo").collapse("show");

</script>
</body>
</html>