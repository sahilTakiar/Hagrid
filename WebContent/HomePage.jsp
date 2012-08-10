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
<title>HAGRID - Hadoop Grid Usage Monitor</title>

<!-- IMPORT CSS AND JAVASCRIPT -->
<link href="CSS/bootstrap.css" rel="stylesheet" type="text/css" />
<link href="CSS/jquery-ui-1.8.21.custom.css" rel="stylesheet" type="text/css" />
<script src="Javascript/jquery-1.7.2.min.js" type="text/javascript"></script>
<script src="Javascript/jquery-ui-1.8.21.custom.min.js" type="text/javascript"></script>

</head>

<body>
	<h1 style="margin: 25px;" align="center">HAGRID: The Hadoop Grid Monitoring Tool</h1>
	<div style="margin: 25px;">
		<div class="page-header">
			<h2>Overview of HAGRID</h2>
		</div>
		<div class="well">HAGRID is a visualization tool used to display
			task and job level parameters from the log files generated from each
			Hadoop job. The tool displays graphs of important metrics such as
			task time, spilled records, combiner input and output, etc. To use
			HAGRID simply enter the Job ID of your Hadoop job into the form and
			press submit.
		</div>
	</div>

	<div style="margin: 25px;">
		<div class="page-header">
			<h2>HAGRID Commands</h2>
		</div>
	</div>

	<form method="POST" action="TaskServlet" style="margin: 25px;">
		<h4>Enter Job ID: </h4>
		<input type="text" name="jobId"/><br/>
		<input class="btn btn-primary" type="submit" value="Submit" />
	</form>
	
</body>
</html>