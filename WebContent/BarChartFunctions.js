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

// CREATES A GRAPH USING dataIn AT POSITIONS posx, posy WITH DIMENSIONS width, height WITH AXISBUFFER axisBuffer AND TITLE title
function drawGraph(dataIn, posx, posy, width, height, axisBuffer, divTag, title) {
	
	// SPLIT THE INPUT DATA BY COMMAS
	var dataValue = new String(dataIn).split(",");
	
	dataValue.splice(dataValue.length-1, 1);
	
	var graphBuffer = 25;
	
	// DEFINE THE HEIGHT OF THE VALUE DISPLAY BOX
	var heightT = 100;

	// DEFINE THE WIDTH AND THE HEIGHT OF THE GRAPH
	var widthG = width + axisBuffer;
	var heightG = height + heightT;

	var start;
	var end;

	// DEFINE THE RANGE OF THE Y AXIS
	var max = Math.max.apply(Math, dataValue);
	var min = Math.min.apply(Math, dataValue);

	// LOGIC TO MAKE AXIS DIMENSIONS
	if ( max == 0 && min == 0 ) {
		start = 0;
		end = 10;
	} else {
		start = 0;
		end = max;
	}

	// DEFINE THE WIDTH AND HEIGHT OF THE BARS
	var widthB = (widthG - axisBuffer) / dataValue.length;
	var heightB = d3.scale.linear()
	.domain([start, end])
	.range([0, heightG - heightT]);

	// DEFINE THE NUMBER OF TICKS ON THE AXIS
	var numTicks = 5;
	var text;
	
	// CREATES THE BODY FOR THE GRAPH
	var chart = d3.select("body").append("svg")
	.attr("class", "chart")
	.attr("width", widthG)
	.attr("height", heightG + graphBuffer)
	.append("g")
	.attr("transform", "translate(" + (posx + axisBuffer) + "," + posy + ")");
	
	// CREATES THE RECTANGLES IN THE GRAPH
	chart.selectAll("rect")
	.data(dataValue)
	.enter().append("svg:rect")
	.attr("x", function(d, i) { return i * widthB; })
	.attr("y", function(d) { return heightG - heightB(d); })
	.attr("width", widthB)
	.attr("height", function(d) { return heightB(d); })
	.attr("fill", function(d, i) {
		if ( dataTask[i].indexOf("r_") != -1 ) {
			return "LightSlateGray";
		} else {
			return "steelblue";
		}
	})
	
	// CHANGE VALUES ON MOUSEOVER
	.on("mouseover", function(d, i) { 

		d3.select(this).style("fill", "black");

		// REMOVE PREVIOUS VALUES
		chart.selectAll(".valueDisp").remove();
		chart.selectAll(".taskDisp").remove();

		// DISPLAYS THE VALUE NUMBER ON MOUSEOVER
		chart.selectAll(".valueDisp")
		.data([d])
		.enter().append("text")
		.attr("class", "valueDisp")
		.attr("x", 50)
		.attr("y", 50)
		.style("fill", "black")
		.text(function (d) { if ( title.indexOf("Time") != -1 ) { return numberWithCommas(d) + " seconds"; } else { return numberWithCommas(d); } });

		// DISPLAYS THE TASK ID VALUE ON MOUSEOVER
		chart.selectAll(".taskDisp")
		.data([dataTask[i]])
		.enter().append("text")
		.attr("class", "taskDisp")
		.attr("x", 55)
		.attr("y", 75)
		.text(dataTask[i]);

	})

	// REMOVES THE VALUE ON MOUSEOUT
	.on("mouseout", function(d, i) {
		if ( dataTask[i].indexOf("r_") != -1 ) {
			d3.select(this).style("fill", "LightSlateGray");
		} else { 
			d3.select(this).style("fill", "steelblue"); } });
	
	// CREATE THE Y AXIS DIVISION LINES
	chart.selectAll(".yLines")
	.data(heightB.ticks(numTicks))
	.enter().append("line")
	.attr("class", "yLines")
	.attr("x1", 0)
	.attr("x2", widthG)
	.attr("y1", function(d) { return heightG - heightB(d); })
	.attr("y2", function(d) { return heightG - heightB(d); })
	.style("stroke", "#ccc");

	// CREATE THE Y AXIS VALUES
	chart.selectAll(".axisDisp")
	.data(heightB.ticks(numTicks))
	.enter().append("text")
	.attr("class", "axisDisp")
	.attr("x", 0)
	.attr("y", function(d) { return heightG - heightB(d); })
	.attr("dx", -5)
	.attr("text-anchor", "end")
	.text(String);
	
	// DISPLAYS THE TEXT LABELS
	insertText(0, 20, "title", title, chart);
	insertText(7.5, 50, "valueText", "Value:", chart);
	insertText(7.5, 75, "taskidText", "TaskId:", chart);
	insertText(50, 50, "valueDisp", "Rollover graph to display value!", chart);
	insertText(55, 75, "taskDisp", "Rollover graph to display value!", chart);
	
	// CREATE THE LINES FOR THE VALUE DISPLAY
	drawLine(0, widthG, 30, 30, "black", "boxLines1", chart);
	drawLine(0, widthG, heightT - 15, heightT - 15, "black", "boxLines2", chart);
	drawLine(0, 0, 30, heightT - 15, "black", "boxLines3", chart);
	drawLine(widthG - posx - axisBuffer - 1, widthG - posx - axisBuffer - 1, 30, heightT - 15, "black", "boxLines4", chart);
	
	// ADD IN THE STATS BUTTON TO DISPLAY MEAN AND STANDARD DEVIATION
	chart.append("foreignObject")
    .attr("x", width-55)
    .attr("y", 0)
	.attr("width", 55)
    .attr("height", 27.5)
    .append("xhtml:body")
    .html("<a href='#' class='btn btn-primary' rel='popover' data-original-title='Map and Reduce Statistics'>Stats</a>");

	var mapValues = [];
	var reduceValues = [];
	
	for ( var i = 0; i < dataTask.length; i++ ) {
		if ( dataTask[i].indexOf("r") != -1 ) {
			reduceValues.push(dataValue[i]);
		} else {
			mapValues.push(dataValue[i]);
		}
	}
	
	// CALCULATIONS FOR AVERAGES AND STANDARD DEVIATION
	var stats = "Map Average: " + numberWithCommas(round(mean(mapValues))) + "<br>" + "Map Standard Deviation: " + numberWithCommas(round(stdev(mapValues))) + "<br>" + "Reduce Average: " + numberWithCommas(round(mean(reduceValues))) + "<br>" + "Reduce Standard Deviation: " + numberWithCommas(round(stdev(reduceValues))); 
	
	if ( title.indexOf("Map") != -1 ) {
		stats = "Map Average: " + numberWithCommas(round(mean(mapValues))) + "<br>" + "Map Standard Deviation: " + numberWithCommas(round(stdev(mapValues)));
	}
	if ( title.indexOf("Reduce") != -1 ) {
		stats = "Reduce Average: " + numberWithCommas(round(mean(reduceValues))) + "<br>" + "Reduce Standard Deviation: " + numberWithCommas(round(stdev(reduceValues)));
	}
	
	$(".btn").popover({content: stats, placement: "left"});
}

// INSERTS AT LINE AT COORDINATES X1 X2 Y1 Y2
function drawLine(x1, x2, y1, y2, color, name, chart) {
	chart.selectAll("." + name)
	.data([1])
	.enter().append("line")
	.attr("class", name)
	.attr("x1", x1)
	.attr("x2", x2)
	.attr("y1", y1)
	.attr("y2", y2)
	.style("stroke", color);
}

// INSERTS TEXT AS POSITION (X, Y)
function insertText(x, y, name, text, chart) {
	chart.selectAll("." + name)
	.data([1])
	.enter().append("text")
	.attr("class", name)
	.attr("x", x)
	.attr("y", y)
	.text(text);
}

// CALCULATES THE MEAN OF SOME GIVEN DATA
function mean(dataValue) {
	var sum = 0;
	for ( var i = 0; i < dataValue.length-1; i++ ) {
		sum += parseFloat(dataValue[i], "10");
	}
	return (sum/dataValue.length-1);
}

// CALCULATES THE STANDARD DEVIATION OF SOME GIVEN DATA
function stdev(dataValue) {
	var stdDev = Math.sqrt(getVariance(dataValue));
	return stdDev;
}

// CALCULTES THE VARIANCE OF SOME GIVEN DATA
function getVariance(dataValue) {
	var avg = mean(dataValue);
	var i = dataValue.length;
	var v = 0;
	while ( i-- ) {
		v += Math.pow((dataValue[i] - avg), 2);
	}
	v /= dataValue.length;
	return v;
}

// ROUNDS A NUMBERS
function round(num) {
	return Math.round(num*1000) / 1000;
}

// FORMATS A NUMBER SO IT IS COMMA SEPARATED
function numberWithCommas(x) {
    return x.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}