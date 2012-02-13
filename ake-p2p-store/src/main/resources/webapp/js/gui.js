/**
* Real-World G-Lab P2P Visualisierung
* Created by Richard Mietz
* mietz@iti.uni-luebeck.de
**/

/**
* Entry point
*/
window.onload = function() {  
	constants();
	objects();

	bar = drawBar(150,300,750,40,0,"2^160","Distribution of triples");
	
	putBar = drawBar(150,500,750,20,"","","");
	getBar = drawBar(150,520,750,20,0,"2^160","Distribution of operations");
	paper.text(135,510,"Put").attr(textAttr);
	paper.text(135,530,"Get").attr(textAttr);
		
	//test2();
	
	startWebSocket();
	setInterval ( "drawStatus()", 1000 );
}

function test2()
{
	peer = { 
			"type" : "status",
			"id" : "host1",
            "min"  : 0,
            "max"  : Math.pow(2,159),
			"triples" : 123,
			"redirects" : 20,
			"gets" : 203,
			"puts" : 56
		};
	peer2 = { 
			"type" : "status",
			"id" : "host2",
            "min"  : Math.pow(2,159),
            "max"  : Math.pow(2,160)-Math.pow(2,159)/2,
			"triples" : 312,
			"redirects" : 20,
			"gets" : 303,
			"puts" : 156
		};
	peer3 = { 
			"type" : "status",
			"id" : "host3",
            "min"  : Math.pow(2,160)-Math.pow(2,159)/2,
            "max"  : Math.pow(2,160),
			"triples" : 252,
			"redirects" : 20,
			"gets" : 33,
			"puts" : 235
		};
	processStatusMessage(peer);
	processStatusMessage(peer2);
	processStatusMessage(peer3);
	
	updMessage = {
			"type" : "update",
			"id" : "host3",
			"hash" : getRandom(0,maxBar),
			"oldvalue" :  "example1",	
			"value" :  "example2"
		};
		getMessage = {
			"type" : "get",
			"id" : "host4",
			"hash" : getRandom(0,maxBar),
			"response" :  ["example1",	"example2"]
		};
	processOpMessage(updMessage);
	processOpMessage(getMessage);
}

function test() {
	window.setTimeout("addRandomPeer()",1000);
	window.setTimeout("addRandomPeer()",2000);
	window.setTimeout("addRandomPeer()",3000);
	window.setTimeout("addRandomPeer()",4000);
	window.setTimeout("addRandomPeer()",4500);
	window.setTimeout("addRandomPeer()",5000);
	
	window.setTimeout("addRandomMessage()",getRandom(3000,6000));
	window.setTimeout("addRandomMessage()",getRandom(3000,6000));
	window.setTimeout("addRandomMessage()",getRandom(3000,6000));
	window.setTimeout("addRandomMessage()",getRandom(3000,6000));
	window.setTimeout("addRandomMessage()",getRandom(3000,6000));
	window.setTimeout("addRandomMessage()",getRandom(3000,6000));
}

/*
* Initialization
*
*/
function objects() {
	
    paper = new Raphael("holder", 1000, 800);

	chart = paper.set();	
	opChart = paper.set();	
	peers = paper.set();
	peersOp = paper.set();
	
	peersJSON = new Array();
}

function constants() {
	textAttr = {fill: "white", "font-size": "12"};
	maxBar = Math.pow(2,160);
	maxColor = "#8B0000";
	mediumColor = "#FF8B00";
	lowColor = "#008000";
	
	delColor = "90-red-darkred";
	insColor = "90-grey-darkgrey";
	updColor = "90-darkgrey-darkorange";
	retrieveColor = "90-green-darkgreen";
	
	wsUri = "ws://rwglab.itm.uni-luebeck.de:8881/websocket";
}

/**
* Web socket functions
*
*/

function startWebSocket() { 
	websocket = new WebSocket(wsUri); 
	websocket.onopen = function(evt) { onOpen(evt) }; 
	websocket.onclose = function(evt) { onClose(evt) }; 
	websocket.onmessage = function(evt) { onMessage(evt) }; 
	websocket.onerror = function(evt) { onError(evt) }; 	
}  

function onOpen(evt) { 
	showMessage("Info","WebSocket connection established");
}  

function onClose(evt) { 
	showMessage("Info","WebSocket connection closed");
}  

function onMessage(evt) { 
	var json = JSON.parse(evt.data);
	if(json.type == "status")
	{
		processStatusMessage(json);
	}
	else
	{
		processOpMessage(json);
	}
}  

function onError(evt) { 
	showMessage("Info","Error with WebSocket");
}  

/*
* Test functions
*
*/
function addRandomPeer()
{
	var min = getRandom(0,maxBar);
	var max = getRandom(maxBar/2,maxBar);
	var s  = "hostname1"
	if(getRandom(0,1)==1) { s = "hostname2" }
	while(max<= min)
	{ max = getRandom(maxBar/2,maxBar); }
	peer = { 
			"type" : "status",
			"id" : s,
            "min"  : min,
            "max"  : max,
			"triples" : getRandom(0,20000),
			"redirects" : 20,
			"gets" : getRandom(0,1000),
			"puts" : getRandom(0,100)
		};
	processStatusMessage(peer);
}

function addRandomMessage()
{
	insMessage = {
		"type" : "insert",
		"id" : "host1",
		"hash" : getRandom(0,maxBar),
		"value" :  "example1"	
	};
	delMessage = {
		"type" : "delete",
		"id" : "host2",
		"hash" : getRandom(0,maxBar),
		"value" :  "example1"	
	};
	updMessage = {
		"type" : "update",
		"id" : "host3",
		"hash" : getRandom(0,maxBar),
		"oldvalue" :  "example1",	
		"value" :  "example2"
	};
	getMessage = {
		"type" : "get",
		"id" : "host4",
		"hash" : getRandom(0,maxBar),
		"response" :  ["example1",	"example2"]
	};
	processOpMessage(insMessage);
}


function getRandom(min, max) {
 if(min > max) {  return -1; }
 if(min == max) {  return min; } 
 var r; 
 do {  r = Math.random(); }
 while(r == 1.0);
 return min + r * (max-min+1);
}

function showMessage(title, text) {
	var anim = Raphael.animation({opacity: 0,"fill-opacity": 0}, 1000, "<>",function(){this.remove()});
	var x = 450;
	var y = 250;
	desc = paper.text(x+10,y+10, text);
	var w = desc.getBBox().width+10;
	var h = desc.getBBox().height+10;
	paper.rect(x-w/2,y-h/2,w+40,h+20,10).attr({fill: "white"}).animate(anim.delay(1000));
	paper.path("M" + (x+50-w/2) + "," + (y-h/2) + " h" + (w-20) + "s10,0 10,10 v5h-" + (w+40) + "v-5s0,-10 10,-10").attr({fill: "black"}).animate(anim.delay(1000));
	paper.text(x+25-w/2,y+5-h/2, title).attr({fill: "white"}).animate(anim.delay(1000));
	desc.toFront().animate(anim.delay(1000));	
}

/*
* Processes an operation message from a peer, i.e. draws an arrow representing the operation
*/
function processOpMessage(message) {
	var incoming = true;
	var t = "";
	var bubbleColor = "black";
	switch(message.type)
	{
		case "insert":
			bubbleColor = insColor;
			t = "Insert:\n" + message.value;
			break;
		case "delete":
			bubbleColor = delColor;
			t = "Delete:\n" + message.value;
			break;
		case "update":
			bubbleColor = updColor;
			t = "Update:\n" + message.oldvalue + " -> " + message.value;
			break;
		case "get":
			bubbleColor = retrieveColor;
			incoming = false;
			t = "Retrieve:\nFound " + message.response.length + " triples";
			break;
	}
	drawArrow(incoming, bubbleColor, message.id, message.hash, t);
	//t = message.request[0] + "\n" + message.request[1] + "\n" + message.request[2];
}


/*
* Processes an info message from a peer, i.e. deletes old infos and redraws elements
*/
function processStatusMessage(message)
{
	message.timestamp = new Date().getMilliseconds();
	deletePeer(message.id);
	peersJSON.push(message);
}

function drawStatus()
{
	deleteOldPeers();
	drawPeers();
	drawChart(930,470,20,100,0,getMaxOpAtPeer(),opChart);
	drawChart(930,270,20,100,0,getMaxTriplesAtPeer(),chart);
}

/*
* Deletes the information of the peer with the given id if exists
*/
function deletePeer(id) {
	for(var i = 0;i<peersJSON.length;i++)
	{
		if(peersJSON[i].id==id)
		{
			peersJSON.splice(i,1);
		}
	}
}

/**
* Draws a single peer
**/
function drawPeer(id, min, max, triples) {
	var x = (bar.getBBox(false).width/maxBar)*min + bar.getBBox(false).x;
	var width = ((bar.getBBox(false).width/maxBar)*max + bar.getBBox(false).x) - x;
	peers.push(
		paper.rect(x,bar.getBBox(false).y,width,bar.getBBox(false).height).attr({fill: "90-"+getShadeColor(triples, getMaxTriplesAtPeer())+"-"+getColor(triples, getMaxTriplesAtPeer()), title: id + ": " + triples + " triples", "stroke-width": 0.3}).data("id",id)		
		//paper.text(x+width/2,bar.getBBox(false).y + bar.getBBox(false).height/2,id).attr({fill: "white"}).attr("font-size", "12")
	)	
}

/**
* Draws all peers
**/
function drawPeers() {
	peers.remove();
	peers.clear();
	
	peersOp.remove();
	peersOp.clear();
	
	for(var i = 0;i<peersJSON.length;i++)
	{
		if(peersJSON[i].min>peersJSON[i].max)
		{
			drawPeer(peersJSON[i].id,peersJSON[i].min,maxBar,peersJSON[i].triples);
			drawPeer(peersJSON[i].id,0,peersJSON[i].max,peersJSON[i].triples);
			drawPeerOp(peersJSON[i].id,peersJSON[i].max,maxBar,peersJSON[i].puts,peersJSON[i].gets);
			drawPeerOp(peersJSON[i].id,0,peersJSON[i].min,peersJSON[i].puts,peersJSON[i].gets);
		}
		else
		{
			drawPeer(peersJSON[i].id,peersJSON[i].min,peersJSON[i].max,peersJSON[i].triples);
			drawPeerOp(peersJSON[i].id,peersJSON[i].min,peersJSON[i].max,peersJSON[i].puts,peersJSON[i].gets);
		}
	}
}

function deleteOldPeers() {
	for(var i = 0;i<peersJSON.length;i++)
	{
		if((peersJSON[i].timestamp + 5000) < new Date().getMilliseconds())
		{
			peersJSON.splice(i,1);
		}
	}
}

/**
* Draws a single peer operation information
**/
function drawPeerOp(id, min, max, put, get) {
	var putX = (putBar.getBBox(false).width/maxBar)*min + putBar.getBBox(false).x;
	var getX = (getBar.getBBox(false).width/maxBar)*min + getBar.getBBox(false).x;
	var putWidth = ((putBar.getBBox(false).width/maxBar)*max + putBar.getBBox(false).x) - putX;
	var getWidth = ((getBar.getBBox(false).width/maxBar)*max + getBar.getBBox(false).x) - getX;
	peersOp.push(
		paper.rect(putX,putBar.getBBox(false).y,putWidth,putBar.getBBox(false).height).attr({fill: "90-"+getShadeColor(put, getMaxOpAtPeer())+"-"+getColor(put, getMaxOpAtPeer()), title: id + ": " + put + " put operations", "stroke-width": 0.3}).data("id",id),
		paper.rect(getX,getBar.getBBox(false).y,getWidth,getBar.getBBox(false).height).attr({fill: "90-"+getShadeColor(get, getMaxOpAtPeer())+"-"+getColor(get, getMaxOpAtPeer()), title: id + ": " + get + " get operations", "stroke-width": 0.3}).data("id",id)		
		//paper.text(x+width/2,bar.getBBox(false).y + bar.getBBox(false).height/2,id).attr({fill: "white"}).attr("font-size", "12")
	)	
}

/**
* Calculates the color for the peer depending on the number of triples the peer holds
**/
function getColor(value, max) {
	var percent = (100/max) *value;
	if(percent>50)
	{
		return calc_color(value,mediumColor,maxColor,max/2,max);
	}
	else
	{
		return calc_color(value,lowColor,mediumColor,0,max/2);
	}	
}

/**
* Calculates the shaded color for the peer depending on the number of triples the peer holds
**/
function getShadeColor(value,max) {
	var percent = (100/max) *value;
	var col;
	if(percent>50)
	{
		col = calc_color(value,mediumColor,maxColor,max/2,max);
	}
	else
	{
		col = calc_color(value,lowColor,mediumColor,0,max/2);
	}	
	var rgb = Raphael.getRGB(col);
	var hsb = Raphael.rgb2hsb(rgb.r,rgb.g,rgb.b);
	return Raphael.hsb2rgb(hsb.h,hsb.s,hsb.b-0.2).hex;
}

/**
* Calculates the color between to given colors and with a given number from a given interval
**/
function calc_color(value, start, end, min, max) {
    var n = (value - min) / (max - min);
    var s = parseInt(start.replace("#", ""), 16);
    var e = parseInt(end.replace("#", ""), 16);

    var r = Math.round(((e >> 16) - (s >> 16)) * n) + (s >> 16);
    var g = Math.round((((e >> 8) & 0xFF) - ((s >> 8) & 0xFF)) * n) + ((s >> 8) & 0xFF);
    var b = Math.round(((e & 0xFF) - (s & 0xFF)) * n) + (s & 0xFF);
    b |= (r << 16) | (g << 8);

    return "#" + ("000000" + b.toString(16)).slice(-6);
}

/**
* Determines the maximum number of triples stored by a single peer
**/
function getMaxTriplesAtPeer() {
	var max = 0;
	for(var i = 0;i<peersJSON.length;i++)
	{
		if(peersJSON[i].triples>max)
		{
			max = peersJSON[i].triples;
		}
	}
	return max;
}

/**
* Determines the maximum number of get/put operation executed by a peer
**/
function getMaxOpAtPeer() {
	var max = 0;
	for(var i = 0;i<peersJSON.length;i++)
	{
		if(peersJSON[i].puts>max)
		{
			max = peersJSON[i].puts;
		}
		if(peersJSON[i].gets>max)
		{
			max = peersJSON[i].gets;
		}
	}
	return max;
}

/**
 * Draws the bar for the "zahlenstrahl"
 **/
function drawBar(x,y,width,height,min,max,title) {
	var bar = paper.rect(x, y, width, height).attr({fill: "90-grey-white"}).toBack();
	paper.text(x,y+height+20,min).attr(textAttr);
	paper.text(x-75,y-50,title).attr(textAttr);
	if(max.indexOf("^")>=0)
	{
		parts = max.split("^");
		paper.text(x+width,y+height+20,parts[0]).attr(textAttr);
		paper.text(x+width+parts[0].length*4+10,y+height+10,parts[1]).attr({fill: "white", "font-size": "10"});
	}
	else
	{
		paper.text(x+width,y+height+20,max).attr(textAttr);
	}
	return bar;
}
 
/**
 * Draws a chart to visualize the density color distribution
 **/
function drawChart(x,y,width, height, min, max, chartSet) {
	chartSet.remove();
	chartSet.clear();
	chartSet.push(
		paper.rect(x,y,width,height).attr({fill: "90-"+lowColor+"-"+mediumColor+"-"+maxColor}),
		paper.text(x+width+20,y,max).attr(textAttr),
		paper.text(x+width+10,y+height,min).attr(textAttr)
	)
}

/**
 * Draws an arrow at the given position on the "zahlenstrahl"
 *	@incoming Determines the position of the arrow relative to the given pos
 **/
function drawArrow(incoming,bubbleColor,id,pos,text) {
	var y = bar.getBBox(false).y + bar.getBBox(false).height + 80;
	if(incoming){y -= bar.getBBox(false).height + 80;}
	var x =  (bar.getBBox(false).width/maxBar)*pos + bar.getBBox(false).x;
	drawArrowDeep(incoming,bubbleColor,id,x,y, text);
}

/**
 * Draws an arrow at the given x,y coordinates
 **/
function drawArrowDeep(incoming,bubbleColor,id,x,y,text) {
	//var anim = Raphael.animation({transform: "t0,80",opacity: 0,"fill-opacity": 0}, 1000, "<>",function(){this.remove()});
	var anim = Raphael.animation({opacity: 0,"fill-opacity": 0}, 1000, "<>",function(){this.remove()});
	//var textAnim = Raphael.animation({transform: "R90",opacity: 0}, 1000, "<>",function(){this.remove()});
	var textAnim = Raphael.animation({opacity: 0}, 1000, "<>",function(){this.remove()});
	var bubble;
	var text;
	if(incoming)
	{
		text = paper.text(x,y-110,text).attr("font-size", "12").animate(anim.delay(1000));
		bubble = drawInBubble(x,y-80,text.getBBox().width+10,text.getBBox().height+10,text).attr({fill: bubbleColor}).animate(anim.delay(1000)).toBack();
	}
	else
	{
		text = paper.text(x,y+25,text).attr("font-size", "12").animate(anim.delay(1000));
		bubble= drawOutBubble(x,y,text.getBBox().width+10,text.getBBox().height,text).attr({fill: bubbleColor}).animate(anim.delay(1000)).toBack();		
	}
	paper.path("M"+x+","+y+" l"+(-10)+","+(-10)+"H"+x+"v"+(-70)+"v"+(70)+"h"+10+"Z").attr({fill: "black"}).animate(anim.delay(1000));
	//paper.text(x+10,y-60,id).transform("r90").attr({fill: "white", "font-size": "12"}).animate(textAnim.delay(1000));
}

/**
 * Draws an ingoing bubble with a minimum size
 **/
function drawInBubble(x,y,width,height,text) {
	if(width<=40) width = 40;
	if(height<=20) height = 20;
	return paper.path("M"+x+","+y+" s0,-10 -10,-10h-"+(width/2-20)+"s-10,0 -10,-10v-"+(height-20)+"s0,-10 10,-10 h"+(width-20)+"s10,0 10,10v"+(height-20)+"s0,10 -10,10h-"+(width/2-20)+"s-10,0 -10,10");
 }
 
 /**
 * Draws an outgoing bubble with a minimum size
 **/
 function drawOutBubble(x,y,width,height,text) {
	if(width<=40) width = 40;
	if(height<=20) height = 20;
	return paper.path("M"+x+","+y+" s0,10 -10,10h-"+(width/2-20)+"s-10,0 -10,10v"+(height-20)+"s0,10 10,10 h"+(width-20)+"s10,0 10,-10v-"+(height-20)+"s0,-10 -10,-10h-"+(width/2-20)+"s-10,0 -10,-10");
 }

