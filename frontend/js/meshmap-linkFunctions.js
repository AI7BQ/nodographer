/**
 * 
 */

function linkColorsRF_txRate(value, linkMetric) {
	// value = value / 2;
	var midValue = 30;
	if(linkMetric === "txRate") {
		midValue = 50;
	}
	if(linkMetric === "SnR") {
		midValue = 20;
	}
	var color;
	var colorParts;
	if(value > midValue) {
		colorParts = (1 - ((value - midValue) / midValue));
	}else {
		colorParts = value / midValue;
	}
	colorParts = Math.round(colorParts * 255);
	if(value < midValue) {
		colorParts = parseInt(colorParts).toString(16);
		if(colorParts.length == 1) {
			colorParts = "0" + colorParts;
		}
		color = "#FF" + colorParts + "00"; 
	}else if(value > midValue) {
		if(colorParts < 0) {
			colorParts = colorParts * -1;
		}
		colorParts = parseInt(colorParts).toString(16);
		if(colorParts.length == 1) {
			colorParts = "0" + colorParts;
		}
		color = "#" + colorParts + "FF00";
		
	}else {
		color = "#FFFF00";
	}
	return color;
}

function linkColorRF_cost(value) {
	value = parseFloat(value);
	var color = "#FF0000";
	if(value <= 1) {
		color = "#00FF00";
	}
	if(value > 1 && value <= 1.5) {
		color = "#FF7700";
	}
	if(value > 1.5 && value < 2) {
		color = "#FFFF00";
	}
	return color;
}

function linkColorRF_quality(value) {
	value = parseFloat(value);
	var color = "#FF0000";
	if(value >= 1) {
		color = "#00FF00";
	}
	if(value < 1 && value > 0.6) {
		color = "#FFFF00";
	}
	return color;
}

 function createLinks(node, list, nodeLat, nodeLon, layer, linkMetric) {
	var objArray = Object.entries(list);
	var linkColor = "#808080";
	for(var i = 0; i < objArray.length; i++) {
		if(objArray[i][1].linkType === "RF") {
			if(typeof objArray[i][1].linkLat === 'undefined' ||
				objArray[i][1].linkLat === null ||
				nodeLat === 'undefined' ||
				nodeLat === null) {
//					console.log("null values - RF");
					continue;
			}else {
				var distance = "";
				if(mapInfo['kilometers']) {
					distance = " Distance " + objArray[i][1].distanceKM + "km (" + objArray[i][1].distanceMiles + "mi)";
				}else {
					distance = " Distance " + objArray[i][1].distanceMiles + "mi (" + objArray[i][1].distanceKM + "km)";
				}

				if(linkMetric === "txRate") {
					linkColor = (linkColorsRF_txRate(parseFloat(objArray[i][1].tx_rate, linkMetric)));

					L.polyline([[parseFloat(nodeLat), parseFloat(nodeLon)],[parseFloat(objArray[i][1].linkLat), parseFloat(objArray[i][1].linkLon)]],
						{color: linkColor, opacity: 0.8, weight: 2, offset: 2}).bindPopup(
						"<div class='linkPopupContent'><strong>" + objArray[i][1].hostname + "</strong> to <strong>" + node +
						" TX rate: " + objArray[i][1].tx_rate + distance +"</div>").addTo(layer);

				}
				if(linkMetric === "Tput") {
					linkColor = (linkColorsRF_txRate(parseFloat(objArray[i][1].expected_throughput, linkMetric)));

					L.polyline([[parseFloat(nodeLat), parseFloat(nodeLon)],[parseFloat(objArray[i][1].linkLat), parseFloat(objArray[i][1].linkLon)]],
						{color: linkColor, opacity: 0.8, weight: 2, offset: 2}).bindPopup(
						"<div class='linkPopupContent'><strong>" + objArray[i][1].hostname + "</strong> to <strong>" + node +
						" Expected Throughput: " + objArray[i][1].expected_throughput + distance +"</div>").addTo(layer);

				}
				if(linkMetric === "SnR") {
					var SnR = (parseFloat(objArray[i][1].noise) - parseFloat(objArray[i][1].signal)) * -1;
//					console.log(parseFloat(SnR));
					linkColor = linkColorsRF_txRate(SnR, linkMetric);
					SnR = SnR * -1;
					L.polyline([[parseFloat(nodeLat), parseFloat(nodeLon)],[parseFloat(objArray[i][1].linkLat), parseFloat(objArray[i][1].linkLon)]],
						{color: linkColor, opacity: 0.8, weight: 2, offset: 2}).bindPopup(
						"<div class='linkPopupContent'><strong>" + objArray[i][1].hostname + "</strong> to <strong>" + node +
						" SnR:" + SnR + " sig: " + objArray[i][1].signal + " noise: " + objArray[i][1].noise + distance + "</div>").addTo(layer);
				}
				if(linkMetric === "cost") {
					linkColor = (linkColorRF_cost(objArray[i][1].linkCost));
					L.polyline([[parseFloat(nodeLat), parseFloat(nodeLon)],[parseFloat(objArray[i][1].linkLat), parseFloat(objArray[i][1].linkLon)]],
                                                {color: linkColor, opacity: 0.8, weight: 2, offset: 2}).bindPopup(
                                                "<div class='linkPopupContent'><strong>" + objArray[i][1].hostname + "</strong> to <strong>" + node +
                                                " Link Cost: " + objArray[i][1].linkCost + distance + "</div>").addTo(layer);
				}

				if(linkMetric === "qual") {
					linkColor = (linkColorRF_quality(objArray[i][1].linkQuality));
					L.polyline([[parseFloat(nodeLat), parseFloat(nodeLon)],[parseFloat(objArray[i][1].linkLat), parseFloat(objArray[i][1].linkLon)]],
                                                {color: linkColor, opacity: 0.8, weight: 2, offset: 2}).bindPopup(
                                                "<div class='linkPopupContent'><strong>" + objArray[i][1].hostname + "</strong> to <strong>" + node +
                                                " Link Quality: " + objArray[i][1].linkQuality + distance + "</div>").addTo(layer);
				}

/*
				if(objArray[i][1].tx_rate >= "40") {
					linkColor = "#00FF00";
				}
				if(objArray[i][1].tx_rate < "40" && objArray[i][1].tx_rate > "11") {
					var val = parseInt(objArray[i][1].tx_rate, 16);
					linkColor = "#" + val + "FF00";
				}
				if(objArray[i][1].tx_rate < "11") {
					linkColor = "#FF0000";
				}
*/
//				console.log("RF LINK! tx_rate: " + objArray[i][1].tx_rate + " = #" + val + "FF00" +
//					" linkLat: " + objArray[i][1].linkLat + " linkLon: " + objArray[i][1].linkLon +
//					" nodeLat: " + nodeLat + " nodeLon: " + nodeLon);
			}
		}
		if(objArray[i][1].linkType === "DTD") {
			if(typeof objArray[i][1].linkLat === 'undefined' ||
				objArray[i][1].linkLat === null ||
				nodeLat === 'undefined' ||
				nodeLat === null) {
//					console.log("null values - DTD");
					continue;
			}else {
				linkColor = "#A020F0";
//				L.polyline([[parseFloat(nodeLat), parseFloat(nodeLon)],[parseFloat(objArray[i][1].linkLat), parseFloat(objArray[i][1].linkLon)]],
//					{color: linkColor, opacity: 0.2, weight: 2, offset: 2}).addTo(layer);
				if(linkMetric === "supernode") {
					L.polyline([[parseFloat(nodeLat), parseFloat(nodeLon)],[parseFloat(objArray[i][1].linkLat), parseFloat(objArray[i][1].linkLon)]],
										{color: linkColor, opacity: 0.5, weight: 2, offset: 2}).bindPopup(
					                                                "<div class='linkPopupContent'><strong>" + objArray[i][1].hostname + "</strong> to <strong>" + node +
					                                                " DTD</div>").addTo(superNodeLinks);
				}else {
					L.polyline([[parseFloat(nodeLat), parseFloat(nodeLon)],[parseFloat(objArray[i][1].linkLat), parseFloat(objArray[i][1].linkLon)]],
										{color: linkColor, opacity: 0.5, weight: 2, offset: 2}).bindPopup(
					                                                "<div class='linkPopupContent'><strong>" + objArray[i][1].hostname + "</strong> to <strong>" + node +
					                                                " DTD</div>").addTo(noRFLinks);
				}
			}
		}
		if(objArray[i][1].linkType === "TUN" || objArray[i][1].linkType === "WIREGUARD") {
			linkColor = "steelblue";
			if(typeof objArray[i][1].linkLat === 'undefined' ||
				objArray[i][1].linkLat === null ||
				nodeLat === 'undefined' ||
				nodeLat === null) {
//					console.log("null values - TUN");
					continue;
			}else {
//				L.polyline([[parseFloat(nodeLat), parseFloat(nodeLon)],[parseFloat(objArray[i][1].linkLat), parseFloat(objArray[i][1].linkLon)]],
//					{color: linkColor, opacity: 0.2, weight: 2, offset: 2}).addTo(layer);
				if(linkMetric === "supernode") {
					L.polyline([[parseFloat(nodeLat), parseFloat(nodeLon)],[parseFloat(objArray[i][1].linkLat), parseFloat(objArray[i][1].linkLon)]],
										{color: linkColor, opacity: 0.5, weight: 2, offset: 2}).bindPopup(
					                                                "<div class='linkPopupContent'><strong>" + objArray[i][1].hostname + "</strong> to <strong>" + node +
					                                                " TUN</div>").addTo(superNodeLinks);
				}else {
					L.polyline([[parseFloat(nodeLat), parseFloat(nodeLon)],[parseFloat(objArray[i][1].linkLat), parseFloat(objArray[i][1].linkLon)]],
										{color: linkColor, opacity: 0.5, weight: 2, offset: 2}).bindPopup(
					                                                "<div class='linkPopupContent'><strong>" + objArray[i][1].hostname + "</strong> to <strong>" + node +
					                                                " TUN</div>").addTo(noRFLinks);
				}
			}
		}
		if(!'linkType' in objArray[i][1]) {
			continue;
		}
	}
}
