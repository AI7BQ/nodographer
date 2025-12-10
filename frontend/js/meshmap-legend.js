/**
 * part of KG6WXC meshmap
 * 2024 KG6WXC
 */
var legendHidden = L.control({position: 'topright'});
legendHidden.onAdd = function(map) {
	var div = L.DomUtil.create('div', 'info legendHidden', L.DomUtil.get('map'));
	div.innerHTML += '<button id="legendLinkButtonHidden" onclick="hideLegend()">Show Legend</button>';
	return div;
};

var legend = L.control({position: 'topright'});
legend.onAdd = function (map) {
	var div = L.DomUtil.create('div', 'info legend', L.DomUtil.get('map'));
	div.innerHTML +=
		'<div id="legend">' +
		'<div id="legendGradientRectangle">10&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;TX Rate (mbps)&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;100</div>' +
		'<div id="legendDTDLink">DTD</div>' +
		'<div id="legendTunnelLink">Tunnel</div>' +
		'<div id="legendMapOrigin"><img class="legendBandIcon" src="images/mapMarkers/pulse.svg">Map Origin</div>' +
		'<div id="legendUnknownOther">' +
		'<img class="legendBandIcon" src="images/mapMarkers/greyRadioCircle-icon.png">Non RF' +
		'</div>' +
		'<div id="legend900MHz">' +
		'<img class="legendBandIcon" src="images/mapMarkers/magentaRadioCircle-icon.png">900MHz' +
		'</div>' +
		'<div id="legend2GHz">' +
		'<img class="legendBandIcon" src="images/mapMarkers/purpleRadioCircle-icon.png">2GHz' +
		'</div>' +
		'<div id="legend3GHz">' +
		'<img class="legendBandIcon" src="images/mapMarkers/blueRadioCircle-icon.png">3GHz' +
		'</div>' +
		'<div id="legend5GHz">' +
		'<img class="legendBandIcon" src="images/mapMarkers/goldRadioCircle-icon.png">5GHz' +
		'</div>' +
		'<div id="legendHide"><button id="legendLinkButton" onclick="hideLegend()">Hide</button></div>' +
		'<div id="darkModeToggle"><input id="darkModeCheckBox" type="checkbox" name="darkMode" data-theme-toggle />' +
		'<label for="darkModeCheckBox">Dark</label></div>' +
		'</div>';
	return div;
};

function hideLegend() {
	var lgd = document.getElementsByClassName("legend");
	var lgdHidden = document.getElementsByClassName("legendHidden");
	if (lgd[0].style.display === "" || lgd[0].style.display === "block") {
		lgd[0].style.display = "none";
		lgdHidden[0].style.display = "block";
	}else {
		lgd[0].style.display = "block";
		lgdHidden[0].style.display = "none";
	}
}

function toggleDarkMode() {
	if(window.matchMedia(darkModeText).matches) {
		window.setMedia(lightModeText);
		document.getElementById("darkModeCheckBox").checked = false;
	}else {
		window.setMedia(darkModeText);
		document.getElementById("darkModeCheckBox").checked = true;
	}
}
