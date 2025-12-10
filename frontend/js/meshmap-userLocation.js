/*
* Part of meshmap - kg6wxc 2024
*
*/
function getLocation() {
	// Geolocation requires HTTPS or localhost - skipped for mesh network HTTP access
	// If needed in the future, implement with HTTPS or local coordinates
}

function usePosition(position) {
//	mapInfo['mapCenterCoords'][0] = position.coords.latitude;
//	mapInfo['mapCenterCoords'][1] = position.coords.longitude;
	youAreHereIconRef = L.marker([position.coords.latitude, position.coords.longitude], { icon: userLocationIcon }).addTo(map);
	youAreHereIconRef._icon.id = "youAreHere";
	map.panTo([position.coords.latitude, position.coords.longitude]);
}

