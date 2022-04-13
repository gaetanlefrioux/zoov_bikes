let col_order = ["bike_id", "lat", "lon", "vehicle_type_id", "current_range_meters", "distance", "free_state_start"];
let input_to_name = {
  "latitudeInput": "lat",
  "longitudeInput": "lon",
  "freeSinceInput": "free_since",
  "maxDistanceInput": "max_distance"
};

function update_table(data) {
  let tbody = document.getElementById('table-body');

  // empty the table
  tbody.innerHTML = '';

  // fill the table with the data
  data.forEach(bike => {
    let newRow = tbody.insertRow();
    col_order.forEach(col => {
      let newCell = newRow.insertCell();
      let value = (col === 'free_state_start') ? new Date(bike[col]).toLocaleString() : bike[col];
      let newText = document.createTextNode(value);
      newCell.appendChild(newText);
    });
  });
}

function build_url() {
  const url = new URL(`${window.origin}/api/free_bikes`);
  for (const [input_id, name] of Object.entries(input_to_name)) {
    url.searchParams.append(name, document.getElementById(input_id).value);
  }
  return url;
}

function update_free_bikes(event) {
  fetch(build_url())
    .then(function(response) {
      if (response.status !== 200) {
        console.log(`Status code: ${response.status}`);
        return;
      }
      response.json().then(update_table);
    })
    .catch((err) => {
      console.log("Error when fetching free bikes: " + err);
    })
}

document.addEventListener("DOMContentLoaded", update_free_bikes);
const interval = setInterval(update_free_bikes, 5000);
