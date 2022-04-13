let col_order = ["bike_id", "total_distance", "total_time", "total_trip_count"];
let input_to_name = {
  "hourWindowInput": "hour_window"
};

function update_template(data) {
  let tbody = document.getElementById('table-body');

  // empty the table
  tbody.innerHTML = '';

  // fill the table with the data
  data["by_bikes"].forEach(bike => {
    let newRow = tbody.insertRow();
    col_order.forEach(col => {
      let newCell = newRow.insertCell();
      let value = bike[col];
      let newText = document.createTextNode(value);
      newCell.appendChild(newText);
    });
  });

  document.getElementById("total-distance").textContent = data["global"]["total_distance"];
  document.getElementById("total-time").textContent = data["global"]["total_time"];
  document.getElementById("total-trip").textContent = data["global"]["total_trip_count"];
}

function build_url() {
  const url = new URL(`${window.origin}/api/bikes_statistics`);
  for (const [input_id, name] of Object.entries(input_to_name)) {
    url.searchParams.append(name, document.getElementById(input_id).value);
  }
  return url;
}

function update_bike_statistics(event) {
  fetch(build_url())
    .then(function(response) {
      if (response.status !== 200) {
        console.log(`Status code: ${response.status}`);
        return;
      }
      response.json().then(update_template);
    })
    .catch((err) => {
      console.log("Error when fetching free bikes: " + err);
    })
}

document.addEventListener("DOMContentLoaded", update_bike_statistics);
const interval = setInterval(update_bike_statistics, 5000);
