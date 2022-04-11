CREATE TABLE IF NOT EXISTS current_free_bikes (
  system_update_time TIMESTAMP NOT NULL,
  system_id  VARCHAR NOT NULL,
  bike_id VARCHAR NOT NULL,
  lat REAL NOT NULL,
  lon REAL NOT NULL,
  is_reserved BOOLEAN NOT NULL,
  is_disabled BOOLEAN NOT NULL,
  vehicle_type_id VARCHAR NOT NULL,
  last_reported TIMESTAMP NOT NULL,
  current_range_meters INT NOT NULL,
  station_id VARCHAR,
  pricing_plan_id VARCHAR NOT NULL,
  rental_uri_android VARCHAR NOT NULL,
  rental_uri_ios VARCHAR NOT NULL,
  PRIMARY KEY (system_update_time, system_id, bike_id)
);

CREATE TABLE IF NOT EXISTS bikes (
  system_id  VARCHAR NOT NULL,
  bike_id VARCHAR NOT NULL,
  lat REAL NOT NULL,
  lon REAL NOT NULL,
  is_reserved BOOLEAN,
  is_disabled BOOLEAN,
  vehicle_type_id VARCHAR NOT NULL,
  last_reported TIMESTAMP NOT NULL,
  current_range_meters INT NOT NULL,
  station_id VARCHAR,
  pricing_plan_id VARCHAR NOT NULL,
  rental_uri_android VARCHAR NOT NULL,
  rental_uri_ios VARCHAR NOT NULL,
  is_free BOOLEAN NOT NULL,
  free_state_start TIMESTAMP,
  PRIMARY KEY (system_id, bike_id)
);

CREATE TABLE IF NOT EXISTS trips (
  system_id  VARCHAR NOT NULL,
  bike_id VARCHAR NOT NULL,
  start_time TIMESTAMP NOT NULL,
  end_time TIMESTAMP NOT NULL,
  start_lat REAL NOT NULL,
  start_lon REAL NOT NULL,
  end_lat REAL,
  end_lon REAL,
  PRIMARY KEY (system_id, bike_id, start_time)
);
