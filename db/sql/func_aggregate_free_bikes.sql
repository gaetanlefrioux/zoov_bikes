CREATE OR REPLACE FUNCTION aggregate_free_bikes(sys_update_time timestamp)
RETURNS void AS $$

  -- End trips for bikes newly free
  update trips
  set
    end_time = subquery.last_reported,
    end_lat = subquery.lat,
    end_lon = subquery.lon
  from (
      select system_id, bike_id, lat, lon, last_reported
      from current_free_bikes
      where CONCAT(system_id, '', bike_id) in (
        select CONCAT(system_id, '', bike_id) from bikes
        where is_free = FALSE
      ) and system_update_time = sys_update_time
  ) as subquery
  where
    subquery.system_id = trips.system_id
    and subquery.bike_id = trips.bike_id
    and trips.start_time in (
      select max(start_time) from trips
      where system_id = subquery.system_id
      and bike_id = subquery.bike_id
    );

  -- insert bikes not seen by the system at this time
  insert into bikes(
    system_id, bike_id, lat, lon, is_reserved,
    is_disabled, vehicle_type_id, last_reported,
    current_range_meters, station_id, pricing_plan_id,
    rental_uri_android, rental_uri_ios, is_free, free_state_start
  )
  select
    system_id, bike_id, lat, lon, is_reserved,
    is_disabled, vehicle_type_id, last_reported,
    current_range_meters, station_id, pricing_plan_id,
    rental_uri_android, rental_uri_ios, TRUE, last_reported
  from current_free_bikes
  where
    system_update_time = sys_update_time
  on conflict (system_id, bike_id)
  -- Update bikes that were already free and bikes that are coming back from a trip
  do update set
    lat = EXCLUDED.lat, lon = EXCLUDED.lon, is_reserved = EXCLUDED.is_reserved, is_free = TRUE,
    is_disabled = EXCLUDED.is_disabled, vehicle_type_id = EXCLUDED.vehicle_type_id,
    current_range_meters = EXCLUDED.current_range_meters, station_id = EXCLUDED.station_id, pricing_plan_id = EXCLUDED.pricing_plan_id,
    rental_uri_android = EXCLUDED.rental_uri_android, rental_uri_ios = EXCLUDED.rental_uri_ios, last_reported = EXCLUDED.last_reported,
    free_state_start = least(bikes.free_state_start, EXCLUDED.last_reported);

  -- update bikes that are not free anymore
  update bikes
  set
    is_free = FALSE, free_state_start = NULL
  where CONCAT(bikes.system_id, '', bikes.bike_id) not in (
    select CONCAT(current_free_bikes.system_id, '', current_free_bikes.bike_id)
    from current_free_bikes
    where system_update_time = sys_update_time
  );

  -- create new trip for bikes that are not free anymore
  insert into trips (
    system_id, bike_id, start_time, end_time,
    start_lat, start_lon, end_lat, end_lon, pricing_plan_id
  )
  select
    system_id, bike_id, last_reported, NULL,
    lat, lon, NULL, NULL, pricing_plan_id
  from bikes
  where CONCAT(system_id, '', bike_id) not in (
    select CONCAT(system_id, '', bike_id)
    from current_free_bikes
    where system_update_time = sys_update_time
  );

  -- delete processed free bikes
  delete from current_free_bikes
	where system_update_time = sys_update_time;

$$ LANGUAGE SQL;
