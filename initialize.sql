
DROP TABLE IF EXISTS kdz_20.src_weather;

CREATE TABLE kdz_20.src_weather
	(
        local_time VARCHAR(16),
		air_temp VARCHAR(5),
		atm_pressure VARCHAR(6),
		atm_pressure_sl VARCHAR(6),
		humidity VARCHAR(3),
		wind_direction VARCHAR(200),
		wind_speed VARCHAR(100),
		gust_value VARCHAR(100),
		wp_aero VARCHAR(200),
		wp_os VARCHAR(100),
		cloud_cover VARCHAR(350),
		h_visibility DOUBLE PRECISION,
		dtemp VARCHAR(10),
		loaded_ts TIMESTAMP NOT NULL DEFAULT (NOW()),
		modified_at TIMESTAMP NOT NULL DEFAULT (NOW())
	);

DROP TABLE IF EXISTS kdz_20.etl_weather;
CREATE TABLE kdz_20.etl_weather AS
select DISTINCT
    (select airport_dk from dds.airport where dds.airport.icao_code = 'KIAH') as icao_code,
    to_date(local_time, 'DD.MM.YYYY') AS day_stamp,
	to_date(local_time, 'DD.MM.YYYY HH24:MI') AS local_time,
	air_temp::numeric(3, 1),
	atm_pressure::float,
	atm_pressure_sl::float,
	humidity::integer,
	wind_direction,
	wind_speed::int4,
	gust_value::integer,
	wp_aero,
	wp_os,
	cloud_cover,
	h_visibility::numeric(3, 1),
	dtemp::numeric(3, 1),
	loaded_ts,
	modified_at
from kdz_20.kdz20_src_weather;


DROP TABLE IF EXISTS kdz_20.staging_weather;
CREATE TABLE kdz_20.staging_weather (
	icao_code smallint NOT NULL,
	day_stamp timestamp NOT NULL,
	local_time timestamp NOT NULL,
	air_temp numeric(3, 1),
--	p_station_lvl numeric(4, 1) NOT NULL, -- ????
	humidity int4,
	wind_direction varchar(100) NULL,
	wind_speed int4 NULL,
	ff10_max_gust_value int4 NULL,
	ww_present varchar(100) NULL,
	ww_recent varchar(50) NULL,
	c_total_clouds varchar(250) NOT NULL,
	vv_horizontal_visibility numeric(3, 1) NOT NULL,
	td_temperature_dewpoint numeric(3, 1),
	loaded_ts timestamp NOT NULL DEFAULT now(),
	modified_at timestamp not null
); 


delete from kdz_20.etl_weather where day_stamp = '2021-07-27'

insert into kdz_20.staging_weather (icao_code, day_stamp, local_time, air_temp, humidity, wind_direction, wind_speed, ff10_max_gust_value, ww_present, ww_recent, c_total_clouds, vv_horizontal_visibility, td_temperature_dewpoint, loaded_ts, modified_at)
select icao_code, day_stamp, local_time, air_temp, humidity, wind_direction, wind_speed, gust_value, wp_aero , wp_os, cloud_cover, h_visibility, dtemp, loaded_ts, modified_at
from kdz_20.etl_weather;

drop table if exists kdz_20.etl_weather;
CREATE TABLE kdz_20.etl_weather (
	local_time,
	airport_dk,
	cold,
	rain,
	snow,
	thunderstorm,
	drizzle,
	fog_mist,
	t,
	max_gws,
	w_speed,
	weather_dk,
	loaded_ts,
	modified_at,
	date_start,
	date_end,
	day_dk
	)
	as (
	select distinct 
	kdz_20.staging_weather.local_time,
	dds.airport.airport_dk,
	case when kdz_20.staging_weather.air_temp < 0 then 1 else 0 end,
	case when kdz_20.staging_weather.ww_present LIKE '%rain%' or kdz_20.staging_weather.ww_recent LIKE '%rain%' then 1 else 0 end,
	case when kdz_20.staging_weather.ww_present LIKE '%snow%' or kdz_20.staging_weather.ww_recent LIKE '%snow%' then 1 else 0 end,
	case when kdz_20.staging_weather.ww_present LIKE '%trunderstorm%' or kdz_20.staging_weather.ww_recent LIKE '%trunderstorm%' then 1 else 0 end,
	case when kdz_20.staging_weather.ww_present LIKE '%drizzle%' or kdz_20.staging_weather.ww_recent LIKE '%drizzle%' then 1 else 0 end,
	case when kdz_20.staging_weather.ww_present LIKE '%mist%' or kdz_20.staging_weather.ww_recent LIKE '%mist%' then 1 else 0 end,
	kdz_20.staging_weather.air_temp,
	kdz_20.staging_weather.ff10_max_gust_value,
	kdz_20.staging_weather.wind_speed,
	'1', -- Исправить CONCAT
	kdz_20.staging_weather.loaded_ts,
	kdz_20.staging_weather.modified_at,
	kdz_20.staging_weather.loaded_ts,
	COALESCE(Lead(kdz_20.staging_weather.modified_at) OVER(order by kdz_20.staging_weather.loaded_ts desc), '3000-01-01'),
	MD5(CONCAT(to_timestamp(TO_CHAR(local_time, 'DD.MM.YYYY'), 'DD.MM.YYYY'), modified_at))
	from dds.airport join kdz_20.staging_weather on 1=1 where dds.airport.icao_code = 'KIAH'
      );
     
update kdz_20.etl_weather set weather_dk = concat(cold, rain, snow, thunderstorm, drizzle, fog_mist) where true;
     
drop table if exists dds.kdz20_airport_weather;
CREATE TABLE dds.kdz20_airport_weather as select distinct 
	local_time,
	weather_dk,
	airport_dk,
	cold,
	rain,
	snow,
	thunderstorm,
	drizzle,
	fog_mist,
	t,
	max_gws,
	w_speed,
	date_start,
	date_end,
	loaded_ts,
	modified_at,
	day_dk
from kdz_20.etl_weather;

-- Flights:

DROP TABLE IF EXISTS kdz_20.src_flights;
CREATE TABLE kdz_20.src_flights(
	year varchar(25),
	quarter varchar(16),
	month varchar(16),
	flight_date VARCHAR(50) not null,
	reporting_airline VARCHAR(20),
	tail_number VARCHAR(50),
	flight_number VARCHAR(15),
	origin VARCHAR(50),
	dest VARCHAR(50),
	crs_dep_time VARCHAR(20),
	dep_time VARCHAR(20),
	dep_delay_minutes VARCHAR(10),
	cancelled VARCHAR(5),
	cancellation_code BPCHAR(1),
	air_time DOUBLE PRECISION,
	distance DOUBLE PRECISION,
	weather_delay DOUBLE PRECISION,
	loaded_ts TIMESTAMP NOT NULL DEFAULT (NOW()),
	modified_at TIMESTAMP NOT NULL DEFAULT (NOW())
);

DROP TABLE IF EXISTS kdz_20.etl_flights;
CREATE TABLE kdz_20.etl_flights AS
SELECT DISTINCT
	year::INTEGER,
	quarter::INTEGER,
	month::INTEGER,
	flight_date,
	reporting_airline,
	tail_number,
	flight_number,
	origin,
	dest,
	dep_delay_minutes::float,
	cancelled,
	cancellation_code,
	dep_time,
	air_time,
	crs_dep_time,
	distance,
	weather_delay,
	loaded_ts,
	modified_at
FROM kdz_20.src_flights;


DROP TABLE IF EXISTS kdz_20.staging_flights;
CREATE TABLE kdz_20.staging_flights AS
SELECT distinct
year,
quarter,
month,
flight_date,
reporting_airline,
tail_number,
flight_number,
origin,
dest,
dep_delay_minutes,
cancelled,
cancellation_code,
dep_time,
air_time,
crs_dep_time,
distance,
weather_delay,
loaded_ts,
modified_at
FROM kdz_20.etl_flights
WHERE origin = 'IAH' or dest = 'IAH';

drop TABLE IF EXISTS kdz_20.etl_flights;
CREATE TABLE kdz_20.etl_flights 
as
SELECT DISTINCT
	year,
	quarter,
	month,
	(to_timestamp(flight_date, 'MM/DD/YYYY HH12:MI:SS'))::DATE as flight_date,
	reporting_airline,
	tail_number,
	flight_number,
	origin,
	dest,
	dep_delay_minutes,
	cancelled,
	cancellation_code,
	dep_time,
	air_time,
	crs_dep_time,
	distance,
	weather_delay,
	(concat(flight_date, crs_dep_time))::text as flight_dep_scheduled,
	cast(to_timestamp(flight_date::text, 'MM/DD/YYYY') as timestamp) as flight_scheduled_date,
	now() as flight_dep_actual,
	now() as flight_actual_date,
--	(to_timestamp(concat(flight_date, crs_dep_time), 'MM/DD/YYYY HH12:MI:SS') + interval concat(6, ' minute')) as flight_dep_actual,
--	to_date(flight_dep_actual, 'MM/DD/YYYY') as flight_actual_date,
	md5(concat(origin, (select airport_dk from dds.airport where icao_code = origin))) as origin_dk,
	md5(concat(dest, (select airport_dk from dds.airport where icao_code = dest))) as dest_dk,
	loaded_ts,
	modified_at,
	now() as start_date,
	COALESCE(Lead(kdz_20.staging_flights.modified_at) OVER(order by kdz_20.staging_flights.loaded_ts desc), '3000-01-01') as end_date,
	MD5(CONCAT(to_timestamp(TO_CHAR(loaded_ts, 'DD.MM.YYYY'), 'DD.MM.YYYY'), flight_number)) as flight_dk 
FROM kdz_20.staging_flights
WHERE origin = 'IAH' or dest = 'IAH';

--select distinct flight_number, COUNT(*) kdz_20.etl_flights group by flight_number HAVING COUNT(*) > 1;
--to_char(dep_delay_minutes)
update kdz_20.etl_flights set flight_dep_actual = (to_timestamp(flight_dep_scheduled, 'MM/DD/YYYY HH24:MI:SS')::date + (dep_delay_minutes * interval '1 minute'))::timestamp where true;
update kdz_20.etl_flights set flight_actual_date = (to_timestamp(to_char(flight_dep_actual, 'MM/DD/YYYY'), 'MM/DD/YYYY')) where true;

create table dds.kdz20_flights as select distinct
	year,
	quarter,
	month,
	flight_date,
	reporting_airline,
	tail_number,
	flight_number,
	origin,
	dest,
	dep_delay_minutes,
	cancelled,
	cancellation_code,
	dep_time,
	air_time,
	crs_dep_time,
	distance,
	weather_delay,
	flight_dep_scheduled,
	flight_scheduled_date,
	flight_dep_actual,
	flight_actual_date,
	origin_dk,
	dest_dk,
	loaded_ts,
	modified_at,
	start_date,
	end_date,
	flight_dk
	from kdz_20.etl_flights;








--;