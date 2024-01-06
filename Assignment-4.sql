--1.1
create schema ptv;
--1.2
-- Create & Import Agency Table
create table ptv.agency(
	agency_id numeric,
	agency_name varchar,
	agency_url varchar,
	agency_timezone varchar,
	agency_lang varchar
);
COPY ptv.agency FROM '/data/adata/gtfs/agency.txt' DELIMITER ',' CSV HEADER;

-- Create & Import Routes Table
create table ptv.routes(
 route_id varchar,
 agency_id numeric,
 route_short_name varchar,
 route_long_name varchar,
 route_type numeric,
 route_color varchar,
 route_text_color varchar
);
COPY ptv.routes FROM '/data/adata/gtfs/routes.txt' DELIMITER ',' CSV HEADER;


--Create & Import Calendar Table
create table ptv.calendar(
 service_id varchar,
 monday numeric,
 tuesday numeric,
 wednesday numeric,
 thursday numeric,
 friday numeric,
 saturday numeric,
 sunday numeric,
 start_date date,
 end_date date
);
COPY ptv.calendar FROM '/data/adata/gtfs/calendar.txt' DELIMITER ',' CSV HEADER;

--Create & Import Calendar_dates Table
create table ptv.calendar_dates(
 service_id varchar,
 date date,
 exception_type numeric
);
COPY ptv.calendar_dates FROM '/data/adata/gtfs/calendar_dates.txt' DELIMITER ',' CSV HEADER;

--Create & Import Shapes Table
create table ptv.shapes(
 shape_id varchar,
 shape_pt_lat float,
 shape_pt_lon float,
 shape_pt_sequence numeric,
 shape_dist_traveled float
);
COPY ptv.shapes FROM '/data/adata/gtfs/shapes.txt' DELIMITER ',' CSV HEADER;

--Create & Import Stop_Times Table
create table ptv.stop_times(
 trip_id varchar,
 arrival_time varchar, -- It cannot be time because there is 24:19:00. It raises an error if I use time
 departure_time varchar, -- It cannot be time because there is 24:19:00. It raises an error if I use time
 stop_id varchar,
 stop_sequence numeric,
 stop_headsign varchar,
 pickup_type numeric,
 drop_off_type numeric,
 shape_dist_traveled varchar -- I tried to use float before, but when I used it, I got an error because there were empty strings
);
COPY ptv.stop_times FROM '/data/adata/gtfs/stop_times.txt' DELIMITER ',' CSV HEADER;

--Create & Import Stops Table
create table stops(
 stop_id varchar,
 stop_name varchar,
 stop_lat float,
 stop_lon float
);
COPY ptv.stops FROM '/data/adata/gtfs/stops.txt' DELIMITER ',' CSV HEADER;

--Create & Import Trips Table
create table trips(
 route_id varchar,
 service_id varchar,
 trip_id varchar,
 shape_id varchar,
 trip_headsign varchar,
 direction_id numeric
);
COPY ptv.trips FROM '/data/adata/gtfs/trips.txt' DELIMITER ',' CSV HEADER;

-- 1.3 Create & Import MB2021 Table
ogr2ogr PG:"dbname=gisdb user=postgres" "/data/adata/MB_2021_AUST_SHP_GDA2020/MB_2021_AUST_GDA2020.shp" -nln ptv.mb2021 -overwrite -nlt MULTIPOLYGON

--1.4
--Create & Import LGA2021 Table
create table LGA2021(
 mb_code_2021 varchar,
 lga_code_2021 varchar,
 lga_name_2021 varchar,
 state_code_2021 varchar,
 state_name_2021 varchar,
 aus_code_2021 varchar,
 aus_name_2021 varchar,
 area_albers_sqkm float,
 asgs_loci_uri_2021 varchar
);
COPY ptv.lga2021 FROM '/data/adata/LGA_2021_AUST.csv' DELIMITER ',' CSV HEADER;

--Create & Import SAL2021 Table
create table SAL2021(
 mb_code_2021 varchar,
 sal_code_2021 varchar,
 sal_name_2021 varchar,
 state_code_2021 varchar,
 state_name_2021 varchar,
 aus_code_2021 varchar,
 aus_name_2021 varchar,
 area_albers_sqkm float,
 asgs_loci_uri_2021 varchar
);
COPY ptv.sal2021 FROM '/data/adata/SAL_2021_AUST.csv' DELIMITER ',' CSV HEADER;


-- 1.5 Data Verification
with tbl as
(select table_schema, TABLE_NAME
 from information_schema.tables
 where table_schema in ('ptv'))
select table_schema, TABLE_NAME,
(xpath('/row/c/text()', query_to_xml(format('select count(*) as c from %I.%I', table_schema, TABLE_NAME), FALSE, TRUE, '')))[1]::text::int AS rows_n
from tbl
order by table_name; 


-- 2.1 Filter Melbourne Metrapolitan Area
create table ptv.mb2021_mel as
(
 select * from ptv.mb2021
 where lower(gcc_name21) = lower('Greater Melbourne')
);


-- 2.2 Melbourne Metrapolitan Boundary
create table ptv.melbourne as 
(
 select st_union(wkb_geometry) from ptv.mb2021_mel
);

select * from ptv.melbourne;


-- 2.3 Add Geometry Column to Stops table
-- Step 1 Alter new column
alter table ptv.stops 
add geom geometry;

-- Update the geom with the geom value from their lon and lat
update ptv.stops
set geom = ST_SetSRID(ST_MakePoint(stop_lon, stop_lat), 7844);


-- 2.4 Denormalise GTFS structure
create table ptv.stops_routes_mel as
(
	select distinct s.stop_id, s.stop_name, s.geom , r.route_short_name as "route_number", r.route_long_name as "route_name",
	case 
		when r.route_type = 0 then 'Tram'
		when r.route_type = 2 then 'Train'
		when r.route_type = 3 then 'Bus'
		else 'Unknown'
	end as "Vehicle"
	from ptv.stops s
	join ptv.stop_times st on s.stop_id = st.stop_id 
	join ptv.trips t on st.trip_id = t.trip_id 
	join ptv.routes r on t.route_id = r.route_id
);

select distinct s.stop_id, s.stop_name, s.geom , r.route_short_name as "route_number", r.route_long_name as "route_name",
	st_contain()
	from ptv.stops s
	join ptv.stop_times st on s.stop_id = st.stop_id 
	join ptv.trips t on st.trip_id = t.trip_id 
	join ptv.routes r on t.route_id = r.route_id

-- 2.4.1
select count(*) from ptv.stops_routes_mel;
-- 2.4.2
select count(distinct(stop_id)) from ptv.stops_routes_mel;

select * from mb2021 m;


-- 3.1
WITH BusStops AS (
    -- Filter out stops where the vehicle is a 'Bus'
    SELECT stop_id, geom
    FROM ptv.stops_routes_mel srm
    WHERE "Vehicle" = 'Bus'
),
BusStopsWithMB AS (
    -- Join bus stops with mesh blocks based on spatial location
    SELECT b.stop_id, m.mb_code21, m.sa2_name21
    FROM BusStops b
    JOIN ptv.mb2021_mel m ON ST_Contains(m.wkb_geometry, b.geom)
)
-- Join with sal2021 to get the suburb name and count the bus stops
SELECT s.sal_name_2021 AS suburb_name, COUNT(b.stop_id) AS total_bus_stops
FROM BusStopsWithMB b
JOIN ptv.sal2021 s ON b.mb_code21 = s.mb_code_2021
GROUP BY s.sal_name_2021
ORDER BY total_bus_stops DESC;


-- OPTIMIZED 3.1
-- Ensure spatial indexes exist on the spatial columns
CREATE INDEX IF NOT EXISTS idx_stops_routes_mel_geom ON ptv.stops_routes_mel USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_mb2021_mel_wkb_geometry ON ptv.mb2021_mel USING GIST(wkb_geometry);
-- Filter first: Create a temporary table or CTE with only bus stops
WITH BusStops AS (
    SELECT stop_id, geom
    FROM ptv.stops_routes_mel
    WHERE "Vehicle" = 'Bus'
),
-- Use bounding box checks before detailed spatial checks
BoundingBoxJoin AS (
    SELECT b.stop_id, m.mb_code21, m.sa2_name21
    FROM BusStops b
    JOIN ptv.mb2021_mel m ON b.geom && m.wkb_geometry -- Bounding box check
    WHERE ST_Contains(m.wkb_geometry, b.geom) -- Detailed spatial check
)
-- Final aggregation
SELECT s.sal_name_2021 AS suburb_name, COUNT(b.stop_id) AS total_bus_stops
FROM BoundingBoxJoin b
JOIN ptv.sal2021 s ON b.mb_code21 = s.mb_code_2021
GROUP BY s.sal_name_2021
ORDER BY total_bus_stops DESC;

-- 3.1.1
-- Ensure spatial indexes exist on the spatial columns
CREATE INDEX IF NOT EXISTS idx_stops_routes_mel_geom ON ptv.stops_routes_mel USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_mb2021_mel_wkb_geometry ON ptv.mb2021_mel USING GIST(wkb_geometry);
-- Filter first: Create a temporary table or CTE with only bus stops
WITH BusStops AS (
    SELECT stop_id, geom
    FROM ptv.stops_routes_mel
    WHERE "Vehicle" = 'Bus'
),
-- Use bounding box checks before detailed spatial checks
BoundingBoxJoin AS (
    SELECT b.stop_id, m.mb_code21, m.sa2_name21
    FROM BusStops b
    JOIN ptv.mb2021_mel m ON b.geom && m.wkb_geometry -- Bounding box check
    WHERE ST_Contains(m.wkb_geometry, b.geom) -- Detailed spatial check
)
-- Final aggregation
SELECT s.sal_name_2021 AS suburb_name, COUNT(b.stop_id) AS total_bus_stops
FROM BoundingBoxJoin b
JOIN ptv.sal2021 s ON b.mb_code21 = s.mb_code_2021
GROUP BY s.sal_name_2021
ORDER BY total_bus_stops ASC, s.sal_name_2021 ASC
LIMIT 5;

-- 3.1.2
-- Ensure spatial indexes exist on the spatial columns
CREATE INDEX IF NOT EXISTS idx_stops_routes_mel_geom ON ptv.stops_routes_mel USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_mb2021_mel_wkb_geometry ON ptv.mb2021_mel USING GIST(wkb_geometry);
-- Filter first: Create a temporary table or CTE with only bus stops
WITH BusStops AS (
    SELECT stop_id, geom
    FROM ptv.stops_routes_mel
    WHERE "Vehicle" = 'Bus'
),
-- Use bounding box checks before detailed spatial checks
BoundingBoxJoin AS (
    SELECT b.stop_id, m.mb_code21, m.sa2_name21
    FROM BusStops b
    JOIN ptv.mb2021_mel m ON b.geom && m.wkb_geometry -- Bounding box check
    WHERE ST_Contains(m.wkb_geometry, b.geom) -- Detailed spatial check
),
-- Aggregation by suburb
SuburbStops AS (
    SELECT s.sal_name_2021 AS suburb_name, COUNT(DISTINCT b.stop_id) AS distinct_bus_stops
    FROM BoundingBoxJoin b
    JOIN ptv.sal2021 s ON b.mb_code21 = s.mb_code_2021
    GROUP BY s.sal_name_2021
)

-- Calculate the average
SELECT AVG(distinct_bus_stops) AS average_distinct_stops_per_suburb
FROM SuburbStops;


-- 3.2 LGA BLANKSPOT
-- Ensure spatial indexes exist on the spatial columns
CREATE INDEX IF NOT EXISTS idx_stops_routes_mel_geom ON ptv.stops_routes_mel USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_mb2021_mel_wkb_geometry ON ptv.mb2021_mel USING GIST(wkb_geometry);
-- Filter first: Create a temporary table or CTE with only bus stops
WITH BusStops AS (
    SELECT stop_id, geom
    FROM ptv.stops_routes_mel
    WHERE "Vehicle" = 'Bus'
),

-- Identify mesh blocks without bus stops
MeshBlocksWithoutStops AS (
    SELECT m.mb_code21
    FROM ptv.mb2021_mel m
    LEFT JOIN BusStops b ON m.wkb_geometry && b.geom
    WHERE ST_Contains(m.wkb_geometry, b.geom) is FALSE
    AND m.mb_cat21 = 'Residential'
),

-- Calculate the blankspot percentage for each LGA
BlankspotStats AS (
    SELECT 
        l.lga_name_2021 AS lga_name,
        COUNT(DISTINCT m.mb_code21) AS total_residential_meshblocks,
        COUNT(DISTINCT ms.mb_code21) AS blankspots
    FROM ptv.mb2021_mel m
    JOIN ptv.lga2021 l ON m.mb_code21 = l.mb_code_2021
    LEFT JOIN MeshBlocksWithoutStops ms ON m.mb_code21 = ms.mb_code21
    WHERE m.mb_cat21 = 'Residential'
    AND l.lga_name_2021 LIKE 'Melbourne%'
    GROUP BY l.lga_name_2021
)


-- Display the results
SELECT 
    lga_name,
    total_residential_meshblocks,
    blankspots,
    (blankspots::FLOAT / total_residential_meshblocks) * 100 AS blankspot_percentage
FROM BlankspotStats


----

--- PART 3.2 STATISTICS
-- Ensure spatial indexes exist on the spatial columns
CREATE INDEX IF NOT EXISTS idx_stops_routes_mel_geom ON ptv.stops_routes_mel USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_mb2021_mel_wkb_geometry ON ptv.mb2021_mel USING GIST(wkb_geometry);
-- Filter first: Create a temporary table or CTE with only bus stops
WITH BusStops AS (
    SELECT stop_id, geom
    FROM ptv.stops_routes_mel
    WHERE "Vehicle" = 'Bus'
),

-- Identify mesh blocks without bus stops
MeshBlocksWithoutStops AS (
    SELECT m.mb_code21
    FROM ptv.mb2021_mel m
    LEFT JOIN BusStops b ON m.wkb_geometry && b.geom
    WHERE ST_Contains(m.wkb_geometry, b.geom) is FALSE
    AND m.mb_cat21 = 'Residential'
),
-- Calculate blankspot statistics for each LGA
BlankspotStats AS (
    SELECT 
        lga.lga_name_2021 AS lga_name,
        COUNT(DISTINCT mb.mb_code21) AS total_residential_meshblocks,
        COUNT(DISTINCT CASE WHEN mws.mb_code21 IS NOT NULL THEN mb.mb_code21 ELSE NULL END) AS blankspots
    FROM ptv.mb2021_mel mb
    JOIN ptv.lga2021 lga ON mb.mb_code21 = lga.mb_code_2021
    LEFT JOIN MeshBlocksWithoutStops mws ON mb.mb_code21 = mws.mb_code21
    WHERE mb.mb_cat21 = 'Residential'
    GROUP BY lga.lga_name_2021
)

-- Top 5 LGAs with the highest % of blankspots:
SELECT 
    lga_name,
    total_residential_meshblocks,
    blankspots,
    (blankspots::FLOAT / total_residential_meshblocks) * 100 AS blankspot_percentage
FROM BlankspotStats
ORDER BY blankspot_percentage DESC
LIMIT 5;


-- Average % of blankspots:
SELECT 
    AVG((blankspots::FLOAT / total_residential_meshblocks) * 100) AS avg_blankspot_percentage
FROM BlankspotStats
WHERE total_residential_meshblocks > 0; -- To avoid division by zero

-- Top 5 LGAs with the lowest % of blankspots:
SELECT 
    lga_name,
    total_residential_meshblocks,
    blankspots,
    (blankspots::FLOAT / total_residential_meshblocks) * 100 AS blankspot_percentage
FROM BlankspotStats
WHERE total_residential_meshblocks > 0 -- To avoid division by zero
ORDER BY blankspot_percentage ASC
LIMIT 5;



-- Ensure spatial indexes exist on the spatial columns
CREATE INDEX IF NOT EXISTS idx_stops_routes_mel_geom ON ptv.stops_routes_mel USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_mb2021_mel_wkb_geometry ON ptv.mb2021_mel USING GIST(wkb_geometry);

-- Filter first: Create a temporary table or CTE with only bus stops
WITH BusStops AS (
    SELECT DISTINCT stop_id, geom
    FROM ptv.stops_routes_mel
    WHERE "Vehicle" = 'Bus'
),

-- Identify mesh blocks with bus stops
MeshBlocksWithStops AS (
    SELECT DISTINCT m.mb_code21
    FROM ptv.mb2021_mel m
    JOIN BusStops b ON m.wkb_geometry && b.geom
    WHERE ST_Contains(m.wkb_geometry, b.geom)
    AND m.mb_cat21 = 'Residential'
),

WITH BlankspotStats AS (
    SELECT 
        lga.lga_name_2021 AS lga_name,
        COUNT(DISTINCT mb.mb_code21) AS total_residential_meshblocks,
        COUNT(DISTINCT CASE WHEN sr.stop_id IS NULL THEN mb.mb_code21 ELSE NULL END) AS blankspots
    FROM ptv.mb2021_mel mb
    JOIN ptv.lga2021 lga ON mb.mb_code21 = lga.mb_code_2021
    LEFT JOIN ptv.stops_routes_mel sr ON ST_Contains(mb.wkb_geometry, sr.geom) AND "Vehicle" = 'Bus'
    WHERE mb.mb_cat21 = 'Residential'
    GROUP BY lga.lga_name_2021
)

-- Average % of blankspots:
SELECT 
    AVG((blankspots::FLOAT / total_residential_meshblocks) * 100) AS avg_blankspot_percentage
FROM BlankspotStats
WHERE total_residential_meshblocks > 0; -- To avoid division by zero

-- Top 5 LGAs with the highest % of blankspots:
SELECT 
    lga_name,
    total_residential_meshblocks,
    blankspots,
    (blankspots::FLOAT / total_residential_meshblocks) * 100 AS blankspot_percentage
FROM BlankspotStats
ORDER BY blankspot_percentage DESC
LIMIT 5;

-- TASK 4
-- Using the previous CTEs and calculations:
CREATE TABLE ptv.lga_blankspot AS
(
    -- CTE (Common Table Expression) to filter out only bus stops from the stops_routes_mel table
    WITH BusStops AS (
        SELECT stop_id, geom
        FROM ptv.stops_routes_mel
        WHERE "Vehicle" = 'Bus'
    ),
    
    -- CTE to identify mesh blocks that are residential but don't have any bus stops
    MeshBlocksWithoutStops AS (
        SELECT m.mb_code21
        FROM ptv.mb2021_mel m
        LEFT JOIN BusStops b ON m.wkb_geometry && b.geom
        WHERE ST_Contains(m.wkb_geometry, b.geom) IS FALSE
        AND m.mb_cat21 = 'Residential'
    ),
    
    -- CTE to calculate blankspot statistics for each LGA
    BlankspotStats AS (
        SELECT 
            lga.lga_name_2021 AS lga_name,
            ST_Union(mb.wkb_geometry) AS lga_geom,  -- Aggregating the geometries of the mesh blocks for each LGA
            COUNT(DISTINCT mb.mb_code21) AS total_residential_meshblocks,
            COUNT(DISTINCT CASE WHEN mws.mb_code21 IS NOT NULL THEN mb.mb_code21 ELSE NULL END) AS blankspots
        FROM ptv.mb2021_mel mb
        JOIN ptv.lga2021 lga ON mb.mb_code21 = lga.mb_code_2021
        LEFT JOIN MeshBlocksWithoutStops mws ON mb.mb_code21 = mws.mb_code21
        WHERE mb.mb_cat21 = 'Residential'
        GROUP BY lga.lga_name_2021
    )

    -- Selecting the final results from the BlankspotStats CTE, computing the blankspot percentage, and including the aggregated geometry
    SELECT 
        lga_name,
        lga_geom,
        total_residential_meshblocks,
        blankspots,
        (blankspots::FLOAT / total_residential_meshblocks) * 100 AS blankspot_percentage
    FROM BlankspotStats
);