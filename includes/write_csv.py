from config import *
from DB_Connectors import DBConnector
from import_data import Area_Mode, IDF_Area_Mode, Hanoi_Area_Mode
import numpy as np

def write_bus_stop_csv(
	db_connector:DBConnector,
	area_mode:Area_Mode,
	stops_threshold_line:int=20,
	nb_lines_max:int=100,
):
	query_bus_stop_in_box(db_connector, area_mode, stops_threshold_line, nb_lines_max)
	output = db_connector.con.fetchall()
	write_bus_lines_list_csv(output, area_mode)
	write_bus_lines_csv(output, area_mode)

	return None

def query_bus_stop_in_box(
		db_connector:DBConnector,
		area_mode:Area_Mode,
		stops_threshold_line:int=5,
		nb_lines_max:int=100,
	)->None:
	reduced_roads_path = REDUCED_DATA_PATH / "reduced_roads.shp"
	prefix = type(area_mode).__name__
	if isinstance(area_mode, IDF_Area_Mode):
		query = f"""WITH 
			filtered_stops AS (
				SELECT stop_id
				FROM {prefix}_stops
				WHERE ST_Contains(
				(
					SELECT ST_ConvexHull(ST_Union_Agg(geom)) FROM ST_ReadSHP($reduced_roads_path)
				)
				, ST_Point(stop_lon, stop_lat))
			),
		
			candidate_services AS (
				SELECT trip_id FROM {prefix}_trips
				JOIN {prefix}_calendar USING(service_id)
				WHERE MONDAY AND TUESDAY AND WEDNESDAY AND THURSDAY AND FRIDAY
			),
			
			candidate_routes AS (
				SELECT route_id, route_color FROM {prefix}_routes
				WHERE route_type=3
			),
			
			filtered_trips AS (
				SELECT DISTINCT ON(trip_id) trip_id, route_id, route_color FROM {prefix}_trips
				JOIN candidate_services USING(trip_id)
				JOIN candidate_routes USING(route_id)
			),
			
			candidate_stops AS (
				SELECT stop_id, trip_id, stop_sequence FROM {prefix}_stop_times
				JOIN filtered_stops USING(stop_id)
				WHERE CAST (departure_time[1:2] AS INT)>=7 AND CAST(arrival_time[1:2] AS INT)<=18
			)
			 
		SELECT DISTINCT ON(route_id) route_id, ANY_VALUE(route_color), list(stop_id ORDER BY stop_sequence) FROM candidate_stops
		JOIN filtered_trips USING (trip_id)
		GROUP BY (trip_id, route_id)
		HAVING COUNT(stop_id)>$stops_threshold_line
		ORDER BY COUNT(stop_id) DESC
		LIMIT $nb_lines;"""
	elif isinstance(area_mode, Hanoi_Area_Mode):
		query = f"""WITH 
			filtered_stops AS (
				SELECT stop_id
				FROM {prefix}_stops
				WHERE ST_Contains(
				(
					SELECT ST_ConvexHull(ST_Union_Agg(geom)) FROM ST_ReadSHP($reduced_roads_path)
				)
				, ST_Point(stop_lon, stop_lat))
			),
		
			candidate_services AS (
				SELECT trip_id FROM {prefix}_trips
				JOIN {prefix}_calendar USING(service_id)
				WHERE monday AND tuesday AND wednesday AND thursday AND friday
			),
			
			candidate_routes AS (
				SELECT route_id, route_color FROM {prefix}_routes
				WHERE route_type=3
			),
			
			filtered_trips AS (
				SELECT DISTINCT ON(trip_id) trip_id, route_id FROM {prefix}_trips
				JOIN candidate_services USING(trip_id)
				JOIN candidate_routes USING(route_id)
			),
			
			candidate_stops AS (
				SELECT stop_id, trip_id, stop_sequence FROM {prefix}_stop_times
				JOIN filtered_stops USING(stop_id)
				WHERE departure_time>='7:00:00' AND arrival_time<='18:00:00'
			)
			 
		SELECT DISTINCT ON(route_id) route_id, list(stop_id ORDER BY stop_sequence) FROM candidate_stops
		JOIN filtered_trips USING (trip_id)
		GROUP BY (trip_id, route_id)
		HAVING COUNT(stop_id)>$stops_threshold_line
		ORDER BY COUNT(stop_id) DESC
		LIMIT $nb_lines;"""
	else:
		raise KeyError(f"{area_mode} area mode type unknown")
	db_connector.con.execute(query,
		{
			"reduced_roads_path":str(reduced_roads_path),
			"stops_threshold_line": stops_threshold_line,
			"nb_lines": nb_lines_max
		}
	)
	return None

def write_bus_lines_list_csv(
		output:list[tuple[str, str, list[str]]],
		area_mode:Area_Mode
)->None:
	lines_file_name = REDUCED_DATA_PATH/"lines.txt"

	with open(lines_file_name, "w") as lines_file:
		lines_file.write("name, r, g, b\n")
		for line in output:
			if isinstance(area_mode, IDF_Area_Mode):
				rgb = convert_hex_into_rgb(line[1])
			else:
				rgb = np.random.default_rng(abs(hash(line[0]))).integers(0, 255, 3, endpoint=True)
			lines_file.write(f"{line[0]}, {rgb[0]}, {rgb[1]}, {rgb[2]}\n")
	return None

def convert_hex_into_rgb(
		hex:str
)->tuple[int, int, int]:
	hex = hex.lower()
	r = int(f"0x{(hex[0]+hex[1])}", 0)
	g = int(f"0x{(hex[2]+hex[3])}", 0)
	b = int(f"0x{(hex[4]+hex[5])}", 0)
	
	return (r, g, b)

def write_bus_lines_csv(
		output:list[tuple[str, str,list[str]]],
		area_mode:Area_Mode
)->None:
	for line in output:
		bus_line_path = REDUCED_DATA_PATH/f"{line[0]}.txt"
		fill_stop_into_bus_line_csv(bus_line_path, line, area_mode)
	return None

def fill_stop_into_bus_line_csv(
		file_path:Path,
		line:tuple[str,str, list[str]],
		area_mode:Area_Mode
)->None:
	with open(file_path, "w") as f:
		f.write(f"{line[0]}\n")
		for stop in line[1+isinstance(area_mode, IDF_Area_Mode)]:
			f.write(f"{stop}\n")
	return None