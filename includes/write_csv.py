from config import *
from DB_Connectors import DBConnector

def write_bus_stop_csv(
	box:box,
	db_connector:DBConnector,
	stops_threshold_line:int=20,
	nb_lines_max:int=100,
):
	query_bus_stop_in_box(box, db_connector, stops_threshold_line, nb_lines_max)
	output = db_connector.con.fetchall()
	write_bus_lines_list_csv(output)
	write_bus_lines_csv(output)

	return None

def query_bus_stop_in_box(
		box:box,
		db_connector:DBConnector,
		stops_threshold_line:int=5,
		nb_lines_max:int=100,
	)->None:
	db_connector.con.execute("""
		WITH 
			filtered_stops AS (
				SELECT stop_id
				FROM stops
				WHERE stop_lat BETWEEN $bottom AND $top
				AND stop_lon BETWEEN $left AND $right
			),
		
			candidate_services AS (
				SELECT trip_id FROM trips
				JOIN calendar USING(service_id)
				WHERE MONDAY AND TUESDAY AND WEDNESDAY AND THURSDAY AND FRIDAY
			),
			
			candidate_routes AS (
				SELECT route_id, route_color FROM routes
				WHERE route_type=3
			),
			
			filtered_trips AS (
				SELECT DISTINCT ON(trip_id) trip_id, route_id, route_color FROM trips
				JOIN candidate_services USING(trip_id)
				JOIN candidate_routes USING(route_id)
			),
			
			candidate_stops AS (
				SELECT stop_id, trip_id, stop_sequence FROM stop_times
				JOIN filtered_stops USING(stop_id)
				WHERE CAST (departure_time[1:2] AS INT)>=7 AND CAST(arrival_time[1:2] AS INT)<=18
			)
			 
		SELECT DISTINCT ON(route_id) route_id, ANY_VALUE(route_color), list(stop_id ORDER BY stop_sequence) FROM candidate_stops
		JOIN filtered_trips USING (trip_id)
		GROUP BY (trip_id, route_id)
		HAVING COUNT(stop_id)>$stops_threshold_line
		LIMIT $nb_lines;
		""",
		{
			"left": box.left, 
			"bottom": box.bottom, 
			"right": box.right, 
			"top": box.top,
			"stops_threshold_line": stops_threshold_line,
			"nb_lines": nb_lines_max
		}
	)
	return None

def write_bus_lines_list_csv(
		output:list[tuple[str, str, list[str]]],
)->None:
	lines_file_name = REDUCED_DATA_PATH/"lines.txt"

	with open(lines_file_name, "w") as lines_file:
		lines_file.write("name, r, g, b\n")
		for line in output:
			rgb = convert_hex_into_rgb(line[1])
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
)->None:
	for line in output:
		bus_line_path = REDUCED_DATA_PATH/f"{line[0]}.txt"
		fill_stop_into_bus_line_csv(bus_line_path, line)
	return None

def fill_stop_into_bus_line_csv(
		file_path:Path,
		line:tuple[str,str, list[str]]
)->None:
	with open(file_path, "w") as f:
		f.write(f"{line[0]}\n")
		for stop in line[2]:
			f.write(f"{stop}\n")
	return None