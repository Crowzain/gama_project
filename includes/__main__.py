import duckdb as dd
from pathlib import Path
from dataclasses import dataclass
from typing import Iterable
from dotenv import load_dotenv
import os
import re
from enum import Enum
from urllib.request import urlretrieve
import zipfile
import getopt, sys

class DB_TYPE(Enum):
	DUCKDB = 0
	MY_SQL = 1
	SQLITE = 2

# define paths
PROJECT_ROOT = Path(".")
SHAPEFILE_REPERTORY_PATH = PROJECT_ROOT / "ile-de-france-260112-free.shp"
GTFS_REPERTORY_PATH = PROJECT_ROOT / "IDFM-gtfs"

def create_reduced_data_repertory(
		repertory_name:str|Path|None=None	
	)->Path:
	if repertory_name is None: repertory_name = PROJECT_ROOT / "reduced_data"
	elif isinstance(repertory_name, str): repertory_name = PROJECT_ROOT / repertory_name
	
	if not repertory_name.exists():
		Path.mkdir(repertory_name)
	return repertory_name

REDUCED_DATA_PATH = create_reduced_data_repertory()

def read_cli_option()->tuple[bool, DB_TYPE]:

	verbose = False
	db = DB_TYPE.DUCKDB

	args = sys.argv[1:]
	options = "v"
	long_options = ["verbose", "duckdb", "mysql", "sqlite"]

	dict_options = {
		"--duckdb":DB_TYPE.DUCKDB,
		"--mysql":DB_TYPE.MY_SQL,
		"--sqlite":DB_TYPE.SQLITE
	}

	try:
		arguments, _ = getopt.getopt(args, options, long_options)
		for currentArg, _ in arguments:
			if currentArg in ("-v", "--verbose"):
				verbose = True
			if currentArg in dict_options:
				db = dict_options[currentArg]
			
	except getopt.error as err:
		print(str(err))
	
	return verbose, db

def import_data(
		gtfs_url:str|None=None,
		shapefile_url:str|None=None,
		verbose:bool=False,
		gtfs_zip_path:str|Path|None=None,
		shapefile_zip_path:str|Path|None=None,
	)->None:
	if not GTFS_REPERTORY_PATH.exists():
		import_gtfs(gtfs_url, gtfs_zip_path, verbose)
	elif verbose:
		print("GTFS repertory does already exist")
		
	if not SHAPEFILE_REPERTORY_PATH.exists():
		import_shapefiles(shapefile_url, shapefile_zip_path, verbose)
	elif verbose:
		print("Shapefiles repertory does already exist")
	
	return None

def import_gtfs(
		gtfs_url:str|None=None,
		gtfs_zip_path:str|Path|None=None,
		verbose:bool=False
		)->None:	
	
	if gtfs_url is None: gtfs_url = "https://eu.ftp.opendatasoft.com/stif/GTFS/IDFM-gtfs.zip"
	if gtfs_zip_path is None: gtfs_zip_path = PROJECT_ROOT /"IDFM-gtfs.zip"

	import_external_repertories(gtfs_url, gtfs_zip_path, verbose)
	return None
	
def import_shapefiles(
		shapefile_url:str|None=None,
		shapefile_zip_path:str|Path|None=None,
		verbose:bool=False
		)->None:
	
	if shapefile_url is None: shapefile_url = "https://download.geofabrik.de/europe/france/ile-de-france-latest-free.shp.zip"
	if shapefile_zip_path is None: shapefile_zip_path = PROJECT_ROOT /"ile-de-france-latest-free.shp.zip"

	import_external_repertories(shapefile_url, shapefile_zip_path, verbose)
	return None
	
def import_external_repertories(
		url:str, 
		zip_path:str|Path,
		verbose:bool=False
		)->None:
	
	_, headers = urlretrieve(url, zip_path)
	
	if verbose:
		for name, value in headers.items():
			print(name, value)
	
	with zipfile.ZipFile(zip_path, "r") as zip_ref:
		Path.mkdir(SHAPEFILE_REPERTORY_PATH, mode=0o755)
		zip_ref.extractall(SHAPEFILE_REPERTORY_PATH)
	
	return None

# create an immutable data strucure to store boundaries
@dataclass(frozen=True)
class box:

	# Attributes Declaration
	# using Type Hints
	left: float
	right: float
	
	bottom: float
	top: float

DB_NAME_DICT = {
	"DUCKDB": "",
	"MY_SQL": "mysql_db",
	"SQLITE" : "gama_project_sqlite"
}

DB_PATH_DICT = {
	"DUCKDB": PROJECT_ROOT / "gama_project.db",
	"MY_SQL": PROJECT_ROOT,
	"SQLITE" : PROJECT_ROOT / "gama_project_sqlite.db"
}

default_box_10 = box(2.34781, 2.37206, 48.86500, 48.88456)

def connect_db(
		db_type:DB_TYPE,
	)->dd.DuckDBPyConnection:

	match db_type:
		case DB_TYPE.SQLITE:
			con = dd.connect()
			con = con.execute("INSTALL sqlite;")
			con = con.execute("LOAD sqlite;")
			
			con = con.execute(f"""
					ATTACH '{str(DB_PATH_DICT["SQLITE"])}' AS {DB_NAME_DICT["SQLITE"]} (TYPE sqlite);
					""")
			
			con = con.execute(f"""USE {str(DB_NAME_DICT["SQLITE"])};""")
			
		case DB_TYPE.MY_SQL:
			# environment file to handle MySQL database
			load_dotenv()
			con = dd.connect()
			con = con.execute("INSTALL mysql;")
			con = con.execute("LOAD mysql;")
			con = con.execute("""
				CREATE SECRET (
				TYPE mysql,
				HOST '127.0.0.1',
				PORT $port,
				DATABASE $database,
				USER 'root',
				PASSWORD $passwd
			);
			""",
			{
				"port":os.environ["PORT1"],
				"database":os.environ["DATABASE"],
				"passwd":os.environ["PASSWD"],
			}
			)
			port = int(os.environ["PORT1"])
			database = os.environ["DATABASE"]

			# because no prepared available in this case, we check string validity with a RegExp
			if not re.match(r'^[a-zA-Z0-9_]+$', database): raise ValueError("Invalid database name")
			con = con.execute(f"""
					ATTACH 'host=localhost user=root port={port} database={database}' AS {DB_NAME_DICT["MY_SQL"]} (TYPE mysql);
					""")
			con = con.execute(f"""USE {str(DB_NAME_DICT["MY_SQL"])};""")
		case DB_TYPE.DUCKDB:
			con = dd.connect(DB_PATH_DICT[db_type.name])
		case _:
			con = dd.connect(DB_PATH_DICT[db_type.name])
	return con

def create_tables(
		db_type:DB_TYPE,
		con:dd.DuckDBPyConnection|None=None,
		input_path:str|Path|None=None,
		tables:Iterable[str]|None=None,
		verbose:bool=False,
		box:box|None=None
	)->None:
	
	if con is None:
		con = connect_db(db_type)

	if input_path is None:
		input_path = GTFS_REPERTORY_PATH
	else:
		if isinstance(input_path, str): input_path = Path(input_path)
		if not input_path.exists():
			raise BaseException(f"input path {input_path} does not exist.")
		
	if tables is None:
		tables = map((lambda x: x.stem), input_path.rglob('*.txt'))

	prefix = (None,
		   f"{DB_NAME_DICT["MY_SQL"]}.{DB_NAME_DICT["MY_SQL"]}",
		   f"{DB_NAME_DICT["SQLITE"]}.main"
	)
	for table in tables:
		if "stops" == table and box is not None:
			create_table_query = """
				CREATE OR REPLACE TABLE $table AS 
					SELECT * FROM read_csv($input_file)
					WHERE lon>=$left AND lon<=$right AND stop_lat>=$bottom AND stop_lat<=$top;
			"""
			con.execute(create_table_query, parameters={
				"table": f"{prefix[db_type.value]}.{table}" if db_type!=DB_TYPE.DUCKDB else table,
				"input_file": f"{GTFS_REPERTORY_PATH/table}.txt",
				"left": box.left, 
				"right": box.right, 
				"bottom": box.bottom, 
				"top": box.top})
		elif "stop_times" == table:
			
			create_table_query = f"""
				CREATE OR REPLACE TABLE {f"{prefix[db_type.value]}.{table}" if db_type!=DB_TYPE.DUCKDB else table} AS 
					SELECT * REPLACE (
							CAST(arrival_time AS TIME) AS arrival_time, 
							CAST(departure_time AS TIME) AS departure_time
							) 
					FROM read_csv($input_file)
					WHERE CAST (arrival_time[1:2] AS INT)<24 AND CAST (departure_time[1:2] AS INT)<24;
			"""
			con.execute(create_table_query, parameters={
				"input_file": f"{GTFS_REPERTORY_PATH/table}.txt"
				})
		else:
			if verbose:
				print(table)
			create_table_query = f"""
				CREATE OR REPLACE TABLE {f"{prefix[db_type.value]}.{table}" if db_type!=DB_TYPE.DUCKDB else table} AS 
					SELECT * FROM read_csv($input_file)
			"""
			con.execute(create_table_query, parameters={
				"input_file": f"{GTFS_REPERTORY_PATH/table}.txt",
				})
		
	if verbose:
		if db_type == DB_TYPE.DUCKDB:
			con.sql("SHOW ALL TABLES;").show()
		elif db_type == DB_TYPE.SQLITE:
			con.sql("SELECT name FROM sqlite_master WHERE type='table';").show()
		else:
			con.sql(f"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = {DB_NAME_DICT['MY_SQL']};").show()
	return None


def reduce_shapefiles(
		con:dd.DuckDBPyConnection,
		box:box,
	)->None:

	con.execute("INSTALL spatial;")
	con.execute("LOAD spatial;")
	reduce_roads(box, con)
	reduce_stops(box, con)
	reduce_buildings(box, con)

	return None


def reduce_roads(
		box:box,
		con:dd.DuckDBPyConnection,
		roads_path:Path|str|None=None,
		reduced_roads_path:Path|str|None=None,
		last_road_type_code:int=5135,
	)->None:

	if roads_path is None:
		roads_path = SHAPEFILE_REPERTORY_PATH / "gis_osm_roads_free_1.shp"
	if reduced_roads_path is None:
		clear_files(REDUCED_DATA_PATH, "*reduced_roads*")
		reduced_roads_path = REDUCED_DATA_PATH / "reduced_roads.shp"
	con.execute(
		"""
			COPY (SELECT osm_id, code, name, maxspeed, geom, ST_Length_Spheroid(geom) AS length FROM ST_ReadSHP($input_file)
			WHERE ST_Contains(ST_MakeEnvelope($left, $bottom, $right, $top), geom) AND
			code <= $last_road_type_code AND maxspeed>0) TO $output_file
			WITH (FORMAT gdal, DRIVER 'ESRI Shapefile', LAYER_CREATION_OPTIONS 'WRITE_BBOX=YES', SRS 'EPSG:4326');
		""",
		{
			"input_file": str(roads_path),
			"output_file": str(reduced_roads_path),
			"left": box.left, 
			"bottom": box.bottom, 
			"right": box.right, 
			"top": box.top,
			"last_road_type_code": last_road_type_code
		}
	)
	
	return None

def reduce_buildings(
		box:box,
		con:dd.DuckDBPyConnection,
		building_path:Path|str|None=None,
		reduced_building_path:Path|str|None=None,
		apartments_office_only:bool=False
)->None:
	
	if building_path is None:
		building_path = SHAPEFILE_REPERTORY_PATH / "gis_osm_buildings_a_free_1.shp"
	if reduced_building_path is None:
		clear_files(REDUCED_DATA_PATH, "*reduced_buildings*")
		reduced_building_path = REDUCED_DATA_PATH / "reduced_buildings.shp"

	con.execute(
		f"""
			COPY (SELECT * FROM ST_ReadSHP($input_file)
			WHERE ST_Contains(ST_MakeEnvelope($left, $bottom, $right, $top), geom)
			{"AND type in['apartments', 'office']" if apartments_office_only else ""}) TO $output_file
			WITH (FORMAT gdal, DRIVER 'ESRI Shapefile', LAYER_CREATION_OPTIONS 'WRITE_BBOX=YES', SRS 'EPSG:4326');
		""",
		{
			"input_file": str(building_path),
			"output_file": str(reduced_building_path),
			"left": box.left, 
			"bottom": box.bottom, 
			"right": box.right, 
			"top": box.top
		}
	)
	return None

def reduce_stops(
		box:box,
		con:dd.DuckDBPyConnection,
		reduced_stops_path:Path|str|None=None,
)->None:
	if reduced_stops_path is None:
		clear_files(REDUCED_DATA_PATH, "*reduced_stops*")
		reduced_stops_path = REDUCED_DATA_PATH / "reduced_stops.shp"
		
	con.execute(
		"""
			COPY (SELECT stop_id, ST_Point2D(stop_lon, stop_lat) AS geom FROM stops
			WHERE stop_lat BETWEEN $bottom AND $top
			AND stop_lon BETWEEN $left AND $right) TO $output_file
			WITH (FORMAT gdal, DRIVER 'ESRI Shapefile', LAYER_CREATION_OPTIONS 'WRITE_BBOX=YES', SRS 'EPSG:4326');
		""",
		{
			"output_file": str(reduced_stops_path),
			"left": box.left, 
			"bottom": box.bottom, 
			"right": box.right, 
			"top": box.top
		}
	)
	
	return None

def clear_files(
		repertory_path:Path,
		file_name_regexp:str
	)->None:
	for file in Path.glob(repertory_path, file_name_regexp):
		file.unlink()
	return None

def write_bus_stop_csv(
	box:box,
	con:dd.DuckDBPyConnection,
	stops_threshold_line:int=20,
	nb_lines_max:int=100,
):
	request_bus_stop_in_box(box, con, stops_threshold_line, nb_lines_max)
	output = con.fetchall()
	write_bus_lines_list_csv(output)
	write_bus_lines_csv(output)

	return None

def request_bus_stop_in_box(
		box:box,
		con:dd.DuckDBPyConnection,
		stops_threshold_line:int=20,
		nb_lines_max:int=100,
	)->None:
	
	con.execute("""
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
				SELECT route_id FROM routes
				WHERE route_type=3
			),
			
			filtered_trips AS (
				SELECT DISTINCT ON(trip_id) trip_id, route_id FROM trips
				JOIN candidate_services USING(trip_id)
				JOIN candidate_routes USING(route_id)
			),
			
			candidate_stops AS (
				SELECT stop_id, trip_id, stop_sequence FROM stop_times
				JOIN filtered_stops USING(stop_id)
				WHERE departure_time>make_time(7,0,0) AND arrival_time<=make_time(18,0,0)
			)
			 
		SELECT DISTINCT ON(route_id) route_id, list(stop_id ORDER BY stop_sequence) FROM candidate_stops
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
		output:list[tuple[str,list[str]]],
)->None:
	lines_file_name = REDUCED_DATA_PATH/"lines.txt"

	with open(lines_file_name, "w") as lines_file:
		lines_file.write("name\n")
		for line in output:
			lines_file.write(f"{line[0]}\n")

def write_bus_lines_csv(
		output:list[tuple[str,list[str]]],
)->None:
	for line in output:
		bus_line_path = REDUCED_DATA_PATH/f"{line[0]}.txt"
		fill_stop_into_bus_line_csv(bus_line_path, line)
	return None

def fill_stop_into_bus_line_csv(
		file_path:Path,
		line:tuple[str,list[str]]
)->None:
	with open(file_path, "w") as f:
		f.write(f"{line[0]}\n")
		for stop in line[1]:
			f.write(f"{stop}\n")

if __name__=="__main__":
	verbose, db = read_cli_option()
	import_data()
	con = connect_db(db)
	create_tables(db, con, verbose=verbose)
	write_bus_stop_csv(default_box_10, con, stops_threshold_line=5)


	reduce_shapefiles(con, default_box_10)
