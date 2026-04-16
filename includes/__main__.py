import duckdb as dd
from pathlib import Path
from dataclasses import dataclass
from typing import Iterable
from dotenv import load_dotenv
import abc
import os
import re
from urllib.request import urlretrieve
import zipfile
import getopt, sys

class DBConnector(abc.ABC):
	def __init__(self) -> None:
		super().__init__()
		self.con = dd.connect()
		self.name = None
		self.path = None
		self.prefix = None
	
	@abc.abstractmethod
	def connect(self):
		pass

	def show(self) -> None:
		self.con.sql("SHOW ALL TABLES;").show()
		return None

class DuckDB_Connector(DBConnector):
	def __init__(
			self, 
			path:Path|None=None,
			) -> None:
		
		super().__init__()
		self.path = path or PROJECT_ROOT / "gama_project.db"
		self.connect()

		return None
	
	def connect(self)->None:
		self.con = dd.connect(self.path)
		return None

class SQLite_Connector(DBConnector):
	def __init__(
			self,
			name:str|None=None,
			path:Path|None=None
			) -> None:
		super().__init__()
		self.name = name or "gama_project_sqlite"
		self.path = path or PROJECT_ROOT / "gama_project_sqlite.db"
		self.prefix = "main"
		
	def connect(self)->None:
		self.con = dd.connect()
		self.con.execute("INSTALL sqlite;")
		self.con.execute("LOAD sqlite;")
		
		self.con.execute(f"""
				ATTACH '{self.name}' AS {self.name} (TYPE sqlite);
				""")
		
		self.con.execute("""USE $1;""", [self.name])
		return None
	

class MySQL_Connector(DBConnector):
	def __init__(
			self, 
			name:str|None=None,
			host:str|None=None,
			user:str|None=None
			) -> None:
		
		super().__init__()
		load_dotenv()
		self.name = name or "mysql_db"
		self.prefix = f"{self.name}.{self.name}"

		self.host = host or "127.0.0.1"
		self.user = user or "root"
		self.port = int(os.environ["PORT1"])
		self.database = os.environ["DATABASE"]

		self.connect()

		return None
	
	def connect(self):
		# environment file to handle MySQL database
		self.con = dd.connect()
		self.con.execute("INSTALL mysql;")
		self.con.execute("LOAD mysql;")
		self.con.execute(
			"""
				CREATE SECRET (
					TYPE mysql,
					HOST $host,
					PORT $port,
					DATABASE $database,
					USER $user,
					PASSWORD $passwd
				);
			""",
			{
				"host":self.host,
				"port":self.port,
				"database":self.database,
				"user":self.user,
				"passwd":os.environ["PASSWD"],
			}
		)
		# because no prepared available in this case, we check string validity with a RegExp
		if not re.match(r'^[a-zA-Z0-9_]+$', self.database): raise ValueError("Invalid database name")
		self.con.execute(f"""
				ATTACH 'host=localhost user=root port={self.port} database={self.database}' AS {self.name} (TYPE mysql);
				""")
		self.con.execute(f"""USE {self.name};""")
		return None

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

def read_cli_option()->tuple[bool, DBConnector]:

	verbose = False
	db = DuckDB_Connector()

	args = sys.argv[1:]
	options = "v"
	long_options = ["verbose", "duckdb", "mysql", "sqlite"]

	dict_options = {
		"--duckdb":DuckDB_Connector,
		"--mysql":MySQL_Connector,
		"--sqlite":SQLite_Connector
	}

	try:
		arguments, _ = getopt.getopt(args, options, long_options)
		for currentArg, _ in arguments:
			if currentArg in ("-v", "--verbose"):
				verbose = True
			if currentArg in dict_options:
				db = dict_options[currentArg]()
			
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

default_box_10 = box(2.34781, 2.37206, 48.86500, 48.88456)

def create_tables(
		db_connector:DBConnector,
		input_path:str|Path|None=None,
		tables:Iterable[str]|None=None,
		verbose:bool=False,
		box:box|None=None
	)->None:
	
	if input_path is None:
		input_path = GTFS_REPERTORY_PATH
	else:
		if isinstance(input_path, str): input_path = Path(input_path)
		if not input_path.exists():
			raise BaseException(f"input path {input_path} does not exist.")
		
	if tables is None:
		tables = map((lambda x: x.stem), input_path.rglob('*.txt'))

	for table in tables:
		if "stops" == table and box is not None:
			create_table_query = f"""
				CREATE OR REPLACE TABLE {f"{db_connector.prefix}.{table}" if db_connector.prefix is not None else table} AS 
					SELECT * FROM read_csv($input_file)
					WHERE lon>=$left AND lon<=$right AND stop_lat>=$bottom AND stop_lat<=$top;
			"""
			db_connector.con.execute(create_table_query, parameters={
				"table": f"{db.prefix}.{table}" if db.prefix is not None else table,
				"input_file": f"{GTFS_REPERTORY_PATH/table}.txt",
				"left": box.left, 
				"right": box.right, 
				"bottom": box.bottom, 
				"top": box.top})
		else:
			if verbose:
				print(table)
			create_table_query = f"""
				CREATE OR REPLACE TABLE {f"{db_connector.prefix}.{table}" if db_connector.prefix is not None else table} AS 
					SELECT * FROM read_csv($input_file)
			"""
			db_connector.con.execute(create_table_query, parameters={
				"input_file": f"{GTFS_REPERTORY_PATH/table}.txt",
				})
		
	if verbose:
		db_connector.show()
	return None

def reduce_shapefiles(
		db_connector:DBConnector,
		box:box,
	)->None:

	db_connector.con.execute("INSTALL spatial;")
	db_connector.con.execute("LOAD spatial;")
	reduce_roads(box, db_connector)
	reduce_stops(box, db_connector)
	reduce_buildings(box, db_connector)

	return None

def reduce_roads(
		box:box,
		db_connector:DBConnector,
		roads_path:Path|str|None=None,
		reduced_roads_path:Path|str|None=None,
		last_road_type_code:int=5135,
	)->None:

	if roads_path is None:
		roads_path = SHAPEFILE_REPERTORY_PATH / "gis_osm_roads_free_1.shp"
	if reduced_roads_path is None:
		clear_files(REDUCED_DATA_PATH, "*reduced_roads*")
		reduced_roads_path = REDUCED_DATA_PATH / "reduced_roads.shp"
	db_connector.con.execute(
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
		db_connector:DBConnector,
		building_path:Path|str|None=None,
		reduced_building_path:Path|str|None=None,
		apartments_office_only:bool=False
)->None:
	
	if building_path is None:
		building_path = SHAPEFILE_REPERTORY_PATH / "gis_osm_buildings_a_free_1.shp"
	if reduced_building_path is None:
		clear_files(REDUCED_DATA_PATH, "*reduced_buildings*")
		reduced_building_path = REDUCED_DATA_PATH / "reduced_buildings.shp"

	db_connector.con.execute(
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
		db_connector:DBConnector,
		reduced_stops_path:Path|str|None=None,
)->None:
	if reduced_stops_path is None:
		clear_files(REDUCED_DATA_PATH, "*reduced_stops*")
		reduced_stops_path = REDUCED_DATA_PATH / "reduced_stops.shp"
		
	db_connector.con.execute(
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
	db_connector:DBConnector,
	stops_threshold_line:int=20,
	nb_lines_max:int=100,
):
	request_bus_stop_in_box(box, db_connector, stops_threshold_line, nb_lines_max)
	output = db_connector.con.fetchall()
	write_bus_lines_list_csv(output)
	write_bus_lines_csv(output)

	return None

def request_bus_stop_in_box(
		box:box,
		db_connector:DBConnector,
		stops_threshold_line:int=20,
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
				WHERE CAST (departure_time[1:2] AS INT)>=7 AND CAST(arrival_time[1:2] AS INT)<=18
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
	create_tables(db, verbose=verbose)
	
	reduce_shapefiles(db, default_box_10)
	write_bus_stop_csv(default_box_10, db, stops_threshold_line=5)
