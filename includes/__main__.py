import duckdb as dd
from pathlib import Path
from dataclasses import dataclass
from typing import Iterable
from dotenv import load_dotenv
import os
import re
from enum import Enum

load_dotenv()

# define paths
PROJECT_ROOT = Path(".")
REDUCED_DATA_PATH = PROJECT_ROOT / "reduced_data"
SHAPEFILE_REPERTORY_PATH = PROJECT_ROOT / "/ile-de-france-260112-free.shp"
GTFS_REPERTORY_PATH = PROJECT_ROOT / "IDFM-gtfs"

@dataclass(frozen=True)
class box:

	# Attributes Declaration
	# using Type Hints
	left: float
	right: float
	
	bottom: float
	top: float

class DB_TYPE(Enum):
	DUCKDB = 0,
	MY_SQL = 1,
	SQLITE = 2

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

default_box_13 = box(2.2577, 2.4115, 48.8186, 48.8988)
default_box_10 = box(2.34425, 2.37996, 48.86866, 48.88444)

def connect_db(
		db_type:DB_TYPE,
	)->dd.DuckDBPyConnection:
	
	
	

	match db_type:
		case DB_TYPE.SQLITE:
			con = dd.connect(DB_PATH_DICT["SQLITE"])
			con = con.execute("INSTALL sqlite;")
			con = con.execute("LOAD sqlite;")
		case DB_TYPE.MY_SQL:
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

			if not re.match(r'^[a-zA-Z0-9_]+$', database): raise ValueError("Invalid database name")
			con = con.execute(f"""
					ATTACH 'host=localhost user=root port={port} database={database}' AS {DB_NAME_DICT["MY_SQL"]} (TYPE mysql);
					""")
			con = con.execute("""USE mysql_db;""")
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
	)->dd.DuckDBPyConnection:
	
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
			create_table_query = f"""
				CREATE OR REPLACE TABLE $table AS SELECT * FROM read_csv($input_file)
				WHERE lon>=$left AND lon<=$right AND stop_lat>=$bottom AND stop_lat<=$top;
			"""
			con.execute(create_table_query, parameters={
				"table": f"{prefix[db_type.value]}.{table}" if db_type!=DB_TYPE.DUCKDB else table,
				"input_file": f"{GTFS_REPERTORY_PATH/table}.txt",
				"left": box.left, 
				"right": box.right, 
				"bottom": box.bottom, 
				"top": box.top})
		else:
			print(f"{DB_NAME_DICT[db_type.name]}.{table}")
			create_table_query = f"""
				CREATE OR REPLACE TABLE {f"{prefix[db_type.value]}.{table}" if db_type!=DB_TYPE.DUCKDB else table} AS 
				SELECT * FROM read_csv('{f'{GTFS_REPERTORY_PATH/table}.txt'}', delim=','); 
			"""
			con.execute(create_table_query)
		
	if verbose:
		con.sql(f"SHOW ALL TABLES;").show()
	return con


def reduce_shapefiles(
		box:box,
	)->None:
	con = dd.connect()
	con.execute("INSTALL spatial;")
	con.execute("LOAD spatial;")
	reduce_roads(box, con)
	reduce_stops(box, con)
	reduce_buildings(box, con)

	return None


def reduce_roads(
		box:box,
		con:dd.DuckDBPyConnection|None=None,
		roads_path:Path|str|None=None,
		reduced_roads_path:Path|str|None=None,
		last_road_type_code:int=5135,
	)->None:
	if con is None:
		con = dd.connect()
	con.execute("INSTALL spatial;")
	con.execute("LOAD spatial;")

	if roads_path is None:
		roads_path = SHAPEFILE_REPERTORY_PATH / "gis_osm_roads_free_1.shp"
	if reduced_roads_path is None:
		reduced_roads_path = REDUCED_DATA_PATH / "reduced_roads.shp"
	con.execute(
		f"""
			COPY (SELECT * FROM ST_ReadSHP($input_file)
			WHERE ST_Contains(ST_MakeEnvelope($left, $bottom, $right, $top), geom) AND
			code <= $last_road_type_code) TO $output_file
			WITH (FORMAT gdal, DRIVER 'ESRI Shapefile', LAYER_CREATION_OPTIONS 'WRITE_BBOX=YES', SRS 'EPSG:4326');
		""",
		{
			"input_file": "."+str(roads_path),
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
		con:dd.DuckDBPyConnection|None=None,
		building_path:Path|str|None=None,
		reduced_building_path:Path|str|None=None,
		apartments_office_only:bool=False
)->None:
	if con is None:
		con = dd.connect()
	con.execute("INSTALL spatial;")
	con.execute("LOAD spatial;")
	
	if building_path is None:
		building_path = SHAPEFILE_REPERTORY_PATH / "gis_osm_buildings_a_free_1.shp"
	if reduced_building_path is None:
		reduced_building_path = REDUCED_DATA_PATH / "reduced_buildings.shp"

	con.execute(
		f"""
			COPY (SELECT * FROM ST_ReadSHP($input_file)
			WHERE ST_Contains(ST_MakeEnvelope($left, $bottom, $right, $top), geom)
			{"AND type in['apartments', 'office']" if apartments_office_only else ""}) TO $output_file
			WITH (FORMAT gdal, DRIVER 'ESRI Shapefile', LAYER_CREATION_OPTIONS 'WRITE_BBOX=YES', SRS 'EPSG:4326');
		""",
		{
			"input_file": "."+str(building_path),
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
		con:dd.DuckDBPyConnection|None=None,
		reduced_stops_path:Path|str|None=None,
)->None:
	if con is None:
		con = dd.connect()
	con.execute("INSTALL spatial;")
	con.execute("LOAD spatial;")

	if reduced_stops_path is None:
		reduced_stops_path = REDUCED_DATA_PATH / "reduced_stops.shp"
	
	con.execute(
		f"""
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


def get_reduce_bus_stop(
		box:box,
		con:dd.DuckDBPyConnection|None,
		stops_threshold_line:int=20,
		nb_lines_max:int=100,
		write_file:bool=False
	)->None:
	
	if con is None:
		con = connect_db(DB_TYPE.DUCKDB)
	con.execute("INSTALL spatial;")
	con.execute("LOAD spatial;")

	con.execute("""
		SELECT DISTINCT ON(route_id) route_id, LIST(stop_id) AS stop_sequence FROM trips
  		NATURAL JOIN (SELECT * FROM routes WHERE route_type=3 AND direction_id=0 AND NOT STARTS_WITH(route_long_name, 'N'))
  		NATURAL JOIN (SELECT * FROM stop_times 
						WHERE stop_id in (SELECT stop_id FROM stops 
										WHERE stop_lat BETWEEN $bottom AND $top
										AND stop_lon BETWEEN $left AND $right)
			 		)
		NATURAL JOIN (SELECT * FROM calendar WHERE monday=1)
		GROUP BY route_id 
		HAVING len(stop_sequence)>$stops_threshold_line
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
	output = con.fetchall()
	if write_file:
		new_file_lines = REDUCED_DATA_PATH/"lines.txt"
		with open(new_file_lines, "a+") as f1:
			f1.write("name\n")
			for line in output:
				new_file = REDUCED_DATA_PATH/f"{line[0]}.txt"
				with open(new_file, "a+") as f:
					f.write(f"{line[0]}\n")
					f1.write(f"{line[0]}\n")
					for stop in line[1]:
						f.write(f"{stop}\n")
	return None



con = connect_db(DB_TYPE.DUCKDB)
con2 = connect_db(DB_TYPE.MY_SQL)
con3 = connect_db(DB_TYPE.SQLITE)

#create_tables(DB_TYPE.DUCKDB, con)
#create_tables(DB_TYPE.MY_SQL, con2)
create_tables(DB_TYPE.SQLITE, con3)
#get_reduce_bus_stop(default_box_10, con, write_file=False)
#get_reduce_bus_stop(default_box_10, con2, write_file=False)
con3.sql("SHOW ALL TABLES;").show()
get_reduce_bus_stop(default_box_10, con3, write_file=False)


reduce_shapefiles(default_box_10)
