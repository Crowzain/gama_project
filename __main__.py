import duckdb as dd
from pathlib import Path
from dataclasses import dataclass
from typing import Iterable
from dotenv import load_dotenv
import os
import re

load_dotenv()

# define paths
PROJECT_ROOT = Path(".")
REDUCED_DATA_PATH = PROJECT_ROOT / "reduced_data"
SHAPEFILE_REPERTORY_PATH = PROJECT_ROOT / "/ile-de-france-260112-free.shp"
GTFS_REPERTORY_PATH = PROJECT_ROOT / "IDFM-gtfs"

DATABASE_PATH = PROJECT_ROOT / "gama_project.db"

@dataclass(frozen=True)
class box:

	# Attributes Declaration
	# using Type Hints
	left: float
	right: float
	
	bottom: float
	top: float

default_box_13 = box(2.2577, 2.4115, 48.8186, 48.8988)
default_box_10 = box(2.34425, 2.37996, 48.86866, 48.88444)


def create_db_duckdb(
		db_path:Path|str|None=None,
		insert_gtfs:bool=True,
		verbose:bool=False,
		box:box|None=None
	)->None:
	
	if db_path is None:
		db_path = DATABASE_PATH
	else:
		if isinstance(db_path, str):
			db_path = Path(db_path)
	con = dd.connect(db_path)

	if insert_gtfs:
		create_tables_duckdb(db_path, con, verbose=verbose, box=box)

	return None


def create_tables_duckdb(
		db_path:Path|str|None=None,
		con:dd.DuckDBPyConnection|None=None,
		input_path:str|Path|None=None,
		tables:Iterable[str]|None=None,
		verbose:bool=False,
		box:box|None=None
	)->None:
	
	if db_path is None:
		db_path = DATABASE_PATH
	else:
		if isinstance(db_path, str):
			db_path = Path(db_path)
		if not db_path.exists():
			raise Exception("Error: database does not exist, create it.")
	if con is None:
		con = dd.connect(db_path, read_only=False)

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
				CREATE OR REPLACE TABLE {f"mysql_db.mysql_db.{table}"} AS SELECT * FROM read_csv('{f'{GTFS_REPERTORY_PATH/table}.txt'}')
				WHERE lon>=$left AND lon<=$right AND lat>=$bottom AND lat<=$top;
			"""
			con.execute(create_table_query, parameters={
				"left": box.left, 
				"right": box.right, 
				"bottom": box.bottom, 
				"top": box.top})
		else:
			print(f"mysql_db.{table}")
			create_table_query = f"""
				CREATE OR REPLACE TABLE {f"mysql_db.mysql_db.{table}"} AS SELECT * FROM read_csv('{f'{GTFS_REPERTORY_PATH/table}.txt'}')
			"""
			con.execute(create_table_query)
		
	if verbose:
		con.sql(f"SHOW ALL TABLES;").show()

	return None




def connect_mysql_db(
		insert_gtfs:bool=True,
		verbose:bool=False,
		box:box|None=None
	)->dd.DuckDBPyConnection:
	
	
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
			ATTACH 'host=localhost user=root port={port} database={database}' AS mysql_db (TYPE mysql);
			""")
	con = con.execute("""USE mysql_db;""")

	if insert_gtfs:
		con = create_tables(con, verbose=verbose, box=box)

	return con


def create_tables(
		con:dd.DuckDBPyConnection,
		input_path:str|Path|None=None,
		tables:Iterable[str]|None=None,
		verbose:bool=False,
		box:box|None=None
	)->dd.DuckDBPyConnection:
		
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
				CREATE OR REPLACE TABLE {f"mysql_db.mysql_db.{table}"} AS SELECT * FROM read_csv('{f'{GTFS_REPERTORY_PATH/table}.txt'}')
				WHERE lon BETWEEN $left AND $right 
				AND lat BETWEEN $bottom AND $top;
			"""
			con.execute(create_table_query, parameters={
				"left": box.left, 
				"right": box.right, 
				"bottom": box.bottom, 
				"top": box.top})
		else:
			print(f"mysql_db.{table}")
			create_table_query = f"""
				CREATE OR REPLACE TABLE {f"mysql_db.mysql_db.{table}"} AS SELECT * FROM read_csv('{f'{GTFS_REPERTORY_PATH/table}.txt'}')
			"""
			con.execute(create_table_query)
		
	if verbose:
		con.sql(f"SHOW ALL TABLES;").show()

	return con


def reduce_shapefiles(
		box:box,
	)->None:
	con = dd.connect(DATABASE_PATH)
	con.execute("INSTALL spatial;")
	con.execute("LOAD spatial;")
	reduce_roads(box, con)
	#reduce_bus_stop(box, con)
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
		con = dd.connect(DATABASE_PATH)
		con.execute("INSTALL spatial;")
		con.execute("LOAD spatial;")

	if roads_path is None:
		roads_path = SHAPEFILE_REPERTORY_PATH / "gis_osm_roads_free_1.shp"
	if reduced_roads_path is None:
		reduced_roads_path = REDUCED_DATA_PATH / "reduced_roads.shp"
	print(str(roads_path))
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
		con = dd.connect(DATABASE_PATH)
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


def get_reduce_bus_stop(
		box:box,
		con:dd.DuckDBPyConnection,
		stops_threshold_line:int=20,
		nb_lines_max:int=10,
	)->None:
	
	
	con = dd.connect(DATABASE_PATH)
	#stop_path = SHAPEFILE_REPERTORY_PATH / "gis_osm_buildings_a_free_1.shp"
	#reduced_path_path = REDUCED_DATA_PATH / "reduced_stop.shp"

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
	print(con.fetch_df())
	return None

con = connect_mysql_db(insert_gtfs=False, verbose=False)
con2 = dd.connect(DATABASE_PATH)
get_reduce_bus_stop(default_box_10, con)
get_reduce_bus_stop(default_box_10, con2)


#reduce_shapefiles(default_box_10)
