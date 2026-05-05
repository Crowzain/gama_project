from config import *
from DB_Connectors import DBConnector

def create_reduced_data_repertory()->None:
	if not REDUCED_DATA_PATH.exists():
		Path.mkdir(REDUCED_DATA_PATH)
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
	reduce_intersections(box, db_connector)

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
			COPY (SELECT osm_id, code, name, maxspeed, geom, ST_Length_Spheroid(geom) AS length, oneway, fclass FROM ST_ReadSHP($input_file)
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

def reduce_intersections(
		box:box,
		db_connector:DBConnector,
		intersections_path:Path|str|None=None,
		reduced_intersections_path:Path|str|None=None,
)->None:
	if intersections_path is None:
		intersections_path = SHAPEFILE_REPERTORY_PATH / "gis_osm_traffic_free_1.shp"
	if reduced_intersections_path is None:
		clear_files(REDUCED_DATA_PATH, "*reduced_intersections*")
		reduced_intersections_path = REDUCED_DATA_PATH / "reduced_intersections.shp"
	
	db_connector.con.execute(
		f"""
			COPY (SELECT * FROM ST_ReadSHP($input_file)
			WHERE ST_Contains(ST_MakeEnvelope($left, $bottom, $right, $top), geom) 
			AND code BETWEEN 5201 AND 5203)
			TO $output_file
			WITH (FORMAT gdal, DRIVER 'ESRI Shapefile', LAYER_CREATION_OPTIONS 'WRITE_BBOX=YES', SRS 'EPSG:4326');
		""",
		{
			"input_file": str(intersections_path),
			"output_file": str(reduced_intersections_path),
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