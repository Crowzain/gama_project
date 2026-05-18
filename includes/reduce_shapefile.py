from config import *
from DB_Connectors import DBConnector

import osmnx
import pandas as pd
import geopandas as gpd
import shapely

osmnx.settings.use_cache=False

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
	reduce_stops(db_connector, box)
	reduce_network(db_connector, box)
	reduce_buildings(db_connector, box)

	return None

def reduce_network(
		db_connector:DBConnector,
		box:box,
		reduced_roads_path:Path|str|None=None,
		reduced_intersections_path:Path|str|None=None,
	)->None:

	intersection_gdf, road_gdf = create_gdfs(box)
	intersection_gdf.index = intersection_gdf.index.astype("str")

	export_gdfs_to_shapefiles(intersection_gdf, road_gdf, reduced_roads_path, reduced_intersections_path)
	#intersection_gdf, road_gdf = add_stops_to_gdfs(db_connector, intersection_gdf, road_gdf)
	#export_gdfs_to_shapefiles(intersection_gdf, road_gdf, reduced_roads_path, reduced_intersections_path)

	return None

def create_gdfs(box:box)->tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
	graph = osmnx.graph.graph_from_bbox(
		bbox=(
			box.left,
			box.bottom,
			box.right,
			box.top),
			network_type="drive",
			simplify=False
		)
	return osmnx.convert.graph_to_gdfs(graph)

def add_stops_to_gdfs(
		db_connector:DBConnector,
		intersection_gdf:gpd.GeoDataFrame,
		road_gdf:gpd.GeoDataFrame,
		reduced_stops_path:Path|str|None=None,
		)->tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
	
	if reduced_stops_path is None:
		reduced_stops_path = REDUCED_DATA_PATH / "reduced_stops.shp"
	
	stops_gdf = gpd.read_file(reduced_stops_path)

	rows_to_add = []
	indices_to_drop = []

	for stop in stops_gdf.itertuples():
		intersection_gdf = insert_stop_into_intersection_gdf(stop, intersection_gdf)
		insert_detour_via_stop_into_road_gdf(db_connector, stop, road_gdf, rows_to_add, indices_to_drop)
	
	road_gdf = road_gdf.drop(index=indices_to_drop)
	gdf_with_inserted_row = gpd.GeoDataFrame(rows_to_add, crs=4326)
	road_gdf = pd.concat([road_gdf, gdf_with_inserted_row])
	return intersection_gdf, road_gdf

def insert_stop_into_intersection_gdf(
		stop, 
		intersection_gdf:gpd.GeoDataFrame
)->gpd.GeoDataFrame:
	new_row = gpd.GeoDataFrame({
		"stop_id":stop.stop_id,
		"geometry": [stop.geometry], 
		"y":[stop.geometry.y], 
		"x":[stop.geometry.x],
		"street_count":gpd.np.nan,
		"highway":"stop",
		"junction":gpd.np.nan,
		"ref":gpd.np.nan
		}, crs=4326)
	new_row = new_row.set_index("stop_id")
	intersection_gdf = pd.concat([intersection_gdf, new_row])
	return intersection_gdf

def insert_detour_via_stop_into_road_gdf(
		db_connector:DBConnector,
		stop,
		road_gdf:gpd.GeoDataFrame,
		rows_to_add:list[gpd.GeoSeries],
		indices_to_drop:list[tuple[int, int, int]]
)->None:
	edges_list = get_associated_edges_with_stop(db_connector, stop)
	if len(edges_list)>0:
		split_linestrings_list = split_edges(stop, edges_list, road_gdf)

		for edge, linestring in zip(edges_list, split_linestrings_list):
			original = road_gdf.loc[edge[0], edge[1], edge[2]].copy()

			a = original.copy()
			a["geometry"] = linestring[0]
			a.name = (a.name[0], stop.stop_id, a.name[2])
			
			b = original.copy()
			b["geometry"] = linestring[1]
			b.name = (stop.stop_id, b.name[1], b.name[2])
			
			rows_to_add.append(a)
			rows_to_add.append(b)
			indices_to_drop.append((edge[0], edge[1], edge[2]))
	return None

def get_associated_edges_with_stop(
		db_connector:DBConnector,
		stop,
		reduced_roads_path:Path|str|None=None,
		tolerance:float=0.01,
		length:float=5.0
	)->list[tuple[int,int, int]]:
	
	if reduced_roads_path is None:
		reduced_roads_path = REDUCED_DATA_PATH / "reduced_roads.shp"
	
	list_of_edges_touching_stop = db_connector.con.execute(
			"""
				SELECT u,v, key FROM ST_ReadSHP($road_file)
				WHERE ST_DWithin_GEOS(geom, ST_Point($x, $y), $tolerance) AND length>$length
				ORDER BY ST_Distance(ST_Point($x, $y), geom)
				LIMIT 2;
			""", parameters={"x":stop.geometry.x, "y":stop.geometry.y, "road_file": str(reduced_roads_path), "tolerance": tolerance, "length": length}
		).fetchall()
	edges_touching_stop_number = len(list_of_edges_touching_stop)
	if edges_touching_stop_number>0:
		u_first_closest = list_of_edges_touching_stop[0][0]
		v_first_closest = list_of_edges_touching_stop[0][1]
		if edges_touching_stop_number == 2:
			u_second_closest = list_of_edges_touching_stop[1][0]
			v_second_closest = list_of_edges_touching_stop[1][1]
			if u_first_closest != v_second_closest and v_first_closest != u_second_closest:
				list_of_edges_touching_stop.pop()
	return list_of_edges_touching_stop

def split_edges(
	stop,
	edges_list:list[tuple[int,int]],
	road_gdf:gpd.GeoDataFrame,
	tolerance:float=0.0001
)->list[tuple[shapely.LineString, shapely.LineString]]:
	split_linestrings_list = []
	for edge in edges_list:
		edge_geometry = road_gdf.loc[edge].geometry
		snapped_edge_geometry = shapely.snap(edge_geometry, stop.geometry, tolerance)
		split_linestrings_list.append(split_linestring(edge_geometry, snapped_edge_geometry))
	return split_linestrings_list

def split_linestring(
	original_linestring:shapely.LineString,
	snapped_linestring:shapely.LineString,
)->tuple[shapely.LineString, shapely.LineString]:
	
	original_coords_list = original_linestring.coords[:]
	snapped_coords_list = snapped_linestring.coords[:]
	if len(original_coords_list) == 2:
		return split_segment(original_coords_list, snapped_coords_list)
	else:
		return split_curve(original_coords_list, snapped_coords_list)

def split_segment(
		original_coords_list:list[tuple[float, float]],
		snapped_coords_list:list[tuple[float, float]]
)->tuple[shapely.LineString, shapely.LineString]:
	snapped_coords_index = 1 if original_coords_list[0] == snapped_coords_list[0] else 0
	first_linestring = shapely.LineString([original_coords_list[0], snapped_coords_list[snapped_coords_index]])
	second_linestring = shapely.LineString([snapped_coords_list[snapped_coords_index], original_coords_list[1]])
	return first_linestring, second_linestring

def split_curve(
	original_coords_list:list[tuple[float, float]],
	snapped_coords_list:list[tuple[float, float]]
)->tuple[shapely.LineString, shapely.LineString]:
	for original_coords, modified_coords in zip(original_coords_list, snapped_coords_list):
		if original_coords != modified_coords:
			first_linestring, second_linestring = shapely.ops.split(shapely.LineString(original_coords_list), shapely.Point(original_coords)).geoms
			second_linestring = shapely.LineString([modified_coords]+second_linestring.coords)
			return first_linestring, second_linestring

def export_gdfs_to_shapefiles(
		intersection_gdf:gpd.GeoDataFrame,
		road_gdf:gpd.GeoDataFrame,
		reduced_roads_path:Path|str|None=None,
		reduced_intersections_path:Path|str|None=None,
)->None:
	
	if reduced_roads_path is None:
		clear_files(REDUCED_DATA_PATH, "*reduced_roads*")
		reduced_roads_path = REDUCED_DATA_PATH / "reduced_roads.shp"

	if reduced_intersections_path is None:
		clear_files(REDUCED_DATA_PATH, "*reduced_intersections*")
		reduced_intersections_path = REDUCED_DATA_PATH / "reduced_intersections.shp"

	intersection_gdf.drop(["street_count", "highway", "junction"], axis=1).to_file(reduced_intersections_path)

	road_gdf.drop(["junction", "bridge", "tunnel"], axis=1).to_file(reduced_roads_path)
	return None

def reduce_buildings(
		db_connector:DBConnector,
		box:box,
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
		db_connector:DBConnector,
		box:box,
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
		db_connector:DBConnector,
		box:box,
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