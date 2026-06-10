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
		place:str|box,
	)->None:

	db_connector.con.execute("INSTALL spatial;")
	db_connector.con.execute("LOAD spatial;")
	wkb_enveloppe = build_network(db_connector, place)
	reduce_buildings(db_connector, wkb_enveloppe)

	return None

def build_network(
		db_connector:DBConnector,
		place:str|box,
		reduced_roads_path:Path|str|None=None,
		reduced_intersections_path:Path|str|None=None,
	)->bytes:
	if isinstance(place, str): 
		intersection_gdf, road_gdf = create_gdfs_from_place(place)
	elif isinstance(place, box):
		intersection_gdf, road_gdf = create_gdfs_from_box(place)
	else:
		raise TypeError("Provided input type is neither str nor box")
	#intersection_gdf.index = intersection_gdf.index.astype("str")

	wkb_enveloppe = get_wkb_enveloppe(road_gdf)

	reduce_stops(db_connector, road_gdf, wkb_enveloppe)
	export_gdfs_to_shapefiles(intersection_gdf, road_gdf, reduced_roads_path, reduced_intersections_path)
	return wkb_enveloppe

def create_gdfs_from_box(box:box)->tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
	graph = osmnx.graph.graph_from_bbox(
		bbox=(
			box.left,
			box.bottom,
			box.right,
			box.top
			),
		network_type="drive",
		simplify=False
	)
	return osmnx.convert.graph_to_gdfs(graph)


def create_gdfs_from_place(place:str)->tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
	graph = osmnx.graph.graph_from_place(
		place,
		network_type="drive",
		simplify=False
	)
	return osmnx.convert.graph_to_gdfs(graph)

def reduce_stops(
		db_connector:DBConnector,
		road_gdf:gpd.GeoDataFrame,
		wkb_enveloppe:bytes,
		reduced_stops_path:Path|str|None=None,
		stops_threshold_line:int=5,
		nb_lines_max:int=100,
)->gpd.GeoDataFrame:
	if reduced_stops_path is None:
		clear_files(REDUCED_DATA_PATH, "*reduced_stops*")
		reduced_stops_path = REDUCED_DATA_PATH / "reduced_stops.shp"
	stop_gdf = db_connector.con.execute("""
			WITH 
				filtered_stops AS (
					SELECT stop_id, stop_lat, stop_lon
					FROM stops
					WHERE ST_Contains(ST_GeomFromWKB($wkb_enveloppe), ST_Point(stop_lon, stop_lat))
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
					SELECT stop_id, trip_id, stop_sequence, stop_lon, stop_lat FROM stop_times
					JOIN filtered_stops USING(stop_id)
					WHERE CAST (departure_time[1:2] AS INT)>=7 AND CAST(arrival_time[1:2] AS INT)<=18
				),

				selected_lines AS (
					SELECT DISTINCT ON(route_id) route_id, list(stop_id) AS stop_ids FROM candidate_stops
					JOIN filtered_trips USING (trip_id)
					GROUP BY (trip_id, route_id)
					HAVING COUNT(stop_id)>$stops_threshold_line
					ORDER BY COUNT(stop_id) DESC
					LIMIT $nb_lines
				),
				
				unnested AS (
				SELECT DISTINCT unnest(stop_ids) AS stop_id
				FROM selected_lines
				)
				
				SELECT
					unnested.stop_id,
					fs.stop_lon AS x, 
					fs.stop_lat AS y
				FROM unnested
				JOIN filtered_stops fs USING (stop_id);
		""",
		{
			"wkb_enveloppe": wkb_enveloppe,
			"stops_threshold_line": stops_threshold_line,
			"nb_lines": nb_lines_max
		}
	).df()
	stop_gdf = gpd.GeoDataFrame(stop_gdf)
	stop_gdf.geometry = gpd.points_from_xy(stop_gdf['x'], stop_gdf['y'], crs=4326)
	rows_to_add = []
	indices_to_drop = []
	for stop in stop_gdf.itertuples():
		edges_list = get_associated_edges_with_stop(stop, road_gdf)
		insert_stop_into_road_gdf(stop, stop_gdf, road_gdf, edges_list, rows_to_add, indices_to_drop)			
	stop_gdf.to_file(reduced_stops_path)
	road_gdf = road_gdf.drop(index=indices_to_drop)
	gdf_with_inserted_row = gpd.GeoDataFrame(rows_to_add, crs=4326)
	road_gdf = pd.concat([road_gdf, gdf_with_inserted_row])
	return stop_gdf

def get_wkb_enveloppe(road_gdf:gpd.GeoDataFrame)->bytes:
	return shapely.to_wkb(road_gdf[["geometry"]].union_all().convex_hull)

def insert_stop_into_road_gdf(
		stop,
		stop_gdf:gpd.GeoDataFrame,
		road_gdf:gpd.GeoDataFrame,
		edges_list:list[tuple[int, int, int]],
		rows_to_add:list[gpd.GeoSeries],
		indices_to_drop:list[tuple[int, int, int]]
)->None:
	if len(edges_list)>0:

		split_linestrings_list = split_edges(stop, edges_list, stop_gdf, road_gdf)

		for edge, linestring in zip(edges_list, split_linestrings_list):
			original = road_gdf.loc[edge[0], edge[1], edge[2]].copy()

			a = original.copy()
			a["geometry"] = linestring[0]
			a["length"] = linestring[0].length
			a.names = (a.name[0], stop.stop_id, a.name[2])
			
			b = original.copy()
			b["geometry"] = linestring[1]
			b["length"] = linestring[1].length
			b.names = (stop.stop_id, b.name[1], b.name[2])
			
			rows_to_add.append(a)
			rows_to_add.append(b)
			indices_to_drop.append((edge[0], edge[1], edge[2]))
	return None

def get_associated_edges_with_stop(
		stop,
		road_gdf:gpd.GeoDataFrame,
		distance_from_linestring:float=0.001
	)->list[tuple[int,int, int]]:

	stop_point = stop.geometry

	mask = road_gdf.geometry.dwithin(stop_point, distance_from_linestring)
	candidates = road_gdf[mask].copy()
	candidates["dist"] = candidates.geometry.distance(stop_point)
	candidates = candidates.sort_values("dist")
	
	list_of_edges_touching_stop = list(candidates.index[:2])

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
	edges_list:list[tuple[int, int, int]],
	stop_gdf:gpd.GeoDataFrame,
	road_gdf:gpd.GeoDataFrame
)->list[tuple[shapely.LineString, shapely.LineString]]:
	split_linestrings_list = []
	
	for edge in edges_list:
		distance = road_gdf.loc[edge[0], edge[1], edge[2]].geometry.project(stop.geometry)
		projection_on_edge = road_gdf.loc[edge[0], edge[1], edge[2]].geometry.interpolate(distance)
		
		stop_gdf.loc[stop.Index, "geometry"] = projection_on_edge
		edge_geometry = road_gdf.loc[edge].geometry
		split_linestring_pair = split_linestring(stop, edge_geometry)
		if split_linestring_pair is not None:
			split_linestrings_list.append(split_linestring_pair)
	return split_linestrings_list

def split_linestring(
		stop,
		original_linestring:shapely.LineString,
		distance_from_linestring:float=0.001
)->tuple[shapely.LineString, shapely.LineString]|None:
	
	if len(original_linestring.coords) == 2:
		return split_segment(stop, original_linestring)
	else:
		return split_curve(stop, original_linestring, distance_from_linestring)

def split_segment(
		stop,
		original_linestring:shapely.LineString,
)->tuple[shapely.LineString, shapely.LineString]:
	first_linestring = shapely.LineString([original_linestring.coords[0], stop.geometry])
	second_linestring = shapely.LineString([stop.geometry, original_linestring.coords[1]])
	return first_linestring, second_linestring

def split_curve(
		stop,
		original_linestring:shapely.LineString,
		distance_from_linestring:float=0.001
)->tuple[shapely.LineString, shapely.LineString]:
	snapped_edge_geometry = shapely.snap(original_linestring, stop.geometry, distance_from_linestring)
	i = 0
	geometry_list_length = len(snapped_edge_geometry.coords)
	stop_coord = (stop.geometry.x, stop.geometry.y)

	while i < geometry_list_length-1 and snapped_edge_geometry.coords[i] != stop_coord:
		i+=1
	first_linestring = shapely.LineString(snapped_edge_geometry.coords[:i+1])
	second_linestring = shapely.LineString(snapped_edge_geometry.coords[i:])
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

	intersection_gdf.drop(["street_count", "highway"], axis=1).to_file(reduced_intersections_path)

	road_gdf.drop(["bridge", "tunnel"], axis=1).to_file(reduced_roads_path)
	return None

def reduce_buildings(
		db_connector:DBConnector,
		wkb_enveloppe:bytes,
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
			WHERE ST_Contains(ST_GeomFromWKB($wkb_enveloppe), geom)
			{"AND type in['apartments', 'office']" if apartments_office_only else ""}) TO $output_file
			WITH (FORMAT gdal, DRIVER 'ESRI Shapefile', LAYER_CREATION_OPTIONS 'WRITE_BBOX=YES', SRS 'EPSG:4326');
		""",
		{
			"input_file": str(building_path),
			"output_file": str(reduced_building_path),
			"wkb_enveloppe": wkb_enveloppe
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