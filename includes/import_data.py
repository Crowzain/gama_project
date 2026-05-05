from config import *
from urllib.request import urlretrieve
import zipfile

def import_repertories(
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

	import_external_repertory(gtfs_url, gtfs_zip_path, verbose)
	return None
	
def import_shapefiles(
		shapefile_url:str|None=None,
		shapefile_zip_path:str|Path|None=None,
		verbose:bool=False
		)->None:
	
	if shapefile_url is None: shapefile_url = "https://download.geofabrik.de/europe/france/ile-de-france-latest-free.shp.zip"
	if shapefile_zip_path is None: shapefile_zip_path = PROJECT_ROOT /"ile-de-france-latest-free.shp.zip"

	import_external_repertory(shapefile_url, shapefile_zip_path, verbose)
	return None
	
def import_external_repertory(
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