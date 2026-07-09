from config import *
from urllib.request import urlretrieve
import zipfile
import abc
import warnings
import re
import datetime

class Area_Mode(abc.ABC):
	def __init__(
			self, 
			place:box|str|None,
			gtfs_repertory_path:Path|None,
			shapefile_repertory_path:Path|None,
			load_repertories:bool=False,
			**import_repertories_params
		):
		super().__init__()
		self.place = place
		
		self.gtfs_repertory_path = gtfs_repertory_path
			
		self.shapefile_repertory_path = shapefile_repertory_path

		if load_repertories:
			self.import_repertories(**import_repertories_params)

		
	def import_repertories(
			self,
			**import_repertories_params
	)->None:
		if "verbose" not in import_repertories_params: verbose = False
		else: verbose = import_repertories_params["verbose"]
		

		if "gtfs_url" not in import_repertories_params: gtfs_url = None
		else: gtfs_url = import_repertories_params["gtfs_url"]

		if "gtfs_zip_path" not in import_repertories_params: gtfs_zip_path = None
		else: gtfs_zip_path = import_repertories_params["gtfs_zip_path"]

		if not self.gtfs_repertory_path is None and not self.gtfs_repertory_path.exists():
			self.import_gtfs(gtfs_url, gtfs_zip_path, verbose)
		elif verbose:
			print("GTFS repertory does already exist")


		if "shapefile_url" not in import_repertories_params: shapefile_url = None
		else: shapefile_url = import_repertories_params["shapefile_url"]
		if "shapefile_zip_path" not in import_repertories_params: shapefile_zip_path = None
		else: shapefile_zip_path = import_repertories_params["shapefile_zip_path"]
		
		if not self.shapefile_repertory_path is None and not self.shapefile_repertory_path.exists():
			self.import_shapefiles(shapefile_url, shapefile_zip_path, verbose)
		elif import_repertories_params["verbose"]:
			print("Shapefiles repertory does already exist")
		return None
	
	def get_default_shapefile_repertory_path(self, stem:str)->Path|None:
		pattern = re.compile(f"^{stem}-[0-9]+.*.shp$")
		for file in PROJECT_ROOT.iterdir():
			if pattern.match(file.name) is not None:
				self.shapefile_repertory_path = file
				return file
		return None
		
	@abc.abstractmethod
	def import_gtfs(self, gtfs_url, gtfs_zip_path, verbose)->None:
		pass

	@abc.abstractmethod
	def import_shapefiles(self, shapefile_url, shapefile_zip_path, verbose)->None:
		pass

class IDF_Area_Mode(Area_Mode):
	def __init__(
			self,
			place:box|str|None=None,
			gtfs_repertory_path:Path|None=None,
			shapefile_repertory_path:Path|None=None,
			load_repertories:bool=False,
			verbose:bool=False
		):

		place = place or DEFAULT_BOX_10_TH

		gtfs_repertory_path = gtfs_repertory_path or PROJECT_ROOT / "IDFM-gtfs"

		shapefile_repertory_path = shapefile_repertory_path or self.get_default_shapefile_repertory_path("ile-de-france")
		shapefile_repertory_path = shapefile_repertory_path or Path(f"ile-de-france-{get_current_date_string()}-free.shp")
		

		super().__init__(
			place, 
			gtfs_repertory_path, 
			shapefile_repertory_path,
			load_repertories,
			verbose=verbose
		)

	def import_gtfs(
			self,
			gtfs_url:str|None=None,
			gtfs_zip_path:Path|None=None,
			verbose:bool=False
		)->None:	

		gtfs_url = gtfs_url or "https://eu.ftp.opendatasoft.com/stif/GTFS/IDFM-gtfs.zip"
		gtfs_zip_path = gtfs_zip_path or PROJECT_ROOT /"IDFM-gtfs.zip"

		import_external_repertory(gtfs_url, gtfs_zip_path, verbose)
		return None

	def import_shapefiles(
			self,
			shapefile_url:str|None=None,
			shapefile_zip_path:Path|None=None,
			verbose:bool=False
	)->None:
		warnings.warn("Warning: be aware these files are heavy")
		current_date = get_current_date_string()
		shapefile_url = shapefile_url or "https://download.geofabrik.de/europe/france/ile-de-france-latest-free.shp.zip"
		shapefile_zip_path = shapefile_zip_path or PROJECT_ROOT /f"ile-de-france-{current_date}-free.shp.zip"

		import_external_repertory(shapefile_url, shapefile_zip_path, verbose)
		return None

class Hanoi_Area_Mode(Area_Mode):
	def __init__(
			self,
			place:box|str|None=None,
			gtfs_repertory_path:Path|None=None,
			shapefile_repertory_path:Path|None=None,
			load_repertories:bool=False,
			verbose:bool=False
		):

		place = place or DEFAULT_BOX_HANOI

		gtfs_repertory_path = gtfs_repertory_path or PROJECT_ROOT / "hanoi_gtfs_am"

		shapefile_repertory_path = shapefile_repertory_path or self.get_default_shapefile_repertory_path("hanoi")
		shapefile_repertory_path = shapefile_repertory_path or Path(f"vietnam-{get_current_date_string()}-free.shp")

		super().__init__(
			place, 
			gtfs_repertory_path, 
			shapefile_repertory_path,
			load_repertories,
			verbose=verbose
		)

	def import_gtfs(
		self,
		gtfs_url:str|None=None,
		gtfs_zip_path:Path|None=None,
		verbose:bool=False
	)->None:

		gtfs_url = gtfs_url or "https://datacatalogfiles.worldbank.org/ddh-published/0038236/1/DR0046582/hanoi_gtfs_am.zip"
		gtfs_zip_path = gtfs_zip_path or PROJECT_ROOT /"hanoi_gtfs_am.zip"

		import_external_repertory(gtfs_url, gtfs_zip_path, verbose)
		return None

	def import_shapefiles(
			self,
			shapefile_url:str|None=None,
			shapefile_zip_path:Path|None=None,
			verbose:bool=False
	)->None:
		warnings.warn("Warning: be aware these files are heavy (zip: ~700MB, unzipped: ~2GB)")
		shapefile_url = shapefile_url or "https://download.geofabrik.de/asia/vietnam-latest-free.shp.zip"
		current_date = get_current_date_string()
		shapefile_zip_path = shapefile_zip_path or PROJECT_ROOT /f"vietnam-{current_date}-free.shp.zip"

		import_external_repertory(shapefile_url, shapefile_zip_path, verbose)
		if isinstance(shapefile_zip_path, str):
			shapefile_zip_path = Path(shapefile_zip_path)

		self.reduce_shapefile()
		return None

	def reduce_shapefile(self)->None:
		import duckdb as dd
		with open("hanoi_convex_hull_wkb.bin", "rb") as f:
			wkb_enveloppe = f.read()
		dd.execute("LOAD spatial;")
		new_shapefile_repertory_path = self.shapefile_repertory_path.name.replace("vietnam", "hanoi")
		Path(new_shapefile_repertory_path).mkdir()
		for shapefile in (x for x in self.shapefile_repertory_path.iterdir() if x.suffix==".shp"):
			dd.execute("""
				COPY (SELECT * FROM ST_ReadSHP($input_file)
				WHERE ST_Contains(ST_GeomFromWKB($wkb_enveloppe), geom)) TO $output_file
				WITH (FORMAT gdal, DRIVER 'ESRI Shapefile', LAYER_CREATION_OPTIONS 'WRITE_BBOX=YES', SRS 'EPSG:4326');
				""",
				{
					"input_file": str(shapefile),
					"output_file": str(new_shapefile_repertory_path) +"/"+shapefile.name,
					"wkb_enveloppe": wkb_enveloppe
				})
		for file in self.shapefile_repertory_path.iterdir():
			file.unlink()
		self.shapefile_repertory_path.rmdir()
		self.shapefile_repertory_path = Path(new_shapefile_repertory_path)
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
	if isinstance(zip_path, str):
		zip_path = Path(zip_path)

	unzipped_path =  Path(zip_path.stem)
	with zipfile.ZipFile(zip_path, "r") as zip_ref:
		Path.mkdir(unzipped_path, mode=0o755)
		zip_ref.extractall(unzipped_path)
	
	return None

def get_current_date_string()->str:
	current_date = datetime.date.today()
	return f'{current_date.year%100}{current_date.month:02}{current_date.day:02}'

if __name__ == "__main__":
	area_mode = Hanoi_Area_Mode()
	area_mode.reduce_shapefile()