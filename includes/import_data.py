from config import *
from urllib.request import urlretrieve
import zipfile
import abc
import warnings
import re

class Area_Mode(abc.ABC):
	def __init__(
			self, 
			place:box|str|None,
			gtfs_repertory_path:Path|str|None,
			shapefile_repertory_path:Path|str|None,
			load_repertories:bool=False,
			**import_repertories_params
		):
		super().__init__()
		
		self.place = place
		
		self.gtfs_repertory_path = gtfs_repertory_path
		if isinstance(gtfs_repertory_path, str):
			self.gtfs_repertory_path = Path(gtfs_repertory_path)
			
		
		self.shapefile_repertory_path = shapefile_repertory_path
		if isinstance(shapefile_repertory_path, str):
			self.shapefile_repertory_path = Path(shapefile_repertory_path)
		
		if load_repertories:
			self.import_repertories(**import_repertories_params)
		

	def import_repertories(
			self,
			**import_repertories_params
	)->None:
		
		
		import_repertories_params["gtfs_url"] = import_repertories_params["gtfs_url"] or None
		import_repertories_params["shapefile_url"] = import_repertories_params["shapefile_url"] or None
		import_repertories_params["verbose"] = import_repertories_params["verbose"] or False
		import_repertories_params["gtfs_zip_path"] = import_repertories_params["gtfs_zip_path"] or None
		import_repertories_params["shapefile_zip_path"] = import_repertories_params["shapefile_zip_path"] or None
		
		if not self.gtfs_repertory_path is None and not self.gtfs_repertory_path.exists():
			self.import_gtfs(
				import_repertories_params["gtfs_url"], 
				import_repertories_params["gtfs_zip_path"], 
				import_repertories_params["verbose"]
				)
		elif import_repertories_params["verbose"]:
			print("GTFS repertory does already exist")
			
		if not self.shapefile_repertory_path is None and not self.shapefile_repertory_path.exists():
			self.import_shapefiles(
				import_repertories_params["shapefile_url"], 
				import_repertories_params["shapefile_zip_path"], 
				import_repertories_params["verbose"]
				)
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
			gtfs_repertory_path:Path|str|None=None,
			shapefile_repertory_path:Path|str|None=None,
		):
		super().__init__(place, gtfs_repertory_path, shapefile_repertory_path)

		if place is None: place = DEFAULT_BOX_10_TH

		if gtfs_repertory_path is None: gtfs_repertory_path = PROJECT_ROOT / "IDFM-gtfs"

		if shapefile_repertory_path is None:
			shapefile_repertory_path = self.get_default_shapefile_repertory_path("ile-de-france")
		
		super().__init__(
			place, 
			gtfs_repertory_path, 
			shapefile_repertory_path
		)

	def import_gtfs(
			self,
			gtfs_url:str|None=None,
			gtfs_zip_path:str|Path|None=None,
			verbose:bool=False
		)->None:	

		if gtfs_url is None: gtfs_url = "https://eu.ftp.opendatasoft.com/stif/GTFS/IDFM-gtfs.zip"
		if gtfs_zip_path is None: gtfs_zip_path = PROJECT_ROOT /"IDFM-gtfs.zip"

		import_external_repertory(gtfs_url, gtfs_zip_path, verbose)
		return None

	def import_shapefiles(
			self,
			shapefile_url:str|None=None,
			shapefile_zip_path:str|Path|None=None,
			verbose:bool=False
	)->None:
		warnings.warn("Warning: be aware these files are heavy")
		if shapefile_url is None: shapefile_url = "https://download.geofabrik.de/europe/france/ile-de-france-latest-free.shp.zip"
		if shapefile_zip_path is None: shapefile_zip_path = PROJECT_ROOT /"ile-de-france-latest-free.shp.zip"

		import_external_repertory(shapefile_url, shapefile_zip_path, verbose)
		return None

class Hanoi_Area_Mode(Area_Mode):
	def __init__(
			self,
			place:box|str|None=None,
			gtfs_repertory_path:Path|str|None=None,
			shapefile_repertory_path:Path|str|None=None,
		):

		if place is None: place = DEFAULT_BOX_HANOI

		if gtfs_repertory_path is None: gtfs_repertory_path = PROJECT_ROOT / "hanoi_gtfs_am"

		if shapefile_repertory_path is None:
			shapefile_repertory_path = self.get_default_shapefile_repertory_path("hanoi")
		
		super().__init__(
			place, 
			gtfs_repertory_path, 
			shapefile_repertory_path
		)


	def import_gtfs(
		self,
		gtfs_url:str|None=None,
		gtfs_zip_path:str|Path|None=None,
		verbose:bool=False
	)->None:

		if gtfs_url is None: gtfs_url = "https://datacatalogfiles.worldbank.org/ddh-published/0038236/1/DR0046582/hanoi_gtfs_am.zip"
		if gtfs_zip_path is None: gtfs_zip_path = PROJECT_ROOT /"hanoi_gtfs_am"

		import_external_repertory(gtfs_url, gtfs_zip_path, verbose)
		return None

	def import_shapefiles(
			self,
			shapefile_url:str|None=None,
			shapefile_zip_path:str|Path|None=None,
			verbose:bool=False
	)->None:

		if not LOCKED:
			warnings.warn("Warning: be aware these files are heavy (zip: ~700MB, unzipped: ~2GB)")
			if shapefile_url is None: shapefile_url = "https://download.geofabrik.de/asia/vietnam-latest-free.shp.zip"
			if shapefile_zip_path is None: shapefile_zip_path = PROJECT_ROOT /"vietnam-260623-free.shp.zip"

			import_external_repertory(shapefile_url, shapefile_zip_path, verbose)
			if isinstance(shapefile_zip_path, str):
				shapefile_zip_path = Path(shapefile_zip_path)

			self.reduce_shapefile()
			return None
		else: 
			raise PermissionError("LOCKED: downloading failed to prevent loading heavy files, you can turn it off in the config.py file")

	def reduce_shapefile(
			self
		)->None:
		import duckdb as dd
		with open("hanoi_convex_hull_wkb.bin", "rb") as f:
			wkb_enveloppe = f.read()
		dd.execute("LOAD spatial;")
		new_shapefile_repertory_path = self.shapefile_repertory_path.name.replace("vietnam", "hanoi")
		for shapefile in (x for x in self.shapefile_repertory_path.iterdir() if x.suffix==".shp"):
			dd.execute("""
				COPY (SELECT * FROM ST_ReadSHP($input_file)
				WHERE ST_Contains(ST_GeomFromWKB($wkb_enveloppe), geom)) TO $output_file
				WITH (FORMAT gdal, DRIVER 'ESRI Shapefile', LAYER_CREATION_OPTIONS 'WRITE_BBOX=YES', SRS 'EPSG:4326');
				""",
				{
					"input_file": str(shapefile),
					"output_file": str(new_shapefile_repertory_path) +"/hanoi_"+shapefile.name,
					"wkb_enveloppe": wkb_enveloppe
				})
		self.shapefile_repertory_path.rmdir()
		self.shapefile_repertory_path = new_shapefile_repertory_path
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

if __name__ == "__main__":
	area_mode = Hanoi_Area_Mode()
	area_mode.reduce_shapefile()