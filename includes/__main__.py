from config import *
import getopt, sys
from import_data import IDF_Area_Mode, Hanoi_Area_Mode
from reduce_shapefile import create_reduced_data_repertory, reduce_shapefiles
from write_csv import write_bus_stop_csv
from DB_Connectors import create_tables
from DB_Connectors import DuckDB_Connector, SQLite_Connector, MySQL_Connector

def read_cli_option()->dict:

	verbose = False
	db = None
	stops_threshold_line = 5
	nb_lines_max = 100
	create_db_flag = False
	place = None
	clean = False
	is_IDF_Area_Mode = False

	args = sys.argv[1:]
	options = "v"
	long_options = [
		"verbose", "duckdb", "mysql", "sqlite",
		"create-db", "stops-threshold-line=", 
		"nb-lines-max=", "place=", "clean",
		"IDF"
		]

	dict_options = {
		"--duckdb":DuckDB_Connector,
		"--mysql":MySQL_Connector,
		"--sqlite":SQLite_Connector
	}

	try:
		arguments, _ = getopt.getopt(args, options, long_options)
	except getopt.error as err:
		print(str(err))
	else:
		for currentArg, currentVal in arguments:
			if currentArg in ("-v", "--verbose"):
				verbose = True
			elif currentArg in dict_options:
				db = dict_options[currentArg]()
			elif currentArg == "--stops-threshold-line":
				stops_threshold_line = int(currentVal)
			elif currentArg == "--nb-lines-max":
				nb_lines_max = int(currentVal)
			elif currentArg == "--create-db":
				create_db_flag = True
			elif currentArg == "--place":
				place = currentVal
			elif currentArg == "--clean":
				clean = True
			elif currentArg == "--IDF":
				is_IDF_Area_Mode = True
	if place is None:
		place = DEFAULT_BOX_10_TH if is_IDF_Area_Mode else DEFAULT_BOX_HANOI
	if db is None:
		db = DuckDB_Connector()

	return {
			"verbose": verbose, 
			"db": db, 
			"stops_threshold_line": stops_threshold_line,
			"nb_lines_max": nb_lines_max,
			"create_db_flag": create_db_flag, 
			"place": place,
			"clean": clean,
			"is_IDF_Area_Mode": is_IDF_Area_Mode,
		}

def clear_files(
		repertory_path:Path,
		file_name_regexp:str
	)->None:
	for file in Path.glob(repertory_path, file_name_regexp):
		file.unlink()
	return None

if __name__=="__main__":
	create_reduced_data_repertory()
	cli_dict = read_cli_option()

	area_mode = IDF_Area_Mode(cli_dict["place"]) if cli_dict["is_IDF_Area_Mode"] else Hanoi_Area_Mode(cli_dict["place"])

	current_folder = Path()
	
	if cli_dict["clean"]:
		clear_files(REDUCED_DATA_PATH, "*")
		clear_files(current_folder, "*.db")
		sys.exit("Workspace has been successfully cleared")
	
	if cli_dict["create_db_flag"]:
		create_tables(cli_dict["db"], area_mode, verbose=cli_dict["verbose"], input_path=area_mode.gtfs_repertory_path)
	
	reduce_shapefiles(cli_dict["db"], cli_dict["place"], area_mode)
	write_bus_stop_csv(
		cli_dict["db"], 
		area_mode,
		stops_threshold_line=cli_dict["stops_threshold_line"], 
		nb_lines_max=cli_dict["nb_lines_max"]
	)
