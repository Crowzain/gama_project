from config import *
import getopt, sys
from import_data import import_repertories
from reduce_shapefile import create_reduced_data_repertory, reduce_shapefiles
from write_csv import write_bus_stop_csv
from DB_Connectors import create_tables
from DB_Connectors import DuckDB_Connector, SQLite_Connector, MySQL_Connector

def read_cli_option()->dict:

	verbose = False
	db = DuckDB_Connector()
	stops_threshold_line = 5
	nb_lines_max = 100
	create_db_flag = False
	#place = "10th arrondissement"
	place = DEFAULT_BOX_HANOI
	clean = False

	args = sys.argv[1:]
	options = "v"
	long_options = [
		"verbose", "duckdb", "mysql", "sqlite",
		"create-db", "stops-threshold-line=", 
		"nb-lines-max=", "place=", "clean"
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

	return {
			"verbose": verbose, 
			"db": db, 
			"stops_threshold_line": stops_threshold_line,
			"nb_lines_max": nb_lines_max,
			"create_db_flag": create_db_flag, 
			"place": place,
			"clean": clean,
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
	import_repertories()
	if cli_dict["clean"]:
		clear_files(REDUCED_DATA_PATH, "*")
		current_folder = Path()
		clear_files(current_folder, "*.db")
		sys.exit("Workspace has been successfully cleared")
	if cli_dict["create_db_flag"]:
		create_tables(cli_dict["db"], verbose=cli_dict["verbose"], input_path=(Path("hanoi_gtfs_am") if ZONE_MODE=="H" else None))
	
	reduce_shapefiles(cli_dict["db"], cli_dict["place"])
	write_bus_stop_csv(
		cli_dict["place"], 
		cli_dict["db"], 
		stops_threshold_line=cli_dict["stops_threshold_line"], 
		nb_lines_max=cli_dict["nb_lines_max"]
		)
