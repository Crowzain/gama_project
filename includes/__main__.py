from config import *
import getopt, sys
from import_data import import_repertories
from reduce_shapefile import create_reduced_data_repertory, reduce_shapefiles
from write_csv import write_bus_stop_csv
from DB_Connectors import create_tables
from DB_Connectors import DBConnector, DuckDB_Connector, SQLite_Connector, MySQL_Connector

def read_cli_option()->tuple[bool, DBConnector, int, int, bool]:

	verbose = False
	db = DuckDB_Connector()
	stops_threshold_line = 5
	nb_lines_max = 100
	create_db_flag = False

	args = sys.argv[1:]
	options = "v"
	long_options = [
		"verbose", "duckdb", "mysql", "sqlite",
		"create-db", "stops-threshold-line=", "nb-lines-max="
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
	return verbose, db, stops_threshold_line, nb_lines_max, create_db_flag

if __name__=="__main__":
	create_reduced_data_repertory()
	verbose, db, stops_threshold_line, nb_lines_max, create_db_flag = read_cli_option()
	import_repertories()
	if create_db_flag:
		create_tables(db, verbose=verbose)
	
	reduce_shapefiles(db, DEFAULT_BOX)
	write_bus_stop_csv(DEFAULT_BOX, db, stops_threshold_line=stops_threshold_line, nb_lines_max=nb_lines_max)
