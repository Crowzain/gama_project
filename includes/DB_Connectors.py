from config import *
import duckdb as dd
import os
import abc
import re
from dotenv import load_dotenv
from typing import Iterable

class DBConnector(abc.ABC):
	def __init__(self):
		super().__init__()
		self.con = dd.connect()
		self.name = None
		self.path = None
		self.prefix = None
	
	@abc.abstractmethod
	def connect(self):
		pass

	def show(self) -> None:
		self.con.sql("SHOW ALL TABLES;").show()
		return None

class DuckDB_Connector(DBConnector):
	def __init__(
			self, 
			path:Path|None=None,
			):
		
		super().__init__()
		self.path = path or PROJECT_ROOT / "gama_project.db"
		self.connect()
	
	def connect(self)->None:
		self.con = dd.connect(self.path)
		return None

class SQLite_Connector(DBConnector):
	def __init__(
			self,
			name:str|None=None,
			path:Path|None=None
			) -> None:
		super().__init__()
		self.name = name or "gama_project_sqlite"
		self.path = path or PROJECT_ROOT / "gama_project_sqlite.db"
		self.prefix = "main"
		
	def connect(self)->None:
		self.con = dd.connect()
		self.con.execute("INSTALL sqlite;")
		self.con.execute("LOAD sqlite;")
		
		self.con.execute(f"""
				ATTACH '{self.name}' AS {self.name} (TYPE sqlite);
				""")
		
		self.con.execute("""USE $1;""", [self.name])
		return None
	

class MySQL_Connector(DBConnector):
	def __init__(
			self, 
			name:str|None=None,
			host:str|None=None,
			user:str|None=None
			) -> None:
		
		super().__init__()
		load_dotenv()
		self.name = name or "mysql_db"
		self.prefix = f"{self.name}.{self.name}"

		self.host = host or "127.0.0.1"
		self.user = user or "root"
		self.port = int(os.environ["PORT1"])
		self.database = os.environ["DATABASE"]

		self.connect()
	
	def connect(self):
		# environment file to handle MySQL database
		self.con = dd.connect()
		self.con.execute("INSTALL mysql;")
		self.con.execute("LOAD mysql;")
		self.con.execute(
			"""
				CREATE SECRET (
					TYPE mysql,
					HOST $host,
					PORT $port,
					DATABASE $database,
					USER $user,
					PASSWORD $passwd
				);
			""",
			{
				"host":self.host,
				"port":self.port,
				"database":self.database,
				"user":self.user,
				"passwd":os.environ["PASSWD"],
			}
		)
		# because no prepared available in this case, we check string validity with a RegExp
		if not re.match(r'^[a-zA-Z0-9_]+$', self.database): raise ValueError("Invalid database name")
		self.con.execute(f"""
				ATTACH 'host=localhost user=root port={self.port} database={self.database}' AS {self.name} (TYPE mysql);
				""")
		self.con.execute(f"""USE {self.name};""")
		return None

def create_tables(
		db_connector:DBConnector,
		input_path:str|Path|None=None,
		tables:Iterable[str]|None=None,
		verbose:bool=False,
		box:box|None=None
	)->None:
	if input_path is None:
		input_path = GTFS_REPERTORY_PATH
	else:
		if isinstance(input_path, str): 
			input_path = Path(input_path) 
		if not input_path.exists():
			raise BaseException(f"input path {input_path} does not exist.")
		
	if tables is None:
		tables = map((lambda x: x.stem), input_path.rglob('*.txt'))

	for table in tables:
		if verbose:
			print(table)
		if "stops" == table and box is not None:
			create_table_query = f"""
				CREATE OR REPLACE TABLE {f"{db_connector.prefix}.{table}" if db_connector.prefix is not None else table} AS 
					SELECT * FROM read_csv($input_file)
					WHERE lon>=$left AND lon<=$right AND stop_lat>=$bottom AND stop_lat<=$top;
			"""
			db_connector.con.execute(create_table_query, parameters={
				"table": f"{db_connector.prefix}.{table}" if db_connector.prefix is not None else table,
				"input_file": f"{input_path/table}.txt",
				"left": box.left, 
				"right": box.right, 
				"bottom": box.bottom, 
				"top": box.top})
		else:
			create_table_query = f"""
				CREATE OR REPLACE TABLE {f"{db_connector.prefix}.{table}" if db_connector.prefix is not None else table} AS 
					SELECT * FROM read_csv($input_file)
			"""
			db_connector.con.execute(create_table_query, parameters={"input_file": f"{input_path/table}.txt",})
		
	if verbose:
		db_connector.show()
	return None