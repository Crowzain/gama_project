# Gama project - Bus Network Simulation
------

## 1. Table of content
- 1. [Table of Contents](#1-table-of-content)
- 2. [Installation](#2-installation)
- 3. [Perspectives](#3-to-do-and-perspectives)
- 4. [Authors and acknowledgment](#4-perspectives-ideas)

## 2. Installation

### Download gtfs and shapefiles
After cloning the repo, move to includes and execute:
```
$> python .
```
Options:
* `-v`, `--verbose`: verbose mode
* `--duckdb`: use a duckdb database 
* `--mysql`: use a mysql database (server connexion required, see instructions below)
* `--sqlite`: use a sqlite database

This script also create csv files related to lines, and smaller files according to a smaller working zone (10th arrondissement)
into `reduced_data` folder.

### MySQL database
After cloning the repo, to connect to MySQL database server execute:
```
$> docker compose up --build
```

To stop and remove the container:
```
$> docker compose down -v
```

## 3. To do and perspectives
- [ ] add random traffic
- [ ] add more buses on lines
- [ ] adjust bus speed (according to traffic and acceleration)
- [ ] consider personal behavior
- [ ] consider source and target not necessarily on stops

## 4. Author
This project is currently developped by Pierre B.