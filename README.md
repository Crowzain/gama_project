# Gama project - Bus Network Simulation
------

## 1. Table of content
1. [Table of Contents](#1-table-of-content)
2. [Installation](#2-installation)
3. [Fix me](#3-fix-me)
4. [To do and perspectives](#4-to-do-and-perspectives)
5. [Author](#5-author)
6. [License](#6-license)

## 2. Installation

### Download gtfs and shapefiles
After cloning the repo, move to includes and execute:
```
$> python .
```
Options:
* `-v`, `--verbose`: verbose mode
* `--duckdb`: use a duckdb database *(by default)*
* `--mysql`: use a mysql database (server connexion required, see instructions below)
* `--sqlite`: use a sqlite database

This script also creates csv files related to lines, and smaller files according to a smaller working zone (10th arrondissement)
into `reduced_data` folder.

### Connect to the MySQL database

> [!IMPORTANT]
> *Docker is required*

After cloning the repo, make sure that Docker is running and then connect to MySQL database server by executing:
```
$> docker compose up --build
```

To stop and remove the container:
```
$> docker compose down
```
> [!NOTE]
> if you do not want persistant database, you can delete it by adding `-v` option.

## 3. Fix me
No known bug to fix

## 4. To do and perspectives
* [x] add more buses on lines
* [ ] add random traffic
* [ ] adjust bus speed (according to traffic and acceleration)
* [ ] consider personal behavior
* [ ] consider source and target not necessarily on stops

## 5. Author
This project is currently developped by Pierre B.

## 6. License
This project is licensed under the **MIT License**.  
See the [LICENSE](./LICENSE) file for full details.

[![MIT License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)