from dataclasses import dataclass
from pathlib import Path

# create an immutable data strucure to store boundaries
@dataclass(frozen=True)
class box:

	# Attributes Declaration
	# using Type Hints
	left: float
	right: float
	
	bottom: float
	top: float
	

# define paths
PROJECT_ROOT = Path(".")
SHAPEFILE_REPERTORY_PATH = PROJECT_ROOT / "ile-de-france-260112-free.shp"
GTFS_REPERTORY_PATH = PROJECT_ROOT / "IDFM-gtfs"

REDUCED_DATA_PATH = PROJECT_ROOT / "reduced_data"
#DEFAULT_BOX = box(2.34781, 2.37206, 48.86500, 48.88456)
DEFAULT_BOX = "10th arrondissement"
#DEFAULT_BOX = "Paris"
