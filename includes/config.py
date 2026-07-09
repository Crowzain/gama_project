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
SHAPEFILE_REPERTORY_PATH_HANOI = PROJECT_ROOT / "hanoi-260623-free.shp"
GTFS_REPERTORY_PATH_HANOI = PROJECT_ROOT / "hanoi_gtfs"

REDUCED_DATA_PATH = PROJECT_ROOT / "reduced_data"
DEFAULT_BOX_10_BB = box(2.34781, 2.37206, 48.86500, 48.88456)
DEFAULT_BOX_10_TH = "10th arrondissement"
DEFAULT_BOX_HANOI = box(105.80503, 105.85104, 21.03756, 21.08242)
