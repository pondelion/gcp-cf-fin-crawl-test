import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from cf_v4_src.rdb import (
    init_rdb,
)


init_rdb()