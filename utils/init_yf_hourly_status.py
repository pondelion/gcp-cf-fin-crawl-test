import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from cf_v2_src.rdb import (
    db,
    init_rdb,
    YahooHourlyUpdateStatusModel
)

init_rdb()

for i in range(24):
    db.add(
        YahooHourlyUpdateStatusModel(
            code_cut_index=i,
            last_succeeded=False
        )
    )
db.commit()
db.close()
