"""
LOAD CERTAINTY GOVERNMENT DATA INTO DB FOR 2022
Author: Cecile Murray
08/16/2022
"""

import numpy as np
import pandas as pd
import sqlite3

# local config
import scrape.scrape_config as config

# GLOBALS
DATA_PATH = config.DATA_PATH
DB = config.DB_PATH


if __name__ == "__main__":

    dbconn = sqlite3.connect(DB)

    data = pd.read_excel(DATA_PATH + 'Master Status Spreadsheet 2022.xlsx', nrows=908, dtype=str)

    # create ID
    data['id_idcd_plant'] = data.ID + data.PLANT

    # clean whitespace out of names and concatenate into one name
    data = data.replace(np.nan, '', regex=True)
    data['MNAME1'] = data.MNAME1.str.rstrip()
    data['MNAME2'] = data.MNAME2.str.rstrip()
    data['MNAME'] = data['MNAME1'].astype(str) + ' ' + data['MNAME2'].astype(str)

    # add some additional fields we will want
    data['is_queried_search'] = False
    data['is_scraped'] = False
    data['start_url'] = None

    final_fields = [
        "ID",
        "PLANT",
        "MNAME1",
        "MNAME2",
        "ST",
        "id_idcd_plant",
        "MNAME",
        "start_url",
        "is_queried_search",
        "is_scraped",
    ]
    data = data[final_fields]

    # now load into database
    data.to_sql('gov_info', dbconn, if_exists='append', index=False)
