"""
RELOAD GOV_INFO START URLS WITH NEXT URL CANDIDATES

This module loads remaining urls from the Google queries into the
 gov_info table so they can be queried in the scrapy pipeline. Only
 certgovs where no sites were scraped are currently affected.
"""

import pandas as pd
import sqlite3

import scrape_config as config
DB = config.DB_PATH



def update_gov_info_with_new_url(dbconn, url_index):
    '''
    Update DB gov_info table to insert a new url into the start_url
     field
    
    Takes:
    - active database connection
    - index of next url to take (1-10)
    Returns:
    - None
    '''

    cur = dbconn.cursor()

    # run join to load new url
    update = '''
            WITH govs_not_scraped AS (
                SELECT id_idcd_plant, MNAME, is_queried_search, date_queried_search
                FROM gov_info
                WHERE is_scraped=0
            ),
            new_google_result AS (
                SELECT * 
                FROM latest_queries
                WHERE url_index=?
            )
            INSERT INTO gov_info (
                    id_idcd_plant,
                    MNAME,
                    start_url
                )
                SELECT a.id_idcd_plant, a.MNAME, b.start_url FROM govs_not_scraped AS a
                INNER JOIN 
                new_google_result AS b
                ON a.id_idcd_plant=b.id_idcd_plant
            ON CONFLICT(id_idcd_plant) DO UPDATE SET start_url = excluded.start_url;
            '''
    cur.execute(update, (url_index, ))

    return


if __name__ == '__main__':

    dbconn = sqlite3.connect(DB)

    update_gov_info_with_new_url(dbconn, 2)