# -*- coding: utf-8 -*-
"""
QUERY GOOGLE SEARCH API FOR GOVERNMENT NAMES
Created on Tue Feb 23 10:03:31 2021

@author: gweng001

Get governments without known URLs from GMAF; search for health insurance pages
using the Google Custom Search API; store results in DB
"""

#pip install google-api-python-client

import datetime
import json
import pandas as pd
import numpy as np
import sqlite3
import time
import tldextract

from googleapiclient.discovery import build

# import globals from local config
import scrape_config as config
DB = config.DB_PATH
API_CALLS_LEFT = config.API_CALLS_LEFT
EXCLUDE_DOMAINS = config.EXCLUDE_DOMAINS
SEARCH_TERM = config.SEARCH_TERM


# import local module with API keys
import keys
API_KEY = keys.GOOGLE_API_KEY
CSE_ID = keys.GOOGLE_CSE_ID



def google_search(search_term, api_key=API_KEY, cse_id=CSE_ID, **kwargs):
    '''
    Method to query google API: each time this is called = 1 API call
    
    Takes:
    - search term string
    - api key
    - custom search engine ID
    Returns:
    - dict of results
    '''

    service = build("customsearch", "v1", developerKey=api_key)
    res = service.cse().list(q=search_term, cx=cse_id, **kwargs).execute() 
        
    return res


def extract_search_urls(result):
    '''
    Extract all urls from Google custom search API result
    
    Takes:
    - result object from search API
    Returns:
    - dataframe with url order and url
    '''
    
    links = []
    result_index = 1 

    if 'items' not in result.keys():
        print('No results in google search.')
        return pd.DataFrame()
    
    for r in result['items']:
        links.append((result_index, r['link']))
        result_index += 1
    
    df = pd.DataFrame(links, columns=['url_index', 'start_url'])
    
    return df


def update_gov_info_with_results(dbconn, links_df):
    '''
    Update DB gov_info table to record which units we queried and insert
    the first url into the start_url field
    
    Takes:
    - active database connection
    - concatenated dataframe of query results
    Returns:
    - None
    '''

    cur = dbconn.cursor()

    # insert into temp table 
    links_df.to_sql('latest_queries', con=dbconn, index=False, if_exists='replace')

    # now run join thing
    update = '''
            WITH govs_missing_urls AS (
                SELECT id_idcd_plant, MNAME, is_queried_search, date_queried_search
                FROM gov_info
                WHERE start_url IS NUll
            ),
            first_google_result AS (
                SELECT * 
                FROM latest_queries
                WHERE url_index=1
            )
            INSERT INTO gov_info (
                    id_idcd_plant,
                    MNAME,
                    is_queried_search,
                    date_queried_search,
                    start_url
                )
                SELECT a.id_idcd_plant, a.MNAME, b.is_queried_search, b.date_queried_search, b.start_url FROM govs_missing_urls AS a
                INNER JOIN 
                first_google_result AS b
                ON a.id_idcd_plant=b.id_idcd_plant
            ON CONFLICT(id_idcd_plant) DO UPDATE SET date_queried_search = excluded.date_queried_search, is_queried_search = excluded.is_queried_search, start_url = excluded.start_url;
            '''
    cur.execute(update)

    return


if __name__ == '__main__':

    # get govts to query out of the database
    conn = sqlite3.connect(DB)

    govs_to_run = pd.read_sql(
        '''
        SELECT 
            id_idcd_plant, 
            MNAME,
            ST
        FROM gov_info 
        WHERE start_url IS NULL AND external_domain IS NULL;
        ''',
        conn)


    results_df_list = []
    for index, row in govs_to_run.iterrows():


        # assemble components of search string
        unit_name = row['MNAME']
        unit_st = row['ST']
        search_term = SEARCH_TERM
        exclude_domains = EXCLUDE_DOMAINS

        # concatenate to create search string
        search_string = ' '.join([unit_name, unit_st, search_term, exclude_domains])

        print('searching for ', search_string, '...')

        try:

            # query the API
            result = google_search(search_string)

            API_CALLS_LEFT += -1

            # get the results out as a DF, add other metadata
            result_urls_df = extract_search_urls(result)
            result_urls_df['id_idcd_plant'] = row['id_idcd_plant']
            result_urls_df['MNAME'] = unit_name
            result_urls_df['is_queried_search'] = True
            result_urls_df['date_queried_search'] = datetime.datetime.now()
            results_df_list.append(result_urls_df)

        except Exception as e:
            print("Exception extracting urls from result:", e)

        if API_CALLS_LEFT == 0:
            break

        time.sleep(1)

    # make all results into a single dataframe
    results_df = pd.concat(results_df_list)

    try:

        # write to DB
        results_df.to_sql('google_query_results', con=conn, index=False, if_exists='append')

        # mark queried links as complete
        update_gov_info_with_results(conn, results_df)

    # if something about DB update fails, then save results to csv
    except Exception as e:

        now = datetime.datetime.now().strftime("%Y-%m-%d_%H%M")
        results_df.to_csv("google_query_results_{now}.csv".format(now=now), index=False)


    conn.commit()
    conn.close()