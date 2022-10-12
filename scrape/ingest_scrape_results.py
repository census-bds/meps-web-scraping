"""
INGEST RESULTS FROM SBC SCRAPING
This module ingests the two csv files that come out of the Scrapy
pipeline into the database. 

Cecile Murray
8/8/2022
"""

import pandas as pd
import sqlite3

from urllib.parse import urlparse

# import globals from local config
import scrape_config as config
DB = config.DB_PATH
PDF_METADATA = config.PDF_METADATA
SCRAPE_METADATA = config.SCRAPE_METADATA
SCRAPY_PDF_PATH = config.SCRAPY_PDF_PATH


def ingest_pdf_list(dbconn, csv_path):
    '''
    Takes the csv containing list of scraped pdfs from Scrapy and ingest it
    into a DB table to track the base domain, pdf name, and file location

    Takes:
    - active DB connection
    - string filepath to csv with PDFs 
    Returns:
    - merged df of scraped pdf files with gov ID attached
    '''

    cols = [
        'url',
        'relative_filepath',
        'file_hash',
        'download_or_uptodate',
        'base_domain',
    ]
    pdf_df = pd.read_csv(csv_path, names=cols)


    pdf_df['path_to_pdf'] = SCRAPY_PDF_PATH + pdf_df['relative_filepath']

    # now join to find which entities we found pdfs for
    govs = pd.read_sql('SELECT id_idcd_plant, start_url FROM gov_info', dbconn)
    govs['base_domain'] = govs['start_url'].apply(lambda x: urlparse(x).netloc if x else '')

    # join on gov info. watch out for dupes here!
    pdf_df = pdf_df.merge(govs, how='inner', on='base_domain')

    # save to temp table    
    pdf_df.to_sql('scraped_pdfs', dbconn, index=False, if_exists='append')

    # add pdf count to gov_info table here
    cur = dbconn.cursor()

    update = '''
            WITH pdf_count AS (
                SELECT id_idcd_plant, COUNT(DISTINCT file_hash) AS pdf_count
                FROM scraped_pdfs
                GROUP BY id_idcd_plant
            )
            INSERT INTO gov_info (
                    id_idcd_plant,
                    MNAME,
                    pdf_count
            )
                SELECT a.id_idcd_plant, a.MNAME, b.pdf_count FROM gov_info AS a
                INNER JOIN 
                pdf_count AS b
                ON a.id_idcd_plant=b.id_idcd_plant
            ON CONFLICT(id_idcd_plant) DO UPDATE SET pdf_count = excluded.pdf_count;
            '''
    cur.execute(update)

    cur.close()

    return pdf_df


def ingest_scraped_metadata(dbconn, csv_path):
    '''
    Takes the metadata csv generated from Scrapy, which lists all websites
    scraped, and ingest it into the DB so we can track which sites have
    been scraped already.

    Takes:
    - active DB connection
    - string filepath to metadata csv
    Returns:
    - merged df of scraped listing with gov ID attached
    '''

    cols = [
        'referring_url', 
        'url',
        'base_domain',
        'file_type',
    ]
    df = pd.read_csv(csv_path, names=cols)

    # join on base domain to identify which units we scraped
    govs = pd.read_sql('SELECT id_idcd_plant, start_url FROM gov_info', dbconn)
    govs['base_domain'] = govs['start_url'].apply(lambda x: urlparse(x).netloc if x else '')

    # join on gov info. note there could be multiple rows per govt id now
    merged_df = df.merge(govs, how='inner', on='base_domain').drop(columns=['start_url', ])

    # do a check: are there cases where we can't merge on base domain?
    # try:
    #     assert merged_df.shape[0] >= df.shape[0]
    # except AssertionError:
    #     print("AssertionError")
    #     return merged_df


    # add this to a temp table
    merged_df.to_sql('latest_scrape', dbconn, index=False, if_exists='append')


    # now add to gov_info table
    update = '''
            WITH scrape_summary AS (
                SELECT id_idcd_plant, 
                    COUNT(url) as num_scraped,
                    1 AS is_scraped 
                FROM latest_scrape
                GROUP BY id_idcd_plant
            )
            INSERT INTO gov_info (
                id_idcd_plant,
                MNAME,
                is_scraped,
                num_scraped
            )
            SELECT a.id_idcd_plant, a.MNAME, b.is_scraped, b.num_scraped FROM gov_info AS a
            INNER JOIN 
            scrape_summary AS b
            ON a.id_idcd_plant=b.id_idcd_plant
            ON CONFLICT(id_idcd_plant) DO UPDATE SET is_scraped = excluded.is_scraped, num_scraped = excluded.num_scraped;
            '''

    cur = dbconn.cursor()
    cur.execute(update)
    cur.close()

    return merged_df

if __name__ == "__main__":

    dbconn = sqlite3.connect(DB)

    print("ingesting scrape metadata from {metadata}".format(metadata=SCRAPE_METADATA))

    # # load metadata into DB
    scraped_df = ingest_scraped_metadata(dbconn, SCRAPE_METADATA)

    print("ingesting PDF metadata from {metadata}".format(metadata=PDF_METADATA))

    # # load the results into the db
    pdf_df = ingest_pdf_list(dbconn, PDF_METADATA)

    dbconn.close()