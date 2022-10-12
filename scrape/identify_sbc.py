"""
CHECK IF DOWNLOADED PDFS ARE SBC FORMS

This module takes the pdfs from the Scrapy pipeline and checks for key terms
indicating that a pdf is an SBC form. The result is written to the database.
If a pdf is an SBC form, it is copied to the SBC PDF directory for ease of
transfer to MEPS analysts.
"""

import datetime
import sqlite3
import pandas as pd

from multiprocessing import Pool
from pdfminer.high_level import extract_text
from urllib.parse import urlparse

# local config 
import scrape_config as config

# globals from the config
DB = config.DB_PATH
DATA_DIR = config.DATA_DIR
SCRAPY_PDF_PATH = config.SCRAPY_PDF_PATH
SBC_PDF_PATH = config.SBC_PDF_PATH
SBC_TITLE = config.STANDARD_SBC_TITLE_TEXT



def is_pdf_sbc_form(id_idcd_plant, pdfpath, maxpages=3):
    '''
    Check for presence of standard SBC title text

    Takes:
    - string gov unit ID
    - string filepath to PDF
    - int number of pages to check, default 3
    Returns:
    - string gov unit ID
    - string filepath to PDF
    - boolean True if text is found, otherwise false
    '''

    pdf_text = extract_text(pdfpath, maxpages=3).lower()

    if "Glossary of Health Coverage and Medical Terms" in pdf_text:
        return id_idcd_plant, pdfpath, False

    if SBC_TITLE.lower() in pdf_text:
        return id_idcd_plant, pdfpath, True
    
    else:
        return id_idcd_plant, pdfpath, False


def update_gov_info_with_sbc_check(dbconn):
    '''
    Take results of the SBC check and update gov_info table. This method
    wraps a big SQL query.

    Takes:
    - active db connection
    Returns:
    - None
    '''

    cur = dbconn.cursor()

    update = '''
            WITH sbc_check_results AS (
                SELECT id_idcd_plant, SUM(is_pdf_sbc) AS sbc_count
                FROM sbc_check
                GROUP BY id_idcd_plant
            )
            INSERT INTO gov_info (
                    id_idcd_plant,
                    MNAME,
                    sbc_count
                )
                SELECT a.id_idcd_plant, a.MNAME, b.sbc_count FROM gov_info AS a
                INNER JOIN 
                sbc_check_results AS b
                ON a.id_idcd_plant=b.id_idcd_plant
            ON CONFLICT(id_idcd_plant) DO UPDATE SET sbc_count = excluded.sbc_count;
            '''
    cur.execute(update)

    cur.close()


if __name__ == "__main__":

    dbconn = sqlite3.connect(DB)

    # get the paths to the PDFs we haven't checked yet
    query = '''
            WITH scraped_minus_exceptions AS (
                SELECT scraped_pdfs.path_to_pdf,
                    scraped_pdfs.id_idcd_plant
                FROM scraped_pdfs
                LEFT JOIN
                exceptions_sbc_check
                ON scraped_pdfs.path_to_pdf=exceptions_sbc_check.path_to_pdf
                WHERE exception IS NULL
            )
            SELECT DISTINCT scraped_minus_exceptions.id_idcd_plant,
                    scraped_minus_exceptions.path_to_pdf
            FROM scraped_minus_exceptions
            LEFT JOIN sbc_check
            ON scraped_minus_exceptions.id_idcd_plant=sbc_check.id_idcd_plant
                AND scraped_minus_exceptions.path_to_pdf=sbc_check.path_to_pdf 
            WHERE is_pdf_sbc IS NULL;
            '''

    pdf_df = pd.read_sql(query, dbconn, coerce_float=False)

    results = {
        'id_idcd_plant': [],
        'path_to_pdf': [],
        'is_pdf_sbc': [],
    }

    exceptions = {
        'path_to_pdf': [],
        'exception': [],
    }

    start_time = datetime.datetime.now()

    # run parallel processes to check all the SBC forms
    with Pool(processes=4) as pool:

        futures = []

        # iterate through each row of pdf df and set up parallel processes
        # to check if each pdf is an SBC
        for _, row in pdf_df.iterrows():

            id_idcd_plant = row['id_idcd_plant']
            path_to_pdf = row['path_to_pdf']
            futures.append(
                (path_to_pdf, \
                pool.apply_async(is_pdf_sbc_form, (id_idcd_plant, path_to_pdf, )))
            )
    
        # for each process, try to get the results and append to list for df
        for f, fut in futures:

            try:
                id_idcd_plant, path_to_pdf, is_pdf_sbc = fut.get(timeout = 20)
                results['id_idcd_plant'].append(id_idcd_plant)
                results['path_to_pdf'].append(path_to_pdf)
                results['is_pdf_sbc'].append(is_pdf_sbc)
            
            except TimeoutError:
                print('timeout checking pdf at location:', f)
            
            except Exception as e:
                print('exception in sbc check:', e)
                print('pdf is', f)

                exceptions['path_to_pdf'].append(f)
                exceptions['exception'].append(e)

    print('checking for SBCs took', (datetime.datetime.now() - start_time).total_seconds())

    # when parallel is done, concat results in a dataframe
    results_df = pd.DataFrame.from_dict(results)
    exceptions_df = pd.DataFrame.from_dict(exceptions)

    # sometimes this is null; assigning non-null default value so we can skip these
    # in later queries. then we need to convert some exception objects into text
    if exceptions_df.shape[0] > 0:
        exceptions_df['exception'].fillna('0', inplace=True)
        exceptions_df['exception'] = exceptions_df.apply(lambda x: x.exception.__repr__() if x.exception!='0' else x.exception, axis=1) 

    try:

        # write it to DB
        results_df.to_sql('sbc_check', dbconn, index=False, if_exists='append')
        exceptions_df.to_sql('exceptions_sbc_check', dbconn, index=False, if_exists='append')

        # update the main gov_info table
        update_gov_info_with_sbc_check(dbconn)

    except Exception as e:

        print("Exception inserting data to SQL")

    now =  datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    results_df.to_csv("{dir}results_df_{timestamp}.csv".format(dir=DATA_DIR, timestamp=now), index=False)
    exceptions_df.to_csv("{dir}exceptions_df_{timestamp}.csv".format(dir=DATA_DIR, timestamp=now), index=False)

    dbconn.commit()
    dbconn.close()