"""
RENAME AND MOVE SBC FORMS

This module queries the database to get the filenames of the pdfs
 that were identified as SBCs, creates human-readable names for them,
 and moves them into another directory for ease of copying. If the --delete
 argument is provided, it will delete non-SBCs.
"""

import argparse
import os
import pandas as pd
import re
import shutil
import sqlite3

# import globals from local config
import scrape_config as config
DB = config.DB_PATH
ID_COL = config.ID_COLUMN
LOG_DIR = config.DATA_DIR
ORIGIN_DIRECTORY = config.SCRAPY_PDF_PATH
DESTINATION_DIRECTORY = config.SBC_PDF_PATH

 
def get_sbc_pdfs(dbconn):
    '''
    Wrapper method for SQL that queries the database for the SBC pdfs

    Takes:
    - active database connection
    Returns:
    - dataframe of SBC forms with filepath and unit info
    '''

    query = '''
            WITH sbcs AS (
                SELECT id_idcd_plant,
                path_to_pdf
                FROM sbc_check 
                WHERE is_pdf_sbc=1
            ),
            sbcs_with_url AS (
                SELECT DISTINCT url,
                    sbcs.path_to_pdf,
                    sbcs.id_idcd_plant
                FROM scraped_pdfs 
                INNER JOIN
                sbcs 
                ON scraped_pdfs.id_idcd_plant=sbcs.id_idcd_plant AND
                 scraped_pdfs.path_to_pdf=sbcs.path_to_pdf
            )
            SELECT a.id_idcd_plant, 
                MNAME,
                a.path_to_pdf,
                url
            FROM sbcs_with_url AS a
            LEFT JOIN
            gov_info AS b
            ON a.id_idcd_plant=b.id_idcd_plant;
            '''

    results = pd.read_sql(query, dbconn, coerce_float=False)
    return results


def get_pdfs_to_delete(dbconn):
    '''
    Wrapper method for a SQL query that gets the pdfs that aren't SBC forms
    and returns a dataframe so we can delete them

    Takes:
    - active database connection
    Returns:
    - data frame with file paths
    '''
    
    query = '''            
            SELECT path_to_pdf
            FROM sbc_check 
            WHERE is_pdf_sbc=0;
            '''

    results = pd.read_sql(query, dbconn)
    return results
 
    

def get_clean_name(df_row):
    '''
    Creates a good name for a pdf using the following rules:
    - if there is a url, use the last part of it, but remove web junk and
        ensure it ends in .pdf
    - otherwise, use the name of the unit + the sbc index
    Intended for use as a lambda function in df.apply()

    Takes:
    - row of a dataframe with a url field, an MNAME field, and a 
    sbc_index field
    Returns:
    - a new, clean filename as a string
    '''

    if df_row['url']:

        # get the final component of the url after the last slash
        url_components = df_row['url'].split("/")
        last_part = url_components[-1] if len(url_components) > 1 else url_components[0]

        # remove 'ashx' and 'aspx' and any url query syntax from the end of the url
        clean = re.sub('(\.ashx)?\?.+|$', '', last_part, flags=re.I)
        
        # now get extension
        clean_components = clean.split(".")
        extension = clean_components[-1].lower() if len(clean_components) > 1 else None

        # if it's pdf we're done
        if extension == "pdf":
            # print("extension is pdf and clean is", clean)
            return clean
        
        # if there was no extension, add .pdf 
        elif extension is None:
            # print("extension is none but clean is", clean)
            return clean + '.pdf' 
        
        # otherwise swap in the .pdf extension
        else:
            # print("extension is not pdf but exists and clean is", ''.join(clean_components[:-1]))
            return ''.join(clean_components[:-1]) + '.pdf'

    # if there somehow was no URL to work from, make up a name 
    else:

        new_name =  df_row['MNAME'].strip() + \
                    "_SBC_" + \
                    df_row['sbc_index'].astype(str) + \
                    ".pdf"

        return new_name


def copy_sbcs(sbc_df):
    '''
    Copies SBC forms to a new location by looping through a dataframe

    Takes:
    - df with SBC path_to_pdf and a new_path column
    Returns:
    - df with any exceptions
    '''

    exceptions = []

    # loop through the rows and copy files
    for i, row in sbc_df.iterrows():

        try:

            old_filepath = row['path_to_pdf']
            new_filepath = row['new_path']
            shutil.copyfile(old_filepath, new_filepath)

        except Exception as e:

            print("Exception copying file:", e)
            exceptions.append([i, *row.to_list()])

    colnames = ['index'] + list(sbcs_to_move.columns)
    exceptions_df = pd.DataFrame(exceptions, columns=colnames)

    return exceptions_df


def make_subdirectories_for_id(df, base_path):
    '''
    Make subdirectories for each certgov ID 

    Takes:
    - dataframe of SBC forms
    - path to base directory where files will live
    Returns:
    - None
    '''

    certgov_ids = set(df[ID_COL].to_list())

    if os.path.exists(base_path):
        for id in certgov_ids:

            new_dir = base_path + id + "/"
            
            if os.path.exists(new_dir):
                continue
            
            os.mkdir(new_dir)

    else:
        print("Base directory does not exist.")
        raise OSError


def delete_extraneous_pdfs(dbconn):
    '''
    Delete non-SBC PDFs downloaded when scraping

    Takes:
    - active database connection
    Returns:
    - None
    '''

    pdfs_to_delete = get_pdfs_to_delete(dbconn)
    
    for i, row in pdfs_to_delete.iterrows():

        try:
            os.remove(row['path_to_pdf'])   
        
        except Exception as e:
            print("Exception removing files:", e)


if __name__ == "__main__":

    # take command line arguments
    parser = argparse.ArgumentParser(description='Move SBCs to a specified location.')
    parser.add_argument('--d', dest='delete', default=None, help='flag to delete definite non-SBCs')
    args = parser.parse_args()

    # connect to DB and get the dataframe with SBC info
    dbconn = sqlite3.connect(DB)
    sbcs_to_move = get_sbc_pdfs(dbconn)

    # make subdirectories for each certgov ID
    make_subdirectories_for_id(sbcs_to_move, DESTINATION_DIRECTORY)
    
    # get and clean name of PDF from the url
    sbcs_to_move['sbc_index'] = sbcs_to_move.groupby('id_idcd_plant').cumcount()
    sbcs_to_move['clean_name'] = sbcs_to_move.apply(lambda x: get_clean_name(x), axis=1)

    # create new filenames
    sbcs_to_move['new_path'] = DESTINATION_DIRECTORY + \
                                sbcs_to_move[ID_COL] + "/" + \
                                sbcs_to_move['clean_name']
    
    # # move the files and capture exceptions
    exceptions = copy_sbcs(sbcs_to_move)

    # if exceptions.shape[0] > 0:
    #     exceptions.to_csv(LOG_DIR + "exceptions.csv", index=False, mode='a')

    # if command line arg is specified, delete non-SBCs
    if args.delete:
        delete_extraneous_pdfs(dbconn)
