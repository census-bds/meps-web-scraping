"""
Define pipelines for how scraped items are processed
"""

import csv
import datetime
import logging
import pandas as pd

# instantiate logger
logger = logging.getLogger(__name__)

# globals.. config somehow?
# currently modifying these below to add a timestamp
SCRAPED_DATA_PATH = "/data/data/webscraping/scraped_data/"
PDF_METADATA_NAME = "_pdfs_from_sbc_spider.csv"
SCRAPE_METADATA_NAME = "_scrape_run_metadata.csv"

# globals for dev
# PDF_METADATA = "/data/data/webscraping/scraped_data/dev_pdfs.csv"
# SCRAPE_METADATA = "/data/data/webscraping/scraped_data/dev_scrape.csv"


class SbcscrapePipeline(object):
    '''
    Pipeline class for metadata (urls, domains, filetypes) from sbc_spider
    '''

    run_timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H%M")

    def process_item(self, item, spider):
        '''
        Method that processes/saves items coming in from spider
        '''

        # convert data from spider into dataframe
        item_df = pd.DataFrame.from_dict({
            'referring_url': [item['referring_url'],],
            'url': [item['url'],],
            'base_domain': [item['base_domain'],],
            'file_type': [item['file_type'],],
        })

        # if pdf(s) are found, this item component is a list of dicts
        if item['files']:
            files_dict_list = item['files']

            for files_dict in files_dict_list:
                files_dict['base_domain'] = item['base_domain']

            file_metadata = pd.DataFrame.from_dict(files_dict_list)
            pdf_csv_name = SCRAPED_DATA_PATH + self.run_timestamp + PDF_METADATA_NAME
            self.save_scrape_metadata(file_metadata, pdf_csv_name)


        # use category as filename?
        csv_name = SCRAPED_DATA_PATH + self.run_timestamp + SCRAPE_METADATA_NAME
        
        # write to CSV
        self.save_scrape_metadata(item_df, csv_name)

        return item


    def save_scrape_metadata(self, item_df, csv_name):
        '''
        Wrapper for scraped urls + metadata to csv. Note that this
        method appends to a file if the file exists.

        Takes:
        - dataframe chunk to write
        - string name of csv file
        Returns:
        - None
        '''

        with open(csv_name, 'a', newline='') as f:

            writer = csv.writer(f)
            for _, row in item_df.iterrows():
                writer.writerow(row)

        return