SVY_YR='2022'

DATA_PATH = "/data/data/webscraping/inputs/"

DB_PATH = "/data/data/webscraping/sbc_db_2022.sqlite"
DATA_DIR = "/data/data/webscraping/scraped_data/"

# for Google queries
API_CALLS_LEFT = 100
EXCLUDE_DOMAINS = "-site:wikipedia.org -site:facebook.com -site:linkedin.com -site:indeed.com -site:glassdoor.com -site:google.com"
SEARCH_TERM = "employee health insurance"


# don't work inside the spider 
SCRAPY_PDF_PATH = "/data/storage/pdfs/"
SBC_PDF_PATH = "/data/storage/pdfs/sbc/"

# probably have to change these a lot
SCRAPE_METADATA = "/data/data/webscraping/scraped_data/2022-09-23_1602_scrape_run_metadata.csv"
PDF_METADATA = "/data/data/webscraping/scraped_data/2022-09-23_1602_pdfs_from_sbc_spider.csv"

STANDARD_SBC_TITLE_TEXT = 'Summary of Benefits and Coverage'
ID_COLUMN = 'id_idcd_plant'