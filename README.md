# Medical Expenditure Panel Survey (MEPS) Web Scraping Tool

This tool automates the process of searching for Summary of Benefits and Coverage (SBC) health insurance forms   implements three processes:

1. Find urls for certainty governments in the MEPS using the Google Search API.
2. Scrape those webpages to look for PDFs; download results.
3. Identify which of those PDFs are Summary of Benefits and Coverage (SBC) forms.

### How to run the pipeline

#### Setup

1. Clone this repository and create the following additional subdirectories: `inputs/`, `logs/`, and `scraped_data`.

2. Install conda environment + dependencies from `requirements.txt`. 

3. If needed, change proxy settings to allow external internet access. Do this in your `.bashrc` so that scrapy will recognize it.

4. Create a sqlite database with the schema defined in `schema_sbc_db.sql` with `cat schema_sbc_db.sql | sqlite3 sbc_db.sqlite`

5. Update the `scrape_config.py` file in the `scrape/` subdirectory so it reflects your working directories and database names.

6. Get certainty government data inputs from the Government Master Address File (GMAF) and place them in the `inputs/` subdirectory. The `load_db.py` module expects an Excel file called `Master Status Spreadsheet.xlsx` with at least the five following columns: `ID`, `PLANT`, `MNAME1` `MNAME2`, `ST`. With these fields, the module will create unique IDs and names and load the GMAF data into the database. The input file looks different from year to year, so modify the ETL process as needed.

7. Get a Google Custom Search ID and an API key for the search API [here](https://developers.google.com/custom-search/v1/introduction). You can make 100 free queries per day (each of which yield 10 links) before you need to enable billing.

8. Create a module called `keys.py` in the `scrape` subdirectory to hold the Google API information. The pipeline expects this file to contain two globals called `GOOGLE_CSE_ID` and `GOOGLE_API_KEY` for the search engine ID and the API key, respectively.

#### Get websites from Google

If we do not have a start website for a given government unit, we use the Google Custom Search API to find candidate websites. We search the name of the government entity, the state in which it is located, and the term "employee health insurance". We exclude certain domains, such as Facebook and LinkedIn, that we know we cannot scrape. 

##### To run

Run `query_google_api.py`. This module will query the database to assemble the search term for each certainty government, make the API call, and write the first 10 results back to the database. There is a global variable that tracks the number of API calls remaining for each day and stops the queries once the limit is hit. 

Once the queries are done, the script loads the first url from this step into DB table that gets queried in next step.

If it's clear from visual inspection or from results that the first url is not the correct url, use the code in the notebook `scrape/swap out start_url.ipynb` to choose an alternative url from the Google results.

To batch update any government units that were not successfully scraped with a new start url, use the method in `scrape/reload_urls.py`.

#### Scraping websites to look for SBC forms 

We use the Scrapy framework to find and download pdfs on the websites for each certainty government. 

The spider crawls these websites and links found on these websites two levels deep. It will not crawl outside of the base domains of the initial URLs; this is set using the `allow_domains` parameter in the `LinkExtractor`. 

If the spider follows a link with content type pdf, it will send the link to a `FilesPipeline` object, which can download the file asynchronously. 

Files are saved in a subdiretory with hash filename; scrapy ensures files are not downloaded more than once within a run and across runs. The `FilesPipeline` returns a `files` dictionary containing basic information about the download, including the map between url and file hash.

##### To run

1. Check that the names of the metadata files (currently defined in the `pipelines` module) are as desired. 

2. Run `scrapy crawl sbc_spider` in the `scrape/sbcscrape` subdirectory.

Two notes:
 - Scrapy will sometimes end scraping early when provided a list of start URLs with more than 100 items. We used `screen` and specified batches of 50-100 to avoid this issue and to ensure the job would finish in a few hours. The batch size is set within the `sbc_spider` module itself as a global variable.
 - Scraping nearly 1,000 websites for PDFs will probably require several hundred gigabytes of storage space. We developed the `--delete` argument to `scrape/move_sbcs.py` so we could programmatically delete non-SBC PDFs as we went.

#### Identifying which PDFs are SBC forms

The Scrapy pipeline only downloads PDFs, without regard to which are SBC forms and which are not. The module `identify_sbc.py` opens each downloaded PDF and looks for the phrase "Summary of Benefits and Coverage" in the first three pages. This phrase is customizable in `scrape/scrape_config.py`. If it finds that phrase, we say that the PDF is an SBC form. 

We store the results in the database: we insert a count of pdfs and a count of SBCs found back into the `gov_info` table. The `sbc_check` table tracks gov unit IDs, filepaths, and whether each is an SBC form.  

Once we identify the SBCs, we use 'scrape/move_sbcs.py` to move them into another directory with human-readable names. Optionally, this module has a method to delete non-SBC PDFs to reduce the project storage needs.

##### To run

1. From the top level directory, run `python scrape/identify_sbc.py`. 

2. Then run `python scrape/move_sbcs.py`.

### What's in here?


```
├── load_db.py
├── logs (.gitignored)
│   └── spider_exceptions.csv
├── move_sbcs.py
├── README.md
├── scrape - contains code related to Google queries + web scraping 
│   ├── check_exceptions.py
│   ├── explore scraping leftovers.ipynb
│   ├── identify_sbc.py
│   ├── ingest_scrape_results.py
│   ├── move_sbcs.py
│   ├── keys.py (.gitignored)
│   ├── query_google_api.py
│   ├── reload_urls.py
│   ├── requirements.txt
│   ├── sbcscrape
│       ├── sbcscrape
│       │   ├── example.py
│       │   ├── items.py
│       │   ├── middlewares.py
│       │   ├── pipelines.py
│       │   ├── settings.py
│       │   └── spiders
│       │       └── sbc_spider.py
│       └── scrapy.cfg
└── scraped_data - where the scrapy output is saved
```

