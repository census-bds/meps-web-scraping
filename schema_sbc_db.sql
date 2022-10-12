-- SCHEMA FOR SBC DATABASE

--  input table for the Google query process
CREATE TABLE IF NOT EXISTS "gov_info" (
  "ID" TEXT,
  "ID_CD" TEXT,
  "PLANT" TEXT,
  "MNAME1" TEXT,
  "MNAME2" TEXT,
  "ST" TEXT,
  "id_idcd_plant" TEXT NOT NULL,
  "MNAME" TEXT NOT NULL,
  "start_url" TEXT,
  "external_domain" TEXT,
  "is_queried_search" INTEGER,
  "date_queried_search" TEXT,
  "is_scraped" INTEGER,
  "num_scraped" INTEGER,
  "pdf_count" INTEGER,
  "sbc_count" INTEGER,
  UNIQUE(id_idcd_plant)
);


-- results from google search: multiple results per ID here
CREATE TABLE IF NOT EXISTS "google_query_results" (
"url_index" INTEGER,
  "start_url" TEXT,
  "id_idcd_plant" TEXT NOT NULL,
  "MNAME" TEXT,
  "is_queried_search" INTEGER,
  "date_queried_search" TIMESTAMP
);


-- temp table to hold scraped units from latest run
CREATE TABLE IF NOT EXISTS "latest_scrape" (
  "id_idcd_plant" TEXT NOT NULL,
  "is_scraped" INTEGER,
  "num_scraped" INTEGER
);


-- temp table to hold listing of scraped pdfs from latest run
CREATE TABLE IF NOT EXISTS "scraped_pdfs" (
  'url' TEXT,
  'relative_filepath' TEXT,
  'file_hash' TEXT,
  'download_or_uptodate' TEXT,
  'base_domain' TEXT,
  'path_to_pdf' TEXT,
  'id_idcd_plant' TEXT NOT NULL,
  'start_url' TEXT
);

-- listing of SBCs found in SBC check
CREATE TABLE IF NOT EXISTS "sbc_check" (
  'id_idcd_plant' TEXT NOT NULL,
  'path_to_pdf' TEXT,
  'is_pdf_sbc' INTEGER,
  UNIQUE(path_to_pdf)
);