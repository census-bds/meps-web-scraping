"""
CHECK WEBSITES THAT RAISED EXCEPTIONS IN SCRAPY
Try to get PDFs from them
"""


# reads the exception files and processes the links 
# there to see if there are any pdfs. The pdfs are appended to plan_forms_2021.sqlite3
from pdfminer.pdfinterp import PDFResourceManager
from io import StringIO
from pdfminer.pdfpage import PDFPage
from pdfminer.pdfinterp import PDFPageInterpreter
from pdfminer.converter import TextConverter
import pandas as pd
import numpy as np
from pdfminer.layout import LAParams
from os import listdir
from os.path import isfile, join
import re
import os
from pathlib import Path

import urllib
from io import BytesIO

import urllib.request
import requests

import traceback

import joblib

import sqlite3
from ssl import SSLError

import certifi

import datetime

# from checksbc_functions import plan_url

import tldextract

from pdfminer.high_level import extract_text

start_time=datetime.datetime.now()
print('Program Start time:', start_time)

datapath='/data/data/user/gweng001/'
scrapeddata='/data/data/user/gweng001/scrapeddata_2021/'

#==============================================================================#

STANDARD_SBC_TITLE_TEXT = 'Summary of Benefits and Coverage (SBC)'

def load_bytes_for_url_exception( url_IN ):

    # return reference
    bytes_OUT = None


    # declare variables
    me = "load_bytes_for_url"
    status_message = None
    response = None
    success_flag = None
    my_error = None
    trust_store_path = None

    # init - try a few things, fall back to `verify = False`:
    success_flag = False

    # try simple call
    try:
        response = requests.get( url_IN )
        success_flag = True
    except SSLError as my_error:
        print( "simple call failed ( {error} ).".format( error = my_error ) )
    #-- END try simple call --#

    # success?
    if ( success_flag == False ):

        # OS trust store
        trust_store_path = "/etc/ssl/certs"
        try:
            response = requests.get( url_IN, verify = trust_store_path )
            success_flag = True
        except SSLError as my_error:
            print( "OS trust store /etc/ssl/certs failed ( {error} ).".format( error = my_error ) )
        #-- END try OS trust store call --#
    #-- END check if success --#

    # success?
    if ( success_flag == False ):

        # certifi trust store
        trust_store_path = certifi.where()
        try:
            response = requests.get( url_IN, verify = trust_store_path )
            success_flag = True
        except SSLError as my_error:
            print( "certifi trust store {trust_store_path} failed ( {error} ).".format( trust_store_path = trust_store_path, error = my_error ) )
        #-- END try OS trust store call --#

    #-- END check if success --#

    # here, could try a certificate store with just certificates we've collected
    #     to deal with outliers...

    # success?
    if ( success_flag == False ):

        # I give up. no verify
        response = requests.get( url_IN, verify = False )
        success_flag = True

        # TODO - OR, could write the URL to database here, for followup.

    #-- END check if success --#

    # got a response?
    if ( ( success_flag == True ) and ( response is not None ) ):

        # we have a response.  Get bytes.
        bytes_OUT = response.content
        response_OUT=response

    else:

        # not success, or no response, either way, return None.
        bytes_OUT = None
        status_message = "====> In {function_name}(): no reponse, nothing to process ( success_flag: {success_flag}; response: {my_response} )".format(
            function_name = me,
            success_flag = success_flag,
            my_response = response
        )
        print( status_message )

    #-- END check to see if response --#

    return bytes_OUT, response_OUT

#-- END function load_bytes_for_url() --#

def get_pdf_pages( pdf_bytes_IN,
                   max_pages_IN = 3,
                   do_caching_IN = True,
                   page_numbers_IN = set() ):

    # return reference
    pdf_pages_OUT = None

    # declare variables
    pdf_byte_stream = None
    max_pages = None
    do_caching = None
    page_numbers = None

    # make stream of PDF bytes
    pdf_byte_stream = BytesIO( pdf_bytes_IN )

    # call get_pages()
    max_pages = max_pages_IN
    do_caching = do_caching_IN
    page_numbers = page_numbers_IN
    pdf_pages_OUT = PDFPage.get_pages(
        pdf_byte_stream,
        page_numbers,
        maxpages = max_pages,
        caching = do_caching,
        check_extractable = False
    )

    # cleanup - close byte stream
    # pdf_byte_stream.close()

    return pdf_pages_OUT

#-- END function get_pdf_pages() --#

def find_term_in_list_or_string( unknown_object, search_term, case_sensitive = True):

    # return reference
    is_found_OUT = False

    # declare variables
    string_to_search = None
    find_string = None
    list_to_search = None
    list_item = None

    # check type of object
    if isinstance( unknown_object, str ):

        # ignore case?
        if ( case_sensitive == True ):

            # case sensitive
            string_to_search = unknown_object
            string_to_find = search_term

        else:

            # not case sensitive
            string_to_search = unknown_object.lower()
            string_to_find = search_term.lower()

        #-- END prepare string values for search. --#

        # search
        if string_to_find in string_to_search:

            is_found_OUT = True

        #-- END search --#

    elif isinstance(unknown_object, list):

        # search all items in list
        list_to_search = unknown_object
        for list_item in list_to_search:

            # search in item - if found in any, return True.
            is_found_OUT = find_term_in_list_or_string( list_item, search_term, case_sensitive )

            # fall out of loop on first true?
            if ( is_found_OUT == True ):

                break

            #-- END check if found --#

        #-- END loop over list. --#

    #-- END check type of object passed in --#

    return is_found_OUT

#-- END function find_term_in_list_or_string() --#


def pdf_to_txt( page_list_IN, max_pages_IN = 3 ): #https://stackoverflow.com/questions/22800100/parsing-a-pdf-via-url-with-python-using-pdfminer

    # return reference
    pdf_text_OUT = None

    # declare variables
    pdf_resource_manager = None  # rsrcmgr
    output_stream = None  # retstr
    encoding = None  # codec
    laparams = None
    text_converter = None  # device
    pdf_byte_stream = None  # fp
    page_interpreter = None
    page_list = None

    # set up TextConverter
    pdf_resource_manager = PDFResourceManager()
    output_stream = StringIO()
    encoding = 'utf-8'
    laparams = LAParams()

    #new pdfminer doesn't have codec
    text_converter = TextConverter(
        pdf_resource_manager,
        output_stream,
        laparams = laparams
    )

    # create page interpreter.
    page_interpreter = PDFPageInterpreter( pdf_resource_manager, text_converter )

    # get pages - only first 3.
    #page_list = get_pdf_pages(
    #    pdf_bytes_IN,
    #    max_pages_IN = max_pages_IN,
    #    do_caching_IN = True,
    #    page_numbers_IN = set()
    #)
    page_list = page_list_IN

    # if ( ( max_pages_IN is not None ) and ( max_pages_IN > 0 ) ):

    # page_list = page_list[ : max_pages_IN ]

    #-- END check if we limit number of pages --#

    # loop over pages
    for page in page_list:

        # convert page to text.
        page_interpreter.process_page( page )

    #-- END loop over pages --#

    # retrieve text from output stream.
    pdf_text_OUT = output_stream.getvalue()

    # cleanup - close text_converter.
    text_converter.close()
    output_stream.close()

    return pdf_text_OUT

#-- END function pdf_to_text() --#

def plan_url_exception(file_url_IN,scrapedinfo_IN,domain_IN):
    
    # return reference
    result_list_OUT = None

    # declare variables
    my_pid=None
    file_bytes = None
    pdf_pages = None
    pdf_text = None
    page_count = None
    sbc_title_text = None
    is_sbc_title_text_found = None


    # init
    sbc_title_text = STANDARD_SBC_TITLE_TEXT

    my_pid=os.getpid()
    print("=======At top of plan_url(): URL={file_url_IN}, PID = {my_pid}, starting at {current_time}".format( my_pid = my_pid,file_url_IN=file_url_IN, current_time = datetime.datetime.now() ) )
    # for fileurl in urls:
    
    try:

        # retrieve bytes for URL
        # print("============Loading bytes: starting at {current_time}".format(current_time = datetime.datetime.now() ) )
        file_bytes = load_bytes_for_url_exception(file_url_IN )[0]
        response=load_bytes_for_url_exception(file_url_IN )[1]
        content_type =response.headers.get('content-type').lower()
        # print("============Loading bytes: ending at {current_time}".format(current_time = datetime.datetime.now() ) )

        # get pdf pages

        # pdf_pages = get_pdf_pages(
        #     file_bytes,
        #     max_pages_IN = 3,
        #     do_caching_IN = True,
        #     page_numbers_IN = set()
        # )

        # get text of first three pages
        pdf_to_txt_start=datetime.datetime.now()
        # print("====================== PDF to text: starting at {current_time}".format(current_time = datetime.datetime.now() ) )
        # pdf_text = pdf_to_txt( pdf_pages, max_pages_IN = 3 )
        pdf_byte_stream = BytesIO(file_bytes)
        pdf_text=extract_text(pdf_byte_stream, maxpages=3)
        pdf_byte_stream.close()

        pdf_to_txt_duration=datetime.datetime.now()-pdf_to_txt_start
        print("====================== PDF to text Duration:  {duration}".format(duration = datetime.datetime.now() - pdf_to_txt_start) )
        
        #limit processing to pdfs with less than 20 pages
        # Q - if only getting 1st 3 pages in pdf_from_url_to_text(), do you care
        #     what the size of the PDF is? You are only ever looking at first 3,
        #     but to get count, you are parsing out all pages, so over 20. And,
        #     you already are reading entire byte array into memory.g
        #page_count = len( list( pdf_pages ) )
        #if page_count <= 20:

        # check if SBC header text is in text.
        is_sbc_title_text_found = find_term_in_list_or_string(
            pdf_text,
            sbc_title_text, # 'Summary of Benefits and Coverage (SBC)'
            case_sensitive = False
        )

        # was SBC title text found?
        if ( is_sbc_title_text_found == True ) and ('application/pdf' in content_type):

            #find census_id associated with the file (id_idcd_plant is the census_id)
            # does this have to be one line?
            
            folder_id=list(scrapedinfo_IN['census_id'].loc[scrapedinfo_IN.fullpath==file_url_IN])
            folder_id1=[]

            

            try:
                d=response.headers['content-disposition']
                filename=re.findall("filename=(.+)",d)[0].strip('"')
            except KeyError: #if url already has a pdf extension then
                filename=file_url_IN

            for folder_save in folder_id:

                # check again if SBC title text is found...?
                #if ( is_sbc_title_text_found == True ):

                print(folder_save)

                # does this have to be one line?
                filepath= Path(scrapeddata+folder_save+'/'+filename.split('/')[-1])
                filepath.write_bytes(file_bytes)

                # does this have to be one line?
                tst=[folder_save,'no page',file_url_IN,filename.split('/')[-1],domain_IN]

                folder_id1.append( tst )

                #-- END check if SBC title text is found --#

            #-- END loop over folders --#

            result_list_OUT = [folder_id1]

            # no longer... for now.
            #-- END check if SBC title text is found --#

        #-- END page count check. --#

    except Exception as e:

        # traceback.print_exc()
        print( e, file_url_IN )

    #-- END try...except --#
    print("=======At end of plan_url():PID = {my_pid}, completing at {current_time}".format( my_pid = my_pid,file_url_IN=file_url_IN, current_time = datetime.datetime.now() ) )
    
    return result_list_OUT



#skip bad lines
exceptions1=pd.read_csv(datapath+'spider_exceptions_1.csv',dtype=str,error_bad_lines=False,header=None)
exceptions2=pd.read_csv(datapath+'spider_exceptions_2.csv',dtype=str,error_bad_lines=False,header=None)
exceptions3=pd.read_csv(datapath+'spider_exceptions_3.csv',dtype=str,error_bad_lines=False,header=None)
exceptions4=pd.read_csv(datapath+'spider_exceptions_4.csv',dtype=str,error_bad_lines=False,header=None)
exceptions5=pd.read_csv(datapath+'spider_exceptions_5.csv',dtype=str,error_bad_lines=False,header=None)
exceptions6=pd.read_csv(datapath+'spider_exceptions_6.csv',dtype=str,error_bad_lines=False,header=None)
exceptions7=pd.read_csv(datapath+'spider_exceptions_7.csv',dtype=str,error_bad_lines=False,header=None)

df=pd.concat([exceptions1,exceptions2,exceptions3,exceptions4,exceptions5,exceptions6,exceptions7])

df.columns=['fullpath']
df_out=df.drop_duplicates()

domain_ex_exception=[tldextract.extract(domain) for domain in df_out['fullpath'].to_list()]
clean_domain_exception=["{}.{}".format(domain_ex1.domain,domain_ex1.suffix) for domain_ex1 in domain_ex_exception]
clean_domain_exception=[dom.lower() for dom in clean_domain_exception] #164
df_out['clean_domain']=clean_domain_exception

clean_subdomain_exception=["{}.{}.{}".format(domain_ex1.subdomain,domain_ex1.domain,domain_ex1.suffix) for domain_ex1 in domain_ex_exception]



##
conn_gov = sqlite3.connect('/data/data/user/gweng001/2021_gov_api_domain.sqlite3')
c_gov = conn_gov.cursor()
gov_api_domain = pd.read_sql("SELECT * FROM gov_api_domain", conn_gov)

domains1=gov_api_domain['domain']
#remove 'www' we want domains to be stripped of 'www'
domain_ex=[tldextract.extract(domain) for domain in gov_api_domain['domain'].to_list()]
clean_domain=["{}.{}".format(domain_ex1.domain,domain_ex1.suffix) for domain_ex1 in domain_ex]
clean_domain=[dom.lower() for dom in clean_domain]
gov_api_domain['clean_domain']=clean_domain

len(list(set(clean_domain)&(set(clean_domain_exception)))) 

len(list(set(clean_domain)-(set(clean_domain_exception))))

len(list(set(clean_domain_exception)-(set(clean_domain))))

exception_dat=df_out.merge(gov_api_domain[['census_id','clean_domain']], on='clean_domain',how='inner')

my_csv_tst=gov_api_domain
my_csv_tst['id_idcd_plant']=my_csv_tst['census_id']
my_csv_tst['clean_domain']=clean_domain

domain_torun=list(set(exception_dat['clean_domain'].to_list()))

notscraped=[]
plan_form1=[]
for domain in domain_torun:   
    print(domain)

#######THIS PART IS TEMPORARY#######
    if domain=='sandiegocounty.gov' or domain=='vbgov.com' or domain=='usd259.org' or domain=='monroecounty.gov' or domain=='houstonisd.org' or  domain=='michigan.gov' or domain=='lausd.net' or domain=='fcps.edu' or domain=='henry.k12.ga.us' or domain=='unm.edu' :#unm and michigan.gov hangs while sandiego and fcps.edu and vbgov.com and henry.k12.ga.us and monroecounty.gov and lausd.net and houstonisd.org, memory isue
        continue   
#########################################
    scrapedinfo=exception_dat.loc[exception_dat['clean_domain']==domain]
    # scrapedinfo.columns=('url','name','fullpath','filename','domain')
    
    # scrapedinfo_ex=[tldextract.extract(domain) for domain in scrapedinfo['domain'].to_list()]
    # scrapedinfo_clean_domain=["{}.{}".format(domain_ex1.domain,domain_ex1.suffix) for domain_ex1 in scrapedinfo_ex]
    # scrapedinfo['clean_domain']=[dom.lower() for dom in scrapedinfo_clean_domain]

    urls=scrapedinfo['fullpath'].to_list()

    print('URLs to process: '+str(len(urls)))
    # if len(urls)>500:#temporary remove to run smaller files
    #     continue

    p = joblib.Parallel(n_jobs=8, verbose=1, backend='loky')
    print('p DONE')
    jobs = [joblib.delayed(plan_url_exception)(fileurl,scrapedinfo,domain) for fileurl in urls]
    print('jobs DONE')
    try:
        page_filename1=p(jobs)
    except Exception:
        continue
    print('p(jobs) DONE')
    page_filename=list(filter(None,page_filename1))
    print('page_filename DONE')
    
    print('Parallel time DONE:', datetime.datetime.now().strftime("%H:%M:%S"))
    url_lst1=[]
    for page in range(0,len(page_filename)):
            tst1=[sum(page_filename[page],[])]
            tst2=sum(tst1,[])
            df=pd.DataFrame(tst2,columns=['census_id','page','file_url','filename','domain'])
            url_lst1.append(df)
    try:
        if len(url_lst1)==0:
            url_lst=pd.DataFrame(columns=['census_id','page','file_url','filename','domain'])
            url_lst['census_id']=list(set(scrapedinfo.census_id))
            #a domain maps to multiple units and doesn't have any SBC scraped
            url_lst['page']=["no page"] * len(url_lst['census_id'])
            url_lst['file_url']=["SBC not found via crawling"] * len(url_lst['census_id'])
            url_lst['filename']=["SBC not found via crawling"] * len(url_lst['census_id'])
            url_lst['domain']=[domain] * len(url_lst['census_id'])
        else:
            url_lst=pd.concat(url_lst1,ignore_index=True)
    except Exception as e:
        print(e)

    try:
        plan_form1.append(url_lst)
    except Exception as e:
        print(e)

try:       
    flatten=pd.concat(plan_form1,ignore_index=True)
except ValueError:
    flatten=[]
plan_form_out=pd.DataFrame(flatten,columns=['census_id','page','file_url','filename','domain'])
plan_form_out = plan_form_out.applymap(str)
plan_form_out=plan_form_out.drop_duplicates()
plan_form_out.to_csv(datapath+"Exceptions_pdfs.csv",index=False,header=True)


conn = sqlite3.connect('/data/data/user/gweng001/plan_forms_2021.sqlite3')
c = conn.cursor()
plan_forms_2021 = pd.read_sql("SELECT * FROM plan_forms_2021", conn)

exce=pd.read_csv(datapath+'Exceptions_pdfs.csv',dtype=str)
exce1=exce[exce['file_url']!='SBC not found via crawling']
exce2=exce[exce['file_url']=='SBC not found via crawling']

list(set(not_scraped.census_id)&(set(exce2.census_id)))

ID_notin_plan_forms_butin_exce=sorted(list(set(exce1.census_id) - set(plan_forms_2021.census_id)))


print('PROGRAM COMPLETE')

# tst=plan_forms_2021[[ 'census_id', 'page', 'filename']].merge(gov_api_domain[['census_id','domain','clean_domain']], on='census_id',how='left')

# tst.to_csv(datapath+"plan_forms_2021.csv",index=False,header=True)

