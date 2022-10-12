# -*- coding: utf-8 -*-
"""
SBC FORM SPIDER
Created on Mon Jan 11 16:06:38 2021

@author: gweng001

Defines spider for locating and saving SBC form candidates
"""
# to develop crawler
#https://stackoverflow.com/questions/23868784/separate-output-file-for-every-url-given-in-start-urls-list-of-spider-in-scrapy
#https://stackoverflow.com/questions/47361396/scrapy-seperate-output-file-per-starurl

#to add handle exceptions
# https://www.tutorialspoint.com/scrapy/scrapy_requests_and_responses.htm

import sqlite3
from tkinter.messagebox import IGNORE
import pandas as pd

from urllib.parse import urlparse

from scrapy.linkextractors import LinkExtractor
from scrapy.linkextractors import IGNORED_EXTENSIONS
from scrapy.spidermiddlewares.httperror import HttpError 
from scrapy.spiders import CrawlSpider, Rule
from scrapy import signals

from twisted.internet.error import DNSLookupError 
from twisted.internet.error import TimeoutError 
 

# TODO: CONFIG THIS?
DB = "/data/data/webscraping/sbc_db_2022.sqlite"
DEV_URLS = ['https://www.rsa-al.gov/peehip/publications/']


class SbcSpider(CrawlSpider):
    name='sbc_spider'
    custom_settings = {
        'LOG_LEVEL': 'INFO',
    }
    
    batch_size = 30 # scrapy doesn't handle long start_urls lists well, so we batch
    query = '''
            SELECT id_idcd_plant,
                    MNAME,
                    start_url
            FROM gov_info
            WHERE start_url IS NOT NULL AND
            is_scraped=0 
            LIMIT ?;
            '''

    dbconn = sqlite3.connect(DB)
    cur = dbconn.cursor()
    cur.execute(query, (batch_size, ))
    cols = ['id_idcd_plant', 'MNAME', 'start_url', ]
    to_scrape = pd.DataFrame(cur.fetchall(), columns = cols)

    start_urls = to_scrape['start_url'].to_list()
    # start_urls = DEV_URLS
    
    # get unique domains of all start urls, restrict to these
    domains = set([urlparse(url).netloc for url in start_urls])


    rules = (
        Rule(
            LinkExtractor(
                unique=True,
                allow_domains = domains,
                deny_extensions=[i for i in IGNORED_EXTENSIONS if i!="pdf"],
            ),
            callback='parse_item',
            follow=True,
        ),
    )

    
    # output failed start_url in Scrapy stats https://stackoverflow.com/questions/13724730/how-to-get-the-scrapy-failure-urls 
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.failed_urls = []

        self.logger.info(f"df has dimensions {self.to_scrape.shape}")



    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(SbcSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.handle_spider_closed, signals.spider_closed)
        return spider


    def get_original_url(self, response):
        '''
        Sometimes urls redirect, but we want to keep track of original domain
        so we can track where links came from.

        This handles a case where a link from www.auburb.edu redirects to 
        sites.auburn.edu. Scrapy still thinks this is within the domain, and 
        we agree, but we need to track original domain so we can link it back.   

        Takes:
        - response object
        Returns:
        - original url
        '''

        if 'redirect_urls' in response.meta:
            return response.meta['redirect_urls'][0]
        else:
            return response.url



    def handle_spider_closed(self, reason):
        '''
        Custom handler to collect urls that fail to scrape
        '''

        self.logger.info(f"Spider closed: {reason}")
        self.crawler.stats.set_value('failed_urls', ', '.join(self.failed_urls))


    def parse_item(self, response):
        '''
        Main method to control scraping activity

        Yields urls and related info for processing by pipeline module:
        - referring url: if there was one, from where did we get to this page?
        - url: the response url of the page scraped
        - base_domain: the net location of the original url we used to get here
        - file_type: content type from the response header
        - file_urls: list containing the urls of any pdf objects
        '''

        self.logger.debug(f"allowed domains are: {self.domains}")
        
        if response.status == 404:
            self.crawler.stats.inc_value('failed_url_count')
            self.failed_urls.append(response.url)

        # get the base domain
        original_url = self.get_original_url(response)
        base_domain = urlparse(original_url).netloc   

        # split off the https:// and get content type
        content_type = response.headers.get('content-type').lower() 

        self.logger.debug(f"{base_domain}, {original_url}, {str(content_type)}")

        # if there's a pdf extension, pass to pipeline
        if "pdf" in str(content_type):
            self.logger.info(f"pdf found from base domain {base_domain}")
            file_urls = [response.url,]
        else:
            file_urls = []

    
        yield  {
            'referring_url': response.request.headers.get('Referer', None),
            'url':response.url,
            'base_domain': base_domain,
            'file_type': content_type,
            'file_urls': file_urls,
            }
        

    def parse_start_url(self, response):
        '''
        Method to process the start url. Replicates the steps in parse_item.
        '''
        
        if response.status == 404:
            self.crawler.stats.inc_value('failed_url_count')
            self.failed_urls.append(response.url)

        # get the base domain
        original_url = self.get_original_url(response)
        base_domain = urlparse(original_url).netloc   

        # split off the https:// and get content type
        content_type = response.headers.get('content-type').lower() 

        self.logger.debug(f"{base_domain}, {original_url}, {str(content_type)}")

        # if there's a pdf extension, pass to pipeline
        if "pdf" in str(content_type):
            self.logger.info(f"pdf found from base domain {base_domain}")
            file_urls = [response.url,]
        else:
            file_urls = []

    
        yield  {
            'referring_url': response.request.headers.get('Referer', None),
            'url':response.url,
            'base_domain': base_domain,
            'file_type': content_type,
            'file_urls': file_urls,
            }


    def process_exception(self, exception, spider):
        '''
        Track exceptions through the stat collector

        There is probably a better way to do this; I think we'd need to
        look at log output to get the numbers.
        '''
        ex_class = "%s.%s" % (exception.__class__.__module__, exception.__class__.__name__)
        self.crawler.stats.inc_value('downloader/exception_count', spider=spider)
        self.crawler.stats.inc_value('downloader/exception_type_count/%s' % ex_class, spider=spider)


    
