# -*- coding: utf-8 -*-
"""
Created on Mon Jan 11 08:58:20 2021

@author: gweng001
"""

import scrapy
import requests
import urllib.request


from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule

#class ExampleSpider(scrapy.Spider):
class ExampleSpider(CrawlSpider):
    name = 'example'
    allowed_domains = ['WWW.RSA-AL.GOV']#,'www.soa.akpolysubhealthplan.com']
    start_urls = ['HTTPS://WWW.RSA-AL.GOV/PEEHIP/PUBLICATIONS/']#, 'https://soa.akpolysubhealthplan.com/']

    rules=(Rule(LinkExtractor(unique=True,allow_domains=['WWW.RSA-AL.GOV']),callback='parse_item',follow=False),)
    
    def make_requests_from_url(self, url):
        request = Request(url, dont_filter=True)
        request.meta['start_url'] = url
        return request
    def parse_item(self,response):
        items1=response.xpath('//a[@href]/@href').getall()
        items=list(filter(lambda x:x.endswith((".pdf")),items1))
        for i in items :

                yield {
#                     'url':response.urljoin(i)}
                     'url':response.url,
                     'name': i,
                     'filename':i.split('/')[-1]}