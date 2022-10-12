# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

class SbcscrapeItem(scrapy.Item):
    url = scrapy.Field()
    # name = scrapy.Field()
    fullpath=scrapy.Field()
    # filename = scrapy.Field()
    # extra field used later as filename 
    Category = scrapy.Field()
    filetype = scrapy.Field()