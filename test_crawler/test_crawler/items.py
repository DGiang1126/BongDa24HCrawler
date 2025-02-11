# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class TestCrawlerItem(scrapy.Item):
    # define the fields for your item here like:
    # league = scrapy.Field()
    days = scrapy.Field()
    name_match = scrapy.Field()
    result = scrapy.Field()
    match_rounds = scrapy.Field()


    
