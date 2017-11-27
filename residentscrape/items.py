# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class ResidentscrapeItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    eventname = scrapy.Field()
    eventdate = scrapy.Field()
    eventtime = scrapy.Field()
    venuename = scrapy.Field()
    venueadd = scrapy.Field()
    cost = scrapy.Field()
    promoter = scrapy.Field()
    age = scrapy.Field()
    eventurl = scrapy.Field()
    members = scrapy.Field()
    lineup = scrapy.Field()
    eventdetail = scrapy.Field()
    pass
