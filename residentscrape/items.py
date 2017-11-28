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
    promoterName = scrapy.Field()
    promoterAdd = scrapy.Field()
    promoterPhone = scrapy.Field()
    promoterEmail = scrapy.Field()
    promoterWebsite = scrapy.Field()
    promoterFacebook = scrapy.Field()
    promoterTwitter = scrapy.Field()
    promoterInstagram = scrapy.Field()
    promoterURL = scrapy.Field()
    promoterRegion = scrapy.Field()




    def __repr__(self):
        """only print out attr1 after exiting the Pipeline"""
        return repr({"Event Name": self['eventname']})

