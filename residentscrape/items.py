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

class ArtistscrapeItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    eventName = scrapy.Field()
    eventStartDate = scrapy.Field()
    eventEndDate = scrapy.Field()
    eventTime = scrapy.Field()
    venueName = scrapy.Field()
    venueAdd = scrapy.Field()
    cost = scrapy.Field()
    promoter = scrapy.Field()
    age = scrapy.Field()
    eventURL = scrapy.Field()
    members = scrapy.Field()
    lineup = scrapy.Field()
    eventDetail = scrapy.Field()
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
        return repr({"Event Name": self['eventName']})

class ArtistItem(scrapy.Item):

    name = scrapy.Field()
    realName = scrapy.Field()
    sourceRef = scrapy.Field()
    aliases = scrapy.Field()
    country = scrapy.Field()
    followers = scrapy.Field()
    biography = scrapy.Field()
    website = scrapy.Field()
    additional = scrapy.Field()

    def __repr__(self):
        """only print out attr1 after exiting the Pipeline"""
        return repr({"Artist Name": self['name']})
