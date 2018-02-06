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

    artistID = scrapy.Field()
    name = scrapy.Field()
    realName = scrapy.Field()
    sourceRef = scrapy.Field()
    aliases = scrapy.Field()
    country = scrapy.Field()
    followers = scrapy.Field()
    biography = scrapy.Field()
    website = scrapy.Field()
    additional = scrapy.Field()
    email = scrapy.Field()
    facebook = scrapy.Field()
    instagram = scrapy.Field()
    twitter = scrapy.Field()
    discog = scrapy.Field()
    bandcamp = scrapy.Field()
    soundcloud = scrapy.Field()


    profile_pic_url = scrapy.Field()


    sourceURL = scrapy.Field()
    sourceText = scrapy.Field()
    similarArtists = scrapy.Field()

    def __repr__(self):
        """only print out attr1 after exiting the Pipeline"""
        return repr({"Artist Name": self['name']})

class ResidentItem(scrapy.Item):

    artistSourceRef = scrapy.Field()
    scrapeArtistID = scrapy.Field()
    scrapeEventID = scrapy.Field()
    scrapePromoterID = scrapy.Field()
    scrapeVenueID = scrapy.Field()

    ## Fields from eventPage
    eventName = scrapy.Field()
    eventSourceRef = scrapy.Field()
    eventStartDate = scrapy.Field()
    eventStartTime = scrapy.Field()
    eventEndDate = scrapy.Field()
    eventEndTime = scrapy.Field()
    startDate= scrapy.Field()
    endDate = scrapy.Field()
    eventDescription = scrapy.Field()
    eventLineup = scrapy.Field()
    eventFollowers = scrapy.Field()
    eventGoing = scrapy.Field()
    eventMinAge = scrapy.Field()
    eventAdmin = scrapy.Field()
    eventTicketInfo = scrapy.Field()
    eventTicketPrice = scrapy.Field()
    eventTicketTier = scrapy.Field()
    eventTicketURL = scrapy.Field()
    eventFacebook = scrapy.Field()
    eventTwitter = scrapy.Field()
    eventPromotional = scrapy.Field()
    eventSourceURL = scrapy.Field()
    eventSourceText = scrapy.Field()
    eventVenueURL = scrapy.Field()
    eventVenueAddress = scrapy.Field()
    eventPromoters = scrapy.Field()
    eventPromotersURL = scrapy.Field()
    event_image_url = scrapy.Field()

    ## Fields from venuePage
    venueSourceRef = scrapy.Field()
    venueName = scrapy.Field()
    venueAddress = scrapy.Field()
    venueStreet = scrapy.Field()
    venueCity = scrapy.Field()
    venueRegion = scrapy.Field()
    venueState = scrapy.Field()
    venueZip = scrapy.Field()
    venueCountry = scrapy.Field()
    venueGeoLat = scrapy.Field()
    venueGeoLong = scrapy.Field()
    venueAka = scrapy.Field()
    venuePhone = scrapy.Field()
    venueDescription = scrapy.Field()
    venueCapacity = scrapy.Field()
    venueWebsite = scrapy.Field()
    venueGoogleMap = scrapy.Field()
    venueEmail = scrapy.Field()
    venueFollowers = scrapy.Field()
    venueFacebook = scrapy.Field()
    venueTwitter = scrapy.Field()
    venueInstagram = scrapy.Field()
    venueSourceURL = scrapy.Field()
    venueSourceText = scrapy.Field()
    venueLocationURL = scrapy.Field()                     # Found in bandsintown
    venueTBAInsert = scrapy.Field()


    ## Fields from Club/Promoter page
    promoterSourceRef = scrapy.Field()
    promoterName = scrapy.Field()
    promoterAddress = scrapy.Field()
    promoterRegion = scrapy.Field()
    promoterPhone = scrapy.Field()
    promoterFollowers = scrapy.Field()
    promoterWebsite = scrapy.Field()
    promoterGoogleMap = scrapy.Field()
    promoterGeoLat = scrapy.Field()
    prmoterGeoLang = scrapy.Field()
    promoterEmail = scrapy.Field()
    promoterFacebook = scrapy.Field()
    promoterTwitter = scrapy.Field()
    promoterInstagram = scrapy.Field()
    promoterDescription = scrapy.Field()
    promoterSourceURL = scrapy.Field()
    promoterSourceText = scrapy.Field()




    def __repr__(self):
        return repr({"Event Name":self['eventName'],"Venue Name":self['venueName']})

class GoogleMapItem(scrapy.Item):


    venueID = scrapy.Field()
    resultCount = scrapy.Field()
    addressID = scrapy.Field()
    query = scrapy.Field()
    queryID = scrapy.Field()
    address_types = scrapy.Field()
    sourceRef = scrapy.Field()
    formatted_address = scrapy.Field()
    street_address = scrapy.Field()
    street_number = scrapy.Field()
    route = scrapy.Field()
    intersection = scrapy.Field()
    room = scrapy.Field()
    floor = scrapy.Field()
    post_box = scrapy.Field()
    country = scrapy.Field()
    administrative_area_level_1 = scrapy.Field()
    administrative_area_level_2 = scrapy.Field()
    administrative_area_level_3 = scrapy.Field()
    administrative_area_level_4 = scrapy.Field()
    administrative_area_level_5 = scrapy.Field()
    colloquial_area = scrapy.Field()
    locality = scrapy.Field()
    ward = scrapy.Field()
    sublocality = scrapy.Field()
    neighborhood = scrapy.Field()
    premise = scrapy.Field()
    subpremise = scrapy.Field()
    postal_code = scrapy.Field()
    natural_feature = scrapy.Field()
    airport = scrapy.Field()
    park = scrapy.Field()
    point_of_interest = scrapy.Field()
    sourceURL = scrapy.Field()
    sourceText = scrapy.Field()
    longitude = scrapy.Field()
    lattitude = scrapy.Field()

    def __repr__(self):
        """only print out attr1 after exiting the Pipeline"""
        return repr({"Query ID":self['queryID'], "Google Place ID": self['sourceRef'], "VenueID":self['venueID'], "AddressID":self['addressID']})

class InstagramItem(scrapy.Item):

    instaID = scrapy.Field()
    artistID = scrapy.Field()
    name = scrapy.Field()
    follows = scrapy.Field()
    num_posts = scrapy.Field()
    external_url = scrapy.Field()
    sourceRef = scrapy.Field()

    def __repr__(self):
        """only print out attr1 after exiting the Pipeline"""
        return repr({"Insta Source": self['sourceRef']})