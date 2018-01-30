# -*- coding: utf-8 -*-

# Scrapy settings for residentscrape project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     http://doc.scrapy.org/en/latest/topics/settings.html
#     http://scrapy.readthedocs.org/en/latest/topics/downloader-middleware.html
#     http://scrapy.readthedocs.org/en/latest/topics/spider-middleware.html

BOT_NAME = 'residentscrape'

SPIDER_MODULES = ['residentscrape.spiders']
NEWSPIDER_MODULE = 'residentscrape.spiders'


# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = 'residentscrape (+http://www.yourdomain.com)'

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

# Configure maximum concurrent requests performed by Scrapy (default: 16)
#CONCURRENT_REQUESTS = 32

# Configure a delay for requests for the same website (default: 0)
# See http://scrapy.readthedocs.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
#DOWNLOAD_DELAY = 3
# The download delay setting will honor only one of:
#CONCURRENT_REQUESTS_PER_DOMAIN = 16
#CONCURRENT_REQUESTS_PER_IP = 16

# Disable cookies (enabled by default)
COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
#TELNETCONSOLE_ENABLED = False

# Override the default request headers:
DEFAULT_REQUEST_HEADERS = {
    # 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'user-agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36',
    # 'referer':'https://www.residentadvisor.net',
}

# Enable or disable spider middlewares
# See http://scrapy.readthedocs.org/en/latest/topics/spider-middleware.html
#SPIDER_MIDDLEWARES = {
#    'residentscrape.middlewares.ResidentscrapeSpiderMiddleware': 543,
#}

# Enable or disable downloader middlewares
# See http://scrapy.readthedocs.org/en/latest/topics/downloader-middleware.html
# DOWNLOADER_MIDDLEWARES = {
#    'residentscrape.middlewares.ProxyMiddleware': 543,
# }

# Enable or disable extensions
# See http://scrapy.readthedocs.org/en/latest/topics/extensions.html
#EXTENSIONS = {
#    'scrapy.extensions.telnet.TelnetConsole': None,
#}

# Configure item pipelines
# See http://scrapy.readthedocs.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
   # 'residentscrape.pipelines.ResidentscrapePipeline': 300,
   #  'residentscrape.pipelines.ArtistscrapePipeline': 300,
    'residentscrape.pipelines.ArtistSQLPipeLine': 300,
    'residentscrape.pipelines.EventSQLPipeLine': 300,
    'residentscrape.pipelines.GoogleSQLPipeLine': 300,
    'residentscrape.pipelines.InstagramPipeLine': 300,
}

# Enable and configure the AutoThrottle extension (disabled by default)
# See http://doc.scrapy.org/en/latest/topics/autothrottle.html
#AUTOTHROTTLE_ENABLED = True
# The initial download delay
#AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
#AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
#AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
#AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See http://scrapy.readthedocs.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
#HTTPCACHE_ENABLED = True
#HTTPCACHE_EXPIRATION_SECS = 0
#HTTPCACHE_DIR = 'httpcache'
#HTTPCACHE_IGNORE_HTTP_CODES = []
#HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'

## LocalMYSQL credentials
# SQLUSERNAME = 'root'
# DATABASE = 'WDJPNew'
# HOST = 'localhost'

## AWS RD credentials
SQLUSERNAME = 'wdjpuser'
DATABASE = 'wdjpdbcarling'
HOST = 'wdjpdbcarling.czroneq4sqg9.us-west-2.rds.amazonaws.com'



RA_MODE = 'upcoming'                                 # upcoming or history
BIT_MODE = 'upcoming'                                # upcoming or history
RA_YEAR = '2010'                                    # Years past this won't be considered for scrape. Works only with history mode


# LOG_STDOUT = True
# LOG_FILE = '/home/sachin/Sachin/upwork/residentscrape/log/scrape_run_RA_history.log'
FACEBOOK_APP_ID='127062923380'
FACEBOOK_APP_SECRET='0a2b51bc116940af57c65edc58a4c047'
FACEBOOK_API_USE_MODE=1                              # mode 1 for single request and mode 2 for two requests
GOOGLE_API_KEY='AIzaSyCFAeAkwm5EsjXgDXJAv8FGTc2sDs4S4mU'


## source IDs
SONGKICK_SOURCE_ID=6
INSTAGRAM_SOURCE_ID=9
FACEBOOK_SOURCE_ID=4
GIGATOOL_SOURCE_ID=3
BIT_SOURCE_ID=1
RESIDENTADVISOR_SOURCE_ID=2
GOOGLEMAP_SOURCE_ID=20

##
SAVE_SOURCE=0                                        # 0 for No, 1 for Yes