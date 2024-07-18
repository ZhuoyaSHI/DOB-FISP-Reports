# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class DobnycItem(scrapy.Item):
   BIN = scrapy.Field()
   cycle = scrapy.Field()
   fileurl = scrapy.Field()
   FISP = scrapy.Field()
   FISP_json = scrapy.Field()
   photo_count = scrapy.Field()
