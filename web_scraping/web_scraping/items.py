# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class WebScrapingItem(scrapy.Item):
    month = scrapy.Field()
    company_name = scrapy.Field()
    tyre_type = scrapy.Field()
    category = scrapy.Field()
    upper_tag = scrapy.Field()
    lower_tag = scrapy.Field()
    cn_product_name = scrapy.Field()
    en_product_name = scrapy.Field()
    items = scrapy.Field()
    size = scrapy.Field()
    model = scrapy.Field()
