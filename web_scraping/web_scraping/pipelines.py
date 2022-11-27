# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import sqlite3


class WebScrapingPipeline:

    def open_spider(self, spider):
        self.con = sqlite3.connect("scraping_df")
        self.cur = self.con.cursor()

    def process_item(self, item, spider):
        # print(spider.name, "pipelines") #測試item是否有正確傳到pipeline
        insert_sql = "insert into bsmi (month, company_name, tyre_type, category, upper_tag, lower_tag, " \
                     "cn_product_name, en_product_name, items, size, model) values('{}', '{}', '{}', '{}', '{}', " \
                     "'{}', '{}', '{}', '{}', '{}', '{}')".format(item["month"], item["company_name"], item["tyre_type"], item["category"], item["upper_tag"], item["lower_tag"], item["cn_product_name"], item["en_product_name"], item["items"], item["size"], item["model"])
        print(insert_sql)
        self.cur.execute(insert_sql)
        self.con.commit()
        return item

    def close_spider(self, spider):
        self.con.close()
