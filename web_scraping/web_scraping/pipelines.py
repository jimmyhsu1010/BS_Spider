# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import sqlite3


class WebScrapingPipeline:

    def open_spider(self, spider):
        self.con = sqlite3.connect("scraping_df")
        self.cur = self.con.cursor()
        # self.cur.execute('drop table if exists bsmi')
        # self.cur.execute("create table bsmi (month TEXT, company_name TEXT, tyre_type TEXT, "
        #                  "category TEXT, upper_tag int, lower_tag int, cn_product_name TEXT, "
        #                  "en_product_name TEXT, items TEXT, size TEXT, model TEXT) "
        #                  )
        # self.con.commit()

    def process_item(self, item, spider):
        # print(spider.name, "pipelines") #測試item是否有正確傳到pipeline
        insert_sql = 'insert into bsmi(month, company_name, tyre_type, category, upper_tag, lower_tag, ' \
                     'cn_product_name, en_product_name, items, size, model) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ? ,?) '
        self.cur.execute(insert_sql, (item["month"], item["company_name"], item["tyre_type"], item["category"], item["upper_tag"], item["lower_tag"], item["cn_product_name"], item["en_product_name"], item["items"], item["size"], item["model"]))
        self.con.commit()
        return item

    def close_spider(self, spider):
        self.con.close()
