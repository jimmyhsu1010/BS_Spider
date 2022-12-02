import pandas as pd
import scrapy
from web_scraping.items import WebScrapingItem
from selectolax.parser import HTMLParser


class BsmiSpider(scrapy.Spider):
    df = pd.DataFrame(columns=["年月", "公司名稱", "輪胎類型", "類型", "上排標籤", "下排標籤", "品名中文", "品名英文", "項次", "規格", "型號"])
    name = 'bsmi'
    allowed_domains = ['civil.bsmi.gov.tw']
    custom_settings = {
        'ITEM_PIPELINES': {'web_scraping.pipelines.WebScrapingPipeline': 300}
    }
    start_urls = ['https://civil.bsmi.gov.tw/bsmi_pqn/pqn/uqi1105f.do']
    product_category = "汽車輪胎"
    company_query = {"id": "", "state": "queryAll", "queryAllFlag": "false", "orderByColumn": "", "isAscending": "true",
                     "progID": "UQI1105F", "progName": "各類貨品檢驗合格名單查詢", "downloadFlag": "", "permissionField": "",
                     "yearMonth": "", "kindTypes": "J401", "markNo": "", "markYear": "", "markPer": "",
                     "markStartNo": "", "markEndNo": "", "applRegno": "", "currentPageSize": "10",
                     "currentPage": "1"}  # 查詢每月名單

    detail_query = {"id": "", "state": "queryAll", "queryAllFlag": "false", "orderByColumn": "", "isAscending": "true",
                    "progID": "UQI5101F", "progName": "檢驗標識-標準檢驗局印製", "downloadFlag": "", "permissionField": "",
                    "q_markYearPer": "IC2", "q_markNo": "8220190", "currentPageSize": "10", "currentPage": "1"}

    def parse(self, response):
        mon_list = response.xpath("//select[@id='yearMonth']/option/text()").extract()
        url = "https://civil.bsmi.gov.tw/bsmi_pqn/pqn/uqi1105f.do"
        print(mon_list)
        answer = int(input("爬取全部月份請按‘1’\n爬取最新月份請按‘2’\n"))
        if answer == 1:
            for month in mon_list:
                self.company_query["yearMonth"] = month
                print("年月是{},現在是第{}頁".format(month, self.company_query["currentPage"]))
                yield scrapy.FormRequest(url, method="POST", formdata=self.company_query, callback=self.parse_company,
                                         dont_filter=True,
                                         meta={"page_info": self.company_query["currentPage"], "url": url,
                                               "month": self.company_query["yearMonth"],
                                               "company_query": self.company_query})
        else:
            self.company_query["yearMonth"] = mon_list[0]
            yield scrapy.FormRequest(url, method="POST", formdata=self.company_query, callback=self.parse_company,
                                     dont_filter=True, meta={"page_info": self.company_query["currentPage"], "url": url,
                                                             "month": self.company_query["yearMonth"],
                                                             "company_query": self.company_query})
            # break  # 後續更新只要最新一個月的就可以了

    def parse_company(self, response):
        company_q = {"id": "", "state": "queryAll", "queryAllFlag": "false", "orderByColumn": "",
                     "isAscending": "true",
                     "progID": "UQI1105F", "progName": "各類貨品檢驗合格名單查詢", "downloadFlag": "", "permissionField": "",
                     "yearMonth": "", "kindTypes": "J401", "markNo": "", "markYear": "", "markPer": "",
                     "markStartNo": "", "markEndNo": "", "applRegno": "", "currentPageSize": "10",
                     "currentPage": "1"}  # 查詢每月名單
        cur_page = response.meta.get("page_info")
        month = response.meta.get("month")
        url = response.meta.get("url")
        query = company_q
        # company_infos = response.xpath("//body[1]/div[2]/form[1]/div[4]/div[2]/div[@class='row']")
        parser = HTMLParser(response.text)

        next_url = "https://civil.bsmi.gov.tw/bsmi_pqn/pqn/uqi5101f.do"
        for content in parser.css("#listContainer > div.panel-body.listRows > div:nth-child(n+2)"):
            company_content = content.css("div.col-xs-12.col-sm-6.col-md-6.col-lg-4")
            # company_name = company_content[0].text().strip()  # 公司名稱
            tire_type = company_content[-1].text().strip()  # 貨物類型
            other_content = content.css("div.col-xs-12.col-sm-12.col-md-12.col-lg-4")
            id_type = other_content[0].text().strip().replace(" ", "").split("\u3000")[0]  # 類型
            serial_num = other_content[0].text().strip().replace(" ", "").split("\u3000")[-1]
            num1 = serial_num.split("~")[0]  # 上排標籤
            num2 = serial_num.split("~")[-1]  # 下排標籤
            # print([company_name, tire_type, id_type, serial_num, num1, num2])
            self.detail_query["q_markYearPer"] = id_type
            self.detail_query["q_markNo"] = num2
            # print(self.detail_query)
            yield scrapy.FormRequest(next_url, method="POST", formdata=self.detail_query,
                                     meta={"next_url": next_url, "t_type": tire_type,
                                           "category": id_type, "upper_tag": num1, "lower_tag": num2,
                                           }, dont_filter=True, callback=self.company_detail)

        try:
            parser.css_first("#form1 > div.form-group.row.dataRows > "
                             "div.col-xs-12.col-sm-6.col-md-6.col-lg-6.text-right > div > div > "
                             "button:nth-last-child(2)").attributes["title"]
        except KeyError:
            pass
        except AttributeError:
            pass
        else:
            if parser.css_first("#form1 > div.form-group.row.dataRows > "
                                "div.col-xs-12.col-sm-6.col-md-6.col-lg-6.text-right > div > div > "
                                "button:nth-last-child(2)").attributes["title"] == "下一頁":
                query["currentPage"] = str(int(cur_page) + 1)
                query["yearMonth"] = month
                print("下一頁的query是", query)
                yield scrapy.FormRequest(url, method="POST", formdata=query, dont_filter=True,
                                         callback=self.parse_company,
                                         meta={"url": url, "page_info": query["currentPage"], "month": month,
                                               "company_query": query})
            else:
                pass

    def company_detail(self, response):
        item = WebScrapingItem()
        mon = response.xpath("//body[1]/div[2]/form[1]/div[3]/div[2]/div[7]/div[1]/p/text()").extract_first()
        t_type = response.meta.get("t_type")
        ctgy = response.meta.get("category")
        u_tag = response.meta.get("upper_tag")
        l_tag = response.meta.get("lower_tag")
        c_name = response.xpath("//body[1]/div[2]/form[1]/div[4]/div[1]/div[2]/div[3]/a/text()").extract_first()
        cn_name = []
        en_name = []
        no = []
        size = []
        model = []
        for detail in response.xpath(
                "//body[1]/div[2]/form[1]/div[3]/div[2]/div[9]/div[1]/div[@class='form-group row']"):
            keys = detail.xpath("normalize-space(./label/text())").extract()
            values = detail.xpath("normalize-space(./div/text())").extract()
            for k, v in zip(keys, values):
                if "品名中文" in k:
                    cn_name.append(v)
                elif "品名英文" in k:
                    en_name.append(v)
                elif "項次" in k:
                    no.append(v)
                elif "規格" in k:
                    size.append(v)
                else:
                    model.append(v)
        for c, e, i, s, m in zip(cn_name, en_name, no, size, model):
            item["month"] = mon
            item["company_name"] = c_name
            item["tyre_type"] = t_type
            item["category"] = ctgy
            item["upper_tag"] = u_tag
            item["lower_tag"] = l_tag
            item["cn_product_name"] = c
            item["en_product_name"] = e
            item["items"] = i
            item["size"] = s
            item["model"] = m
            yield item
