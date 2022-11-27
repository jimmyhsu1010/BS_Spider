import pandas as pd
import scrapy
from web_scraping.items import WebScrapingItem


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
                     "yearMonth": "11110", "kindTypes": "J401", "markNo": "", "markYear": "", "markPer": "",
                     "markStartNo": "", "markEndNo": "", "applRegno": "", "currentPageSize": "10", "currentPage": "1"}

    detail_query = {"id":"","state":"queryAll","queryAllFlag":"false","orderByColumn":"","isAscending":"true","progID":"UQI5101F","progName":"檢驗標識-標準檢驗局印製","downloadFlag":"","permissionField":"","q_markYearPer":"IC2","q_markNo":"8220190","currentPage":"1"}

    def parse(self, response):
        mon_list = response.xpath("//select[@id='yearMonth']/option/text()").extract()
        url = "https://civil.bsmi.gov.tw/bsmi_pqn/pqn/uqi1105f.do"
        print(mon_list)
        for month in mon_list:
            self.company_query["yearMonth"] = month
            # print(self.company_query["yearMonth"])
            # print(self.company_query)
            yield scrapy.FormRequest(url, method="POST", formdata=self.company_query, callback=self.parse_company, dont_filter=True, meta={"page_info": self.company_query["currentPage"], "url": url, "query": self.company_query, "month": month})

    def parse_company(self, response):
        cur_page = response.meta.get("page_info")
        month = response.meta.get("month")
        url = response.meta.get("url")
        query = response.meta.get("query")
        company_infos = response.xpath("//body[1]/div[2]/form[1]/div[4]/div[2]/div[@class='row']")
        print("年月是{},現在是第{}頁".format(month, cur_page))
        next_url = "https://civil.bsmi.gov.tw/bsmi_pqn/pqn/uqi5101f.do"
        for company in company_infos:
            com_name = company.xpath("./div[1]/text()").extract_first().strip()
            tire_type = company.xpath("./div[2]/text()").extract_first().strip()
            tag_info = company.xpath("./div[3]/text()").extract_first().strip()
            category = tag_info.split("\u3000")[0].replace(" ", "")
            upper_tag = tag_info.split("\u3000")[-1].split("~")[0]
            lower_tag = tag_info.split("\u3000")[-1].split("~")[-1]
            self.detail_query["q_markYearPer"] = category
            self.detail_query["q_markNo"] = lower_tag
            yield scrapy.FormRequest(next_url, method="POST", formdata=self.detail_query, meta={"next_url": next_url, "c_name": com_name, "t_type": tire_type, "category": category, "upper_tag": upper_tag, "lower_tag": lower_tag, "month": month}, dont_filter=True, callback=self.company_detail)

        if len(company_infos) == 10:
            query["currentPage"] = str(int(cur_page) + 1)
            yield scrapy.FormRequest(url, method="POST", formdata=query, dont_filter=True, callback=self.parse_company, meta={"url": url, "page_info": query["currentPage"], "query": query, "month": month})
        else:
            pass

        # print(company_infos)

    def company_detail(self, response):
        item = WebScrapingItem()
        print("現在是公司資料解析")
        mon = response.meta.get("month")
        c_name = response.meta.get("c_name")
        t_type = response.meta.get("t_type")
        ctgy = response.meta.get("category")
        u_tag = response.meta.get("upper_tag")
        l_tag = response.meta.get("lower_tag")
        cn_name = []
        en_name = []
        no = []
        size = []
        model = []
        for detail in response.xpath("//body[1]/div[2]/form[1]/div[3]/div[2]/div[9]/div[1]/div[@class='form-group row']"):
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


        # s = pd.Series([mon, c_name, t_type, ctgy, u_tag, l_tag, cn_name, en_name, item, size, model], index=["年月", "公司名稱", "輪胎類型", "類型", "上排標籤", "下排標籤", "品名中文", "品名英文", "項次", "規格", "型號"])
        # self.df = self.df.append(s, ignore_index=True)
        # self.df = self.df.explode(["品名中文", "品名英文", "項次", "規格", "型號"], ignore_index=True)
        # yield self.df.to_excel(r"C:\Users\kc.hsu\Desktop\20221125商檢局數據.xlsx", index=False)
