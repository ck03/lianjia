# -*- coding: utf-8 -*-
import scrapy
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError
import json
from copy import deepcopy
import re


class LjSpider(scrapy.Spider):
    """
    鏈家網房屋
    /ershoufang/ 二手房
    /zufang/     租房
    /xiaoqu/     小區
    /loupan/     新房
    新房會導到有xx.fang.xxx.com/loupan/這種網域裡去==>多一個fang
    """
    name = 'lj'
    allowed_domains = ['www.lianjia.com']
    start_urls = ['https://www.lianjia.com/city/']
    page = 1
    print("鏈家網爬蟲開始.....")

    def parse(self, response):
        # print(response.url)
        # json_list = response.text.encode("big5")
        # json_list = response.body_as_unicode()
        # json_list =json_list.encode("big5")
        """"""
        # 要找的三種資料 , 新房要獨立出來
        house_cate = ["ershoufang", "zufang", "xiaoqu"]
        li_list = response.xpath("//div[@class='city_list_section']/ul/li")
        for li in li_list:
            item = {}
            p_list = li.xpath(".//div[@class='city_list']/div[@class='city_province']")
            for p in p_list:
                item["province"] = p.xpath("./div/text()").extract_first()
                city_list = p.xpath(".//ul/li")
                for city in city_list:
                    item["city_name"] = city.xpath("./a/text()").extract_first()
                    item["city_url"] = city.xpath("./a/@href").extract_first()
                    # print(item["city_url"])
                    for h in house_cate:
                        item["house_cate"] = h
                        yield scrapy.Request(
                            # "https://xsbn.fang.lianjia.com/" + h + "/",
                            # "https://km.lianjia.com/" + h + "/",
                            item["city_url"] + h + "/",
                            callback=self.parse_city,
                            errback=self.errback_httpbin,
                            dont_filter=True,
                            meta={"item": deepcopy(item)}
                        )
                    # 新房要獨立出來
                    s = re.findall(r"\.fang\.", item["city_url"])
                    if len(s) == 0:
                        s_list = item["city_url"].split(".")
                        s_list.insert(1, "fang")
                        item["city_url"] = ".".join(s_list)
                        item["house_cate"] = "loupan"
                    else:
                        item["house_cate"] = "loupan"
                    yield scrapy.Request(
                        item["city_url"] + "loupan/",
                        callback=self.parse_city,
                        errback=self.errback_httpbin,
                        dont_filter=True,
                        meta={"item": deepcopy(item)}
                    )

        # 測試====
        # item = {}
        # item["house_cate"] = "xiaoqu"
        # item["city_url"] = "https://quanzhou.lianjia.com/"
        # yield scrapy.Request(
        #      "https://quanzhou.lianjia.com/xiaoqu/",
        #      callback=self.parse_city,
        #      errback=self.errback_httpbin,
        #      dont_filter=True,
        #      meta={"item": item}
        # )
        # ========

    def parse_city(self, response):
        item = deepcopy(response.meta["item"])
        # print("next_url=%s" % response.url)
        # print(item["house_cate"])
        # return
        s = response.url.split("/")
        # print(s)
        # return
        if item["house_cate"] == s[-2] or item["house_cate"] == s[-3]:
            print("yes")
            if item["house_cate"] == "ershoufang":
                li_list = response.xpath("//div[@class='leftContent']/ul[@class='sellListContent']/li[contains(@class,'clear LOGVIEWDATA LOGCLICKDATA')]")
                for li in li_list:
                    item["title"] = li.xpath(".//div[@class='title']/a/text()").extract_first()
                    item["house_detail"] = li.xpath(".//div[@class='title']/a/@href").extract_first()
                    item["casename"] = li.xpath(".//div[@class='address']/div/a/text()").extract_first() if len(li.xpath(".//div[@class='address']/div/a/text()")) > 0 else None
                    item["rooms"] = li.xpath(".//div[@class='address']//div//text()").getall()[-1] if len(li.xpath(".//div[@class='address']//div//text()")) > 0 else None
                    if len(li.xpath(".//div[@class='flood']//div//text()")) > 0:
                        flood = li.xpath(".//div[@class='flood']//div//text()").getall()[0]
                        flood = re.sub(r"\-", "", flood.strip())
                        item["flood"] = flood
                    else:
                        item["flood"] = None
                    item["case"] = li.xpath(".//div[@class='flood']//div/a/text()").extract_first() if len(li.xpath(".//div[@class='flood']//div/a/text()")) > 0 else None
                    item["case_url"] = li.xpath(".//div[@class='flood']//div/a/@href").extract_first() if len(li.xpath(".//div[@class='flood']//div/a/@href")) > 0 else None
                    item["totalprice"] = "".join(li.xpath(".//div[@class='priceInfo']/div[@class='totalPrice']//text()").getall()) if len(li.xpath(".//div[@class='priceInfo']/div[@class='totalPrice']//text()")) > 0 else None
                    item["unitprice"] = li.xpath(".//div[@class='priceInfo']/div[@class='unitPrice']/span/text()").extract_first() if len(li.xpath(".//div[@class='priceInfo']/div[@class='unitPrice']/span/text()")) > 0 else None
                    # print(item)
                    yield item
                # 下一頁
                next_dict = response.xpath("//div[@class='contentBottom clear']/div[@class='page-box fr']/div/@page-data").extract_first()
                next_dict = json.loads(next_dict)
                totalpage = int(next_dict["totalPage"])
                curpage = int(next_dict["curPage"])
                if curpage != totalpage:
                    curpage += 1
                    next_url = item["city_url"] + item["house_cate"] + "/pg" + str(curpage)
                    print(next_url)
                    yield scrapy.Request(
                        next_url,
                        callback=self.parse_city,
                        errback=self.errback_httpbin,
                        dont_filter=True,
                        meta={"item": deepcopy(response.meta["item"])}

                    )
            elif item["house_cate"] == "zufang":
                # 租房
                print("租房")
                div_list = response.xpath("//div[@class='content__article']/div[@class='content__list']/div")
                for div in div_list:
                    item["title"] = div.xpath("./div/p[1]/a/text()").extract_first()
                    item["title"] = item["title"].strip()
                    item["house_detail"] = item["city_url"][:-1] + div.xpath("./div/p[1]/a/@href").extract_first()
                    item["district"] = div.xpath("./div/p[2]/a[1]/text()").extract_first() if len(div.xpath("./div/p[2]/a[1]/text()")) > 0 else None
                    item["district_url"] = item["city_url"][:-2] + div.xpath("./div/p[2]/a[1]/@href").extract_first() if len(div.xpath("./div/p[2]/a[1]/@href")) > 0 else None
                    item["casename"] = div.xpath("./div/p[2]/a[2]/text()").extract_first() if len(div.xpath("./div/p[2]/a[2]/text()")) > 0 else None
                    item["casename_url"] = item["city_url"][:-2] + div.xpath("./div/p[2]/a[2]/@href").extract_first() if len(div.xpath("./div/p[2]/a[2]/@href")) > 0 else None
                    rooms = div.xpath("./div/p[2]//text()").getall()
                    rooms = "".join(rooms)
                    rooms = re.sub(r"\s", "", rooms).split("/")
                    item["rooms"] = "/".join(rooms[1:])
                    item["price"] = "".join(div.xpath("./div/span//text()").getall())
                    # print(item)
                    yield item

                # 下一頁
                totalpage = response.xpath("//div[@class='content__pg']/@data-totalpage").extract_first()
                curpage = div_page = response.xpath("//div[@class='content__pg']/@data-curpage").extract_first()
                totalpage = int(totalpage)
                curpage = int(curpage)
                if curpage != totalpage:
                    curpage += 1
                    next_url = item["city_url"] + "zufang/pg{}/".format(str(curpage))
                    print(next_url)
                    yield scrapy.Request(
                        next_url,
                        callback=self.parse_city,
                        errback=self.errback_httpbin,
                        dont_filter=True,
                        meta={"item": deepcopy(response.meta["item"])}
                    )
            elif item["house_cate"] == "xiaoqu":
                print("小區")
                li_list = response.xpath("//div[@class='leftContent']/ul[@class='listContent']/li")
                for li in li_list:
                    item["title"] = li.xpath("./div[@class='info']/div/a/text()").extract_first()
                    item["house_detail"] = li.xpath("./div[@class='info']/div/a/@href").extract_first()
                    case = li.xpath("./div[@class='info']/div[@class='houseInfo']//text()").getall()
                    case = "".join(case)
                    case = re.sub(r"\s", "", case).strip()
                    item["case"] = case.split("|")[1]
                    item["case_url"] = li.xpath("./div[@class='info']/div[@class='houseInfo']/a[last()]/@href").extract_first() if len(li.xpath("./div[@class='info']/div[@class='houseInfo']/a[last()]/@href")) > 0 else None
                    item["district"] = li.xpath("./div[@class='info']/div[@class='positionInfo']/a[1]/text()").extract_first() if len(li.xpath("./div[@class='info']/div[@class='positionInfo']/a[1]/text()")) > 0 else None
                    item["district_url"] = li.xpath("./div[@class='info']/div[@class='positionInfo']/a[1]/@href").extract_first() if len(li.xpath("./div[@class='info']/div[@class='positionInfo']/a[1]/@href")) > 0 else None
                    item["bizcircle"] = li.xpath("./div[@class='info']/div[@class='positionInfo']/a[2]/text()").extract_first() if len(li.xpath("./div[@class='info']/div[@class='positionInfo']/a[2]/text()")) > 0 else None
                    item["bizcircle_url"] = li.xpath("./div[@class='info']/div[@class='positionInfo']/a[2]/@href").extract_first() if len(li.xpath("./div[@class='info']/div[@class='positionInfo']/a[2]/@href")) > 0 else None
                    price = li.xpath("./div[@class='xiaoquListItemRight']/div[1]/div[1]//text()").getall()[:2]
                    price = "".join(price)
                    item["price"] = re.sub(r"m", "平方米", price)
                    item["totalSellCount"] = "".join(li.xpath("./div[@class='xiaoquListItemRight']/div[2]/a//text()").getall()) if len(li.xpath("./div[@class='xiaoquListItemRight']/div[2]/a//text()")) > 0 else None
                    # print(item["totalSellCount"])
                    yield item
                # 下一頁
                next_dict = response.xpath(
                    "//div[@class='contentBottom clear']/div[@class='page-box fr']/div/@page-data").extract_first()
                next_dict = json.loads(next_dict)
                totalpage = int(next_dict["totalPage"])
                curpage = int(next_dict["curPage"])
                if curpage != totalpage:
                    curpage += 1
                    next_url = item["city_url"] + item["house_cate"] + "/pg{}".format(str(curpage))
                    print(next_url)
                    yield scrapy.Request(
                        next_url,
                        callback=self.parse_city,
                        errback=self.errback_httpbin,
                        dont_filter=True,
                        meta={"item": deepcopy(response.meta["item"])}

                    )
            elif item["house_cate"] == "loupan":
                print("新房")
                li_list = response.xpath("//ul[@class='resblock-list-wrapper']/li")
                for li in li_list:
                    item["title"] = li.xpath("./div[@class='resblock-desc-wrapper']/div[@class='resblock-name']/a/text()").extract_first()
                    item["house_detail"] = item["city_url"][:-1] + li.xpath("./div[@class='resblock-desc-wrapper']/div[@class='resblock-name']/a/@href").extract_first()
                    item["district"] = li.xpath("./div[@class='resblock-desc-wrapper']/div[@class='resblock-location']/span[1]/text()").extract_first() if len(li.xpath("./div[@class='resblock-desc-wrapper']/div[@class='resblock-location']/span[1]/text()")) > 0 else None
                    item["block"] = li.xpath("./div[@class='resblock-desc-wrapper']/div[@class='resblock-location']/span[2]/text()").extract_first() if len(li.xpath("./div[@class='resblock-desc-wrapper']/div[@class='resblock-location']/span[2]/text()")) > 0 else None
                    item["address"] = li.xpath("./div[@class='resblock-desc-wrapper']/div[@class='resblock-location']/a[1]/text()").extract_first() if len(li.xpath("./div[@class='resblock-desc-wrapper']/div[@class='resblock-location']/a[1]/text()")) > 0 else None
                    price = li.xpath("./div[@class='resblock-desc-wrapper']/div[@class='resblock-price']//text()").getall()
                    price = "".join(price).strip()
                    price = re.sub(r"\s", "", price)
                    item["price"] = re.sub(r"总价", "|总价", price)
                    # print(item["price"])
                    yield item

                # 下一頁
                self.page += 1
                totalpagecount = response.xpath("//div[@class='page-box']/@data-total-count").extract_first()
                nowpagecount = 10 * int(self.page)
                tp = int(totalpagecount) / 10
                if int(totalpagecount) % 10 != 0:
                    tp += 1
                if self.page <= tp:
                     next_url = item["city_url"] + item["house_cate"] + "/pg{}".format(self.page)
                     print(next_url)
                     yield scrapy.Request(
                         next_url,
                         callback=self.parse_city,
                         errback=self.errback_httpbin,
                         dont_filter=True,
                         meta={"item": deepcopy(response.meta["item"])}

                     )
        else:
            # 沒有符合的url不做任何動作
            pass

    def errback_httpbin(self, failure):
        # log all errback failures,
        # in case you want to do something special for some errors,
        # you may need the failure's type
        # print(repr(failure))
        # if isinstance(failure.value, HttpError):
        if failure.check(HttpError):
            # you can get the response
            response = failure.value.response
            self.logger.error('HttpError on %s', response.url)

            # elif isinstance(failure.value, DNSLookupError):
        elif failure.check(DNSLookupError):
            # this is the original request
            request = failure.request
            self.logger.error('DNSLookupError on %s', request.url)

            # elif isinstance(failure.value, TimeoutError):
        elif failure.check(TimeoutError):
            request = failure.request
            self.logger.error('TimeoutError on %s', request.url)