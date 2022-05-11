# -*- coding: utf-8 -*-
import scrapy
from scrapy import signals
from scrapy import Spider
import re, html, os, itertools
from datetime import datetime
from compliance_crawlers.items import PEPItem
# from opencc import OpenCC
from scrapy.exceptions import CloseSpider
import json
from compliance_crawlers.settings import LOG_FILE_PATH
from compliance_crawlers.settings import MONGO_URI
from compliance_crawlers.settings import MONGO_DB
import string
from pymongo import MongoClient
import pymongo
from bs4 import BeautifulSoup
from compliance_crawlers.utils.utils import removePunctuation, remove_space, removeTextInsideParentheses
from compliance_crawlers.utils.table import html_table_to_list

# website: https://www.ey.gov.tw/Page/77A100A2C8409A3A

class TaiwanExecutiveYuan(scrapy.Spider):
    name = 'PEP_TWN_EXECUTIVE_YUAN'
    type = 'pep'
    start_url = 'https://www.ey.gov.tw/Page/77A100A2C8409A3A'
    base_url = 'https://www.ey.gov.tw'
    president_names = []
    vice_president_names = []
    custom_settings = {
        "LOG_FILE": os.path.join(LOG_FILE_PATH, '{:%Y%m%d%H%M%S}_{}.log'.format(datetime.now(), name)),
        "ROBOTSTXT_OBEY": False
    }

    def start_requests(self):
        return[scrapy.Request(self.start_url, callback=self.parse)]

    def create_pep_item(self, name, desig, incumbent, **kwargs):
        matching_keywords = []
        matching_keywords.append(name)
        desig = '台灣' + desig

        pepitem = PEPItem()

        pepitem['primary_key'] = name
        pepitem['data_source'] = 'PEP_TWN_EXECUTIVE_YUAN'
        pepitem['matching_keywords'] = matching_keywords
        pepitem['region'] = 'TWN'
        pepitem['additional_info'] = {
            'name': name,
            'designation': desig,
            'incumbent': incumbent
        }
        pepitem['incumbent'] = incumbent

        personal_info = {}
        for key, value in kwargs.items():
            if value:
                personal_info[key] = value
        if len(personal_info.keys()) > 0:
            pepitem['additional_info']['personal_info'] = personal_info

        return pepitem

    def parse(self, response):
        offices = response.xpath("//li[@class='mobile_menu']//a//text()").extract()
        office_urls = response.xpath("//li[@class='mobile_menu']//a//@href").extract()

        del offices[0]
        offices.pop()
        del office_urls[0]
        office_urls.pop()

        #chief
        name = response.xpath("//span[@class='h2']//text()").get()
        desig = response.xpath("//span[@class='h2']//text()").get()
        incumbent = True
        photo = response.xpath("//div[@class='other_link ail other_people']//img//@src").get()
        education = response.xpath("//ul[@class='principal'][1]//li//span[2]//text()").extract()

        name = re.sub('行政院|院長| ', '', name).strip()
        desig = re.sub(name, '', desig).strip()
        photo = self.base_url + photo
        for i in range(len(education)):
            education[i] = education[i].strip()

        if name not in self.president_names:
            self.president_names.append(name)
            yield self.create_pep_item(name, desig, incumbent, photo=photo, education=education)

        #others
        for i in range(len(offices)):
            if offices[i] == '副院長':
                url = f'{self.base_url}{office_urls[i]}'
                yield scrapy.Request(url, callback=self.parse_type_1)
            elif offices[i] == '歷任政府首長':
                url = f'{self.base_url}{office_urls[i]}'
                yield scrapy.Request(url, callback=self.parse_type_4)
            else:
                url = f'{self.base_url}{office_urls[i]}'
                if offices[i] == '政務委員' or offices[i] == '秘書長、發言人' or offices[i] == '副秘書長':
                    yield scrapy.Request(url, callback=self.parse_type_2)
                else:
                    yield scrapy.Request(url, callback=self.parse_type_3)

    def parse_type_1(self, response):
        name = response.xpath("//span[@class='h2']//text()").get()
        desig = response.xpath("//span[@class='h2']//text()").get()
        incumbent = True
        photo = response.xpath("//div[@class='other_link ail other_people']//img//@src").get()
        education = response.xpath("//ul[@class='principal'][1]//li//span[2]//text()").extract()

        name = re.sub('行政院|副院長| ', '', name).strip()
        desig = re.sub(name, '', desig).strip()
        photo = self.base_url + photo
        for i in range(len(education)):
            education[i] = education[i].strip()

        if name not in self.vice_president_names:
            self.vice_president_names.append(name)
            yield self.create_pep_item(name, desig, incumbent, photo=photo, education=education)


    def parse_type_2(self, response):
        urls = response.xpath("//li[@class='member_img hvr-outline-in']//a")

        for url in urls:
            link = url.xpath("@href").get()
            full_url = f'{self.base_url}{link}'
            #print(full_url)
            yield scrapy.Request(full_url, callback=self.parse_type_2_details)

    def parse_type_2_details(self, response):
        stopwords_list = ['行政院', '政務委員', '秘書長', '發言人', '政務副秘書長', '常務副秘書長', '兼', '\n', '\t', ' ']
        stopwords = '|'.join(stopwords_list)

        name = response.xpath("//span[@class='h2']//text()").get()
        desig = response.xpath("//span[@class='h2']//text()").get()
        name_and_desig = response.xpath("//span[@class='h2']//text()").get()
        incumbent = True
        photo = response.xpath("//div[@class='other_link ail other_people']//img//@src").get()
        education = response.xpath("//ul[@class='principal'][1]//li//span[2]//text()").extract()

        name = re.sub(stopwords, '', name).strip()
        desig = re.sub(name, '', desig).strip()
        photo = self.base_url + photo
        for i in range(len(education)):
            education[i] = education[i].strip()

        clean_name_and_desig = ''
        if name_and_desig.find('政務委員') > 0:
            clean_name_and_desig = name_and_desig[:name_and_desig.find('政務委員')+len('政務委員')]
        clean_name_and_desig = re.sub('\r|\t|\n', '', clean_name_and_desig).strip()
        name_and_desig = re.sub('\r|\t|\n', '', name_and_desig).strip()

        if len(clean_name_and_desig) != len(name_and_desig):
            if name_and_desig.find('發言人') < 0 and name_and_desig.find('秘書長') < 0:
                desig = response.xpath("//div[@class='top_control']//h2//text()").get()
                desig = re.sub('\r|\t|\n', '', desig).strip()
                name = re.sub(stopwords, '', clean_name_and_desig).strip()

        if desig.find('行政院') < 0:
            desig = '行政院' + desig

        yield self.create_pep_item(name, desig, incumbent, photo=photo, education=education)


    def parse_type_3(self, response):
        people = response.xpath("//ul[@class='grid2 effect']//li")
        stopwords_list = ['部長', '主任委員', '署長', '委員長', '主計長', '人事長', '總裁', '院長', '代理主任委員', '\n', '\t']
        stopwords = '|'.join(stopwords_list)

        for person in people:
            name = person.xpath(".//span[@class='title']//font//text()").extract()[0]
            desig_1st_part = person.xpath(".//span[@class='title']/span//text()").get()
            desig_2 = person.xpath(".//span[@class='title']//font//text()").get()
            incumbent = True
            photo = person.xpath(".//span//img//@src").get()

            name = re.sub(stopwords, '', name).strip()
            desig_2nd_part = re.sub(name, '', desig_2).strip()
            name = re.sub('[^\w]', '', name).strip()
            photo = self.base_url + photo

            if desig_2nd_part == '':
                desig_2_new = person.xpath(".//span[@class='title']//font//text()").extract()[1]
                desig_2nd_part = re.sub('\n|\t', '', desig_2_new).strip()

            desig = desig_1st_part + desig_2nd_part

            yield self.create_pep_item(name, desig, incumbent, photo=photo)

    def parse_type_4(self, response):
        former_vice_president_url = response.xpath("//a[@title='歷任副院長']//@href").get()
        former_vice_president_url = f'{self.base_url}{former_vice_president_url}'

        yield scrapy.Request(former_vice_president_url, callback=self.parse_former_vice_president)

        former_president_urls = response.xpath("//table[@class='table2 rwd-table bilingual']//tr//td//a//@href").extract()

        for url in former_president_urls:
            full_url = f'{self.base_url}{url}'
            yield scrapy.Request(full_url, callback=self.parse_former_president_details)

    def parse_former_president_details(self, response):
        name = response.xpath("//span[@class='h2']//text()").get()
        desig = response.xpath("//div[@class='top_control']//h2//text()").get()
        incumbent = False
        photo = response.xpath("//div[@class='other_link ail other_people']//img//@src").get()
        education = response.xpath("//div[@class='data_left col-8']/ul[1]//li//span[2]//text()").extract()
        experience_and_dates = response.xpath("//div[@class='data_left col-8']//ul[2]//li")
        term_of_office = ''

        name = re.sub('\n|\t| |先生|女士|\r|\u3000|行政院|院長', '', name).strip()
        desig = re.sub('\n|\t|\r|歷任', '', desig).strip()
        desig = '行政院' + desig
        photo = self.base_url + photo
        for i in range(len(education)):
            education[i] = education[i].strip()
            if education[i].find('\u3000') > 0:
                x = education[i].split('\u3000')
                education[i] = x[0]
                for n in range(1, len(x)):
                    education.insert(i+n, x[n])
        for item in experience_and_dates:
            experience = item.xpath(".//span[2]//text()").get()
            date = item.xpath(".//span[1]//text()").get()

            experience = re.sub(r' ?\([^)]+\)', '', experience).strip()
            experience = re.sub(' ', '', experience).strip()

            if experience.find('行政院院長') >= 0 or experience.find('行政院長') >= 0:
                date = re.sub(' ', '', date).strip()
                term_of_office += date + ','
        term_of_office = term_of_office[:term_of_office.rindex(',')]

        if name not in self.president_names:
            self.president_names.append(name)
            yield self.create_pep_item(name, desig, incumbent, photo=photo, education=education, tenure=term_of_office)

    def parse_former_vice_president(self, response):
        urls = response.xpath("//div[@class='words']/table[@class='table2 rwd-table bilingual'][1]//tr//td//a//@href").extract()

        for url in urls:
            full_url = f'{self.base_url}{url}'
            yield scrapy.Request(full_url, callback=self.parse_former_vice_president_details)

    def parse_former_vice_president_details(self, response):
        name = response.xpath("//span[@class='h2']//text()").get()
        desig = response.xpath("//div[@class='top_control']//h2//text()").get()
        incumbent = False
        photo = response.xpath("//div[@class='other_link ail other_people']//img//@src").get()
        education = response.xpath("//div[@class='data_left col-8']/ul[1]//li//span[2]//text()").extract()
        experience_and_dates = response.xpath("//div[@class='data_left col-8']//ul[2]//li")
        term_of_office = ''

        name = re.sub('\n|\t| |先生|女士|\r|\u3000', '', name).strip()
        desig = re.sub('\n|\t|\r|歷任', '', desig).strip()
        desig = '行政院' + desig
        photo = self.base_url + photo
        for i in range(len(education)):
            education[i] = education[i].strip()
        for item in experience_and_dates:
            experience = item.xpath(".//span[2]//text()").get()
            date = item.xpath(".//span[1]//text()").get()

            experience = re.sub('（第1任）|（第2任）', '', experience).strip()

            if experience.find('行政院副院長') >= 0:
                date = re.sub(' ', '', date).strip()
                term_of_office += date + ','
        term_of_office = term_of_office[:term_of_office.rindex(',')]

        if name not in self.vice_president_names:
            self.vice_president_names.append(name)
            yield self.create_pep_item(name, desig, incumbent, photo=photo, education=education, tenure=term_of_office)
