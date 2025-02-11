import scrapy
from test_crawler.items import TestCrawlerItem

class Bongda24hSpider(scrapy.Spider):
    name = "BongDa24H"
    start_urls = [
        "https://bongda24h.vn/bong-da/ket-qua.html", 
    ]

    def parse(self, response):
        # Duyệt qua từng trận đấu
        matches = response.xpath('//div[contains(@class, "matchdetail")]')
        # if not matches:
        #     matches = response.xpath('//div[@class="f-row matchdetail"]')
        for match in matches:
            item = TestCrawlerItem()
            
            item['days'] = match.xpath('.//div[@class="columns-time"]/span[@class="date"]/text()').get()

            teams = match.xpath('.//div[@class="row-teams"]/div[@class="columns-club"]/a/@title').getall()

            if not teams:
                teams = match.xpath('.//div[@class="row-teams"]/div[@class="columns-club"]/span/text()').getall()
                
            teams = [team.strip() for team in teams if team.strip()]

            item['name_match'] = ' - '.join(teams) if teams else "Không xác định"
            item['result'] = match.xpath('.//div[@class="columns-number"]/p/span[@class="soccer-scores"]/text()').get()
            item['match_rounds'] = match.xpath('.//div[@class="columns-time"]/span[@class="vongbang m_hiden"]/text()').get()

            yield item  
