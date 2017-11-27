import re
from urllib import  parse
from html.parser import HTMLParser

class AmazonSearchPage(object):

    def __init__(self, soup):
        self.selector = soup
    
    def get_next_url(self, host):
        hrefs = self.selector.xpath('//a[@id="pagnNextLink"]/@href')
        if not hrefs:
            return None
            
        next_url= host + HTMLParser().unescape(hrefs[0])
        return next_url
    
    @property
    def products(self):
        return self._parse_list_product()

    def _parse_list_product(self):
        products = []
        for li in self.selector.xpath('//div[@id="atfResults"]/ul/li | //div[@id="btfResults"]/ul/li'):
            id = li.xpath('@id')
            rank = int(id[0].split('_')[1]) + 1 if id else -1
            data_asin = li.xpath('@data-asin')
            is_sponsored = li.xpath('.//h5/text()')
            products.append({
                'rank': rank,
                'asin': data_asin[0] if data_asin else '',
                'is_sponsored': 1 if is_sponsored \
                    and is_sponsored[0] == 'Sponsored' else 0
            })
        
        return products
    
    @property
    def right_products(self):
        return self._parse_right_product()

    def _parse_right_product(self):
        products = []
        rank = 1
        for div in self.selector.xpath('//div[@id="paRightContent"]//div[contains(@class,"pa-ad-details")]'):
            href = div.xpath('./div[1]/a/@href')
            if not href:
                continue
            
            o = parse.urlparse(href[0])
            query = parse.parse_qs(o.query)
            if 'url' not in query:
                continue
            
            asin = re.findall(r'(?:https?:\/\/www\.amazon\.(?:com|co\.uk|de|co\.jp|fr|it|es|ca)\/dp\/)([a-zA-Z0-9]+)(?:\/ref=.*)', query['url'][0])
            if not asin:
                continue
            
            products.append({
                'asin': asin[0],
                'rank': rank
            })

            rank += 1

        return products