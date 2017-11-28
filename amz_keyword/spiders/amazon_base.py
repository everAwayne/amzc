import re
import json


class AMZSearchInfo:
    """Extract next level category urls, untill each bottom level,
    and then extract all asins of current category.
    """
    def __init__(self, soup):
        self.soup = soup

    def is_search_page(self):
        """Determinate page is a search page or not
        """
        if self.soup.xpath("//div[@id='searchTemplate']"):
            return True
        return False

    def get_next_url(self):
        hrefs = self.soup.xpath('//a[@id="pagnNextLink"]/@href')
        if not hrefs:
            return
        return hrefs[0]

    def get_asins(self):
        asin_ls = []
        for li in self.soup.xpath('//div[@id="atfResults"]/ul/li | //div[@id="btfResults"]/ul/li'):
            _id = li.xpath('@id')
            rank = int(_id[0].split('_')[1]) + 1 if _id else -1
            data_asin = li.xpath('@data-asin')
            is_sponsored = li.xpath('.//h5//span[contains(@class, "sponsored")]')
            asin_ls.append({
                'rank': rank,
                'asin': data_asin[0] if data_asin else '',
                'is_sponsored': 1 if is_sponsored else 0
            })
        return asin_ls

    def get_search_result(self):
        dct = {'count': None, 'category': []}
        text_ls = self.soup.xpath('//div[@id="s-result-info-bar-content"]//h1[@id="s-result-count"]/text()')
        text = ' '.join(text_ls).lower()
        reg_ret = re.search(r'([\d,]+)\s*results', text)
        if reg_ret:
            dct['count'] = int(reg_ret.group(1).replace(',',''))
        text_ls = self.soup.xpath('//div[@id="s-result-info-bar-content"]//h1[@id="s-result-count"]/span/a/text()')
        if text_ls:
            dct['category'] = [item.strip() for item in text_ls]
        return dct
