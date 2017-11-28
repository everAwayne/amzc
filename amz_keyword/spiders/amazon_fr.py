import re
from .amazon_base import AMZSearchInfo

class AMZFRSearchInfo(AMZSearchInfo):
    """Extract next page url and all asins of current page.
    """
    def get_search_result(self):
        dct = {'count': None, 'category': []}
        text_ls = self.soup.xpath('//div[@id="s-result-info-bar-content"]//h1[@id="s-result-count"]/text()')
        text = ' '.join(text_ls).lower()
        reg_ret = re.search(r'([\d\s]+)\s*résultats', text)
        if reg_ret:
            dct['count'] = int(reg_ret.group(1).replace('\xa0',''))
        text_ls = self.soup.xpath('//div[@id="s-result-info-bar-content"]//h1[@id="s-result-count"]/span/a/text()')
        if text_ls:
            dct['category'] = [item.strip() for item in text_ls]
        return dct
