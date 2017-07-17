import re
import json
from lxml import etree


class AMZINBSRCategoryInfo(object):
    """Extract next level category urls, untill each bottom level,
    and then extract all asins of current category.
    """
    def __init__(self, html):
        self.soup = etree.HTML(html, parser=etree.HTMLParser(encoding='utf-8'))

    def is_bsr_page(self):
        """Determinate page is a product page or not
        """
        if self.soup.xpath("//ul[@id='zg_browseRoot']"):
            return True
        return False

    def is_captcha_page(self):
        """Determinate the page is a captcha page or not
        """
        if self.soup.xpath('//*[contains(@action, "validateCaptcha")]'):
            return True
        return False

    def get_info(self, cate_filter, is_exist):
        url_ls = []
        asin_ls = []
        root = self.soup.xpath("//ul[@id='zg_browseRoot']")
        selected = root[0].xpath(".//li/span[@class='zg_selected']")[0]
        a_ls = selected.getparent().getparent().xpath(".//ul/li/a")
        if a_ls:
            if not is_exist:
                for a in a_ls:
                    if a.text.strip().lower() in cate_filter:
                        continue
                    url_ls.append(a.attrib['href'])
        else:
            page_id = self.soup.xpath("//ol[@class='zg_pagination']/li[contains(@class,'zg_selected')]/@id")[0]
            if page_id == 'zg_page1':
                ls = self.soup.xpath("//ol[@class='zg_pagination']/li[@id!='zg_page1']/a/@href")
                if not is_exist:
                    url_ls.extend([i for i in ls])
            info_ls = self.soup.xpath("//div[@id='zg_centerListWrapper']//div[@class='zg_itemImmersion']//div[@data-p13n-asin-metadata]/@data-p13n-asin-metadata")
            ul = root[0].xpath("./ul")[0]
            cate_ls = []
            while ul is not None:
                next_ul = ul.xpath("./ul")
                if next_ul:
                    cate_ls.append(' '.join(ul.xpath("./li/a/text()")).strip())
                    ul = next_ul[0]
                else:
                    cate_ls.append(selected.text)
                    ul = None
            cate = ':'.join(cate_ls)
            asin_ls = map(lambda x:{'asin': json.loads(x)['asin'], 'cate': cate}, info_ls)
        return url_ls, asin_ls
