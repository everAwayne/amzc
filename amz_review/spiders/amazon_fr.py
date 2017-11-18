import re
import time
import json
from lxml import etree

MONTH_MAP = {
    "janvier": "January",
    "février": "February",
    "mars": "March",
    "avril": "April",
    "mai": "May",
    "juin": "June",
    "juillet": "July",
    "août": "August",
    "septembre": "September",
    "octobre": "October",
    "novembre": "November",
    "décembre": "December",
}

class AMZFRReviewInfo(object):
    """Extract next page num and all reviews of current page.
    """
    def __init__(self, soup):
        self.soup = soup

    def is_review_page(self):
        """Determinate page is a review page or not
        """
        if self.soup.xpath("//div[@id='cm_cr-review_list']"):
            return True
        return False

    def get_info(self):
        """Pack review info
        """
        page_info = self.get_page_info()
        review_ls = self.get_review_ls()
        return page_info, review_ls

    def get_page_info(self):
        info = {
            "cur_page": 1,
            "cur_page_url": None,
            "next_page_url": None,
        }
        btn_ls = self.soup.xpath("//div[@id='cm_cr-pagination_bar']//li[@data-reftag='cm_cr_arp_d_paging_btm']")
        selected_index = None
        for i in range(len(btn_ls)):
            if 'a-selected' in ' '.join(btn_ls[i].xpath("./@class")):
                selected_index = i
                break
        if selected_index is not None:
            page = btn_ls[selected_index].xpath("./a/text()")
            url = ''.join(btn_ls[selected_index].xpath("./a/@href"))
            if page:
                info['cur_page'] = int(page[0])
            info['cur_page_url'] = url
            if selected_index+1 < len(btn_ls):
                url = ''.join(btn_ls[selected_index+1].xpath("./a/@href"))
                info['next_page_url'] = url
        return info

    def month_translate(self, date):
        for m in MONTH_MAP:
            if m in date:
                return date.replace(m, MONTH_MAP[m])
        return date

    def get_review_ls(self):
        review_ls = []
        ls = self.soup.xpath('//div[@id="cm_cr-review_list"]/div[@data-hook="review"]')
        for item in ls:
            review_info = {
                'review_id': '',
                'rating': '',
                'title': '',
                'content': '',
                'author': '',
                'author_id': '',
                'date': '',
                'verified_purchase': '',
            }
            tmp_ls = item.xpath('./@id')
            if tmp_ls:
                review_info['review_id'] = tmp_ls[0].strip()

            tmp = ' '.join(item.xpath('.//i[@data-hook="review-star-rating"]/span/text()'))
            tmp_ls = re.findall(r'[\d,]+', tmp)
            if tmp_ls:
                review_info['rating'] = float(tmp_ls[0].replace(',', '.'))

            tmp_ls = item.xpath('.//a[@data-hook="review-title"]/text()')
            if tmp_ls:
                review_info['title'] = tmp_ls[0].strip()

            tmp_ls = item.xpath('.//span[@data-hook="review-body"]/text()')
            if tmp_ls:
                review_info['content'] = tmp_ls[0].strip()

            tmp_ls = item.xpath('.//a[@data-hook="review-author"]/text()')
            if tmp_ls:
                review_info['author'] = tmp_ls[0].strip()

            tmp = ' '.join(item.xpath('.//a[@data-hook="review-author"]/@href'))
            tmp_ls = re.findall(r'/profile/([^/]+?)/', tmp)
            if tmp_ls:
                review_info['author_id'] = tmp_ls[0].strip()

            tmp_ls = item.xpath('.//span[@data-hook="review-date"]/text()')
            if tmp_ls:
                date = self.month_translate(tmp_ls[0].strip().lower())
                review_info['date'] = time.strftime("%Y-%m-%d", time.strptime(date, "le %d %B %Y"))

            tmp_ls = item.xpath(".//span[@data-hook='avp-badge']")
            review_info['verified_purchase'] = True if tmp_ls else False

            review_ls.append(review_info)
        return review_ls
