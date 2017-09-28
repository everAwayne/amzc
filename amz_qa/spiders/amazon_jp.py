import re
import time
import json
from lxml import etree


class AMZJPQAInfo(object):
    """Extract next page num and all qas of current page.
    """
    def __init__(self, soup):
        self.soup = soup

    def is_qa_page(self):
        """Determinate page is a qa page or not
        """
        if self.soup.xpath('//div[contains(@class,"askTeaserQuestions")]'):
            return True
        return False

    def get_info(self):
        """Pack review info
        """
        next_page = self.get_next_page()
        qa_ls = self.get_qa_ls()
        return next_page, qa_ls

    def get_next_page(self):
        btn_ls = self.soup.xpath("//div[@id='askPaginationBar']/ul/li[@class='a-normal' or @class='a-selected']")
        selected_index = 0
        for i in range(len(btn_ls)):
            if 'a-selected' in ' '.join(btn_ls[i].xpath("./@class")):
                selected_index = i
                break
        if selected_index+1 < len(btn_ls):
            page = btn_ls[selected_index+1].xpath("./a/text()")
            if page:
                return int(page[0])
        return None

    def get_qa_ls(self):
        qa_ls = []
        ls = self.soup.xpath('//div[contains(@class,"askTeaserQuestions")]/div[contains(@class,"a-spacing-base")]')
        for item in ls:
            qa_info = {
                'qa_id': '',
                'vote': '',
                'question': '',
                'answer': '',
                'author': '',
                'date': '',
            }
            tmp_ls = item.xpath('.//div[contains(@id, "question")]/@id')
            if tmp_ls:
                id_ls = tmp_ls[0].strip().split('-')
                if len(id_ls) >= 2:
                    qa_info['qa_id'] = id_ls[1]

            tmp_ls = item.xpath('.//ul[contains(@class, "vote")]/li[@class="label"]/span[@class="count"]/text()')
            if tmp_ls:
                qa_info['vote'] = int(tmp_ls[0].strip())

            tmp_ls = item.xpath('.//div[contains(@id, "question")]//a[@class="a-link-normal"]/text()')
            if tmp_ls:
                qa_info['question'] = tmp_ls[0].strip()

            right_ls = item.xpath('.//div[contains(@class, "a-col-right")]//div[contains(@class,"a-spacing-base")]//div[contains(@class,"a-col-right")]')
            if right_ls:
                tmp_ls = right_ls[0].xpath('.//span[@class="askLongText"]/text()')
                if not tmp_ls:
                    tmp_ls = right_ls[0].xpath('.//span[1]/text()')
                if tmp_ls:
                    qa_info['answer'] = tmp_ls[0].strip()

            tmp_ls = item.xpath('.//span[@class="a-color-tertiary"]/text()')
            tmp = ' '.join([item.strip() for item in tmp_ls])
            reg_ret = re.search(r"投稿者:\s+(.+?)、投稿日:\s+(.+)$", tmp)
            if reg_ret:
                qa_info['author'] = reg_ret.group(1).strip()
                date = reg_ret.group(2).strip()
                qa_info['date'] = time.strftime("%Y-%m-%d", time.strptime(date, "%Y/%m/%d"))

            qa_ls.append(qa_info)
        return qa_ls
