#encoding=utf-8

import re
import json
#import execjs
from lxml import etree


class AMZJPProductInfo:
    """Extract product info from page
    """
    def __init__(self, soup):
        self.soup = soup

    def is_product_page(self):
        """Determinate page is a product page or not
        """
        if self.soup.xpath("//div[@id='dp-container']"):
            return True
        return False

    def get_info(self):
        """Pack product info
        """
        is_fba = self.is_fba()
        title_info = self.get_title()
        brand_info = self.get_brand()
        price_info = self.get_price()
        img_info = self.get_img_info()
        review_info = self.get_review()
        merchant_info = self.get_merchants_info()
        bsr_info = self.get_bsr_info()
        relative_info = self.get_relative_asin()

        return {
            'fba': 1 if is_fba else 0,
            'title': title_info['title'],
            'brand': brand_info['brand'],
            'price': price_info['price'],
            'discount': price_info['discount'],
            'img': img_info['img'],
            #'imgs': img_info['imgs'],
            'review': review_info['review_score'],
            'review_count': review_info['review_num'],
            'merchant': merchant_info['merchant'],
            'merchant_id': merchant_info['merchant_id'],
            'detail_info': bsr_info,
            'relative_info': relative_info,
            }

    def get_title(self):
        """Extract title info
        """
        t_text_ls = self.soup.xpath("//*[@id='title']//text()")
        text_ls = t_text_ls
        title = ' '.join([i.strip() for i in text_ls if i.strip()])
        return {'title': title}


    def get_brand(self):
        """Extract brand info
        """
        t_text_ls = self.soup.xpath("//*[@id='brand']//text()")
        text_ls = t_text_ls
        brand = ' '.join([i.strip() for i in text_ls if i.strip()])
        return {'brand': brand}


    def get_img_info(self):
        """Extract image info
        """
        img_dct = {'img': '', 'imgs':[]}
        img_ls = []
        img_info = self.soup.xpath("//*[@id='imageBlock_feature_div']//script[contains(text(),'ImageBlockATF')]/text()")[0]
        reg_ret = re.search(r"var\s+data\s*=\s*({.+});", img_info, re.S)
        if reg_ret:
            ls = re.findall(r'''["']large["']\s*:\s*["'](.+?)["']''', reg_ret.group(1), re.M)
            img_ls.extend(ls)
            #img_info_dct = execjs.eval(reg_ret.group(1))
            #for item in img_info_dct['colorImages']['initial']:
            #    img_ls.append(item['large'])

        img_dct['imgs'] = img_ls
        img_dct['img'] = img_dct['imgs'][0] if img_dct['imgs'] else ''
        return img_dct


    def get_review(self):
        """Extract review info
        """
        text_ls = self.soup.xpath("//div[@id='averageCustomerReviews_feature_div']//i[contains(@class,'a-icon-star')]//text()")
        review_info = ' '.join([i.strip() for i in text_ls if i.strip()])
        review_score_ls = re.findall(r'[\d.,]+', review_info)
        review_score = 0
        if review_score_ls:
            review_score = float(review_score_ls[1])
        text_ls = self.soup.xpath("//div[@id='averageCustomerReviews_feature_div']//a[@id='acrCustomerReviewLink']//text()")
        review_info = ' '.join([i.strip() for i in text_ls if i.strip()])
        review_num_ls = re.findall(r'[\d.,]+', review_info)
        review_num = 0
        if review_num_ls:
            review_num = int(review_num_ls[0].replace(',',''))

        return {'review_score': review_score, 'review_num': review_num}


    def is_fba(self):
        """Extract fba info
        """
        text_ls = self.soup.xpath("//*[@id='merchant-info']//text()")
        fba_info = ' '.join([i.strip() for i in text_ls if i.strip()]).lower()
        if 'amazon.co.jp が発送' in fba_info:
            return True
        return False


    def get_price(self):
        """Extract price info
        """
        o_price = price = 0
        o_price_ls = self.soup.xpath("//div[@id='price_feature_div']//div[@id='price']//*[@class='a-text-strike']//text()")
        price_text = ' '.join([i.strip() for i in o_price_ls if i.strip()])
        price_ls = re.findall(r'[\d.,]+', price_text)
        if price_ls:
            price_text = price_ls[0].replace(',','')
            o_price = float(price_text)

        price_ls = self.soup.xpath("//div[@id='price_feature_div']//div[@id='price']//*[@id='priceblock_ourprice']//text()")
        if not price_ls:
            price_ls = self.soup.xpath("//div[@id='price_feature_div']//div[@id='price']//*[contains(@id,'priceblock_saleprice')]//text()")
        if not price_ls:
            price_ls = self.soup.xpath("//div[@id='price_feature_div']//div[@id='price']//*[contains(@id,'priceblock_dealprice')]//text()")
        price_text = ' '.join([i.strip() for i in price_ls if i.strip()])
        price_ls = re.findall(r'[\d.,]+', price_text)
        if price_ls:
            price_text = price_ls[0].replace(',','')
            price = float(price_text)

        discount = 1
        if o_price and price:
            discount = round(price/o_price, 2)

        return {'o_price': o_price, 'price': price, 'discount': discount}


    def get_merchants_info(self):
        """Extract merchant info
        """
        text_ls = self.soup.xpath("//*[@id='merchant-info']//a[contains(@href,'seller/at-a-glance.html/ref=dp_merchant_link')]/text()")
        merchant = ' '.join([i.strip() for i in text_ls if i.strip()]).strip()

        text_ls = self.soup.xpath("//form[@id='addToCart']//input[@id='merchantID']/@value")
        merchant_id = ' '.join([i.strip() for i in text_ls if i.strip()]).strip()
        return {"merchant": merchant, "merchant_id": merchant_id}


    def get_bsr_info(self):
        """Extract bsr info
        """
        bsr_dct = {}
        div1 = self.soup.xpath("//*[@id='detail-bullets' or @id='detail_bullets_id']")
        div2 = self.soup.xpath("//*[@id='detailBullets']")
        tr_ls1 = self.soup.xpath("//div[@id='prodDetails']//div[@class='pdTab']//table//tr")
        tr_ls2 = self.soup.xpath("//div[@id='prodDetails']//table[@role='presentation']//tr")
        if div1:
            div = div1[0]
            li_ls = div.xpath(".//div[@class='content']/ul/li")
            for li in li_ls:
                if 'SalesRank' in li.xpath("./@id"):
                    text = ' '.join(li.xpath("./text()")).strip()
                    reg_res = re.search(r'(.+)\s+-\s+([\d,]+)', text)
                    if reg_res:
                        rank = int(reg_res.group(2).replace(',', ''))
                        name = reg_res.group(1).strip()
                        bsr_dct['cat_1_rank'] = rank
                        bsr_dct['cat_1_name'] = name
        elif div2:
            div = div2[0]
            li_ls = div.xpath(".//li[@id='SalesRank']")
            if li_ls:
                li = li_ls[0]
                text = ' '.join(li.xpath("./text()")).strip()
                reg_res = re.search(r'(.+)\s+-\s+([\d,]+)', text)
                if reg_res:
                    rank = int(reg_res.group(2).replace(',', ''))
                    name = reg_res.group(1).strip()
                    bsr_dct['cat_1_rank'] = rank
                    bsr_dct['cat_1_name'] = name
        elif tr_ls1:
            for tr in tr_ls1:
                k_ls = tr.xpath("./td[@class='label']/text()")
                if not k_ls:
                    continue
                if 'SalesRank' in tr.xpath("./@id"):
                    text = ' '.join(tr.xpath("./td[@class='value']/text()")).strip()
                    reg_res = re.search(r'(.+)\s+-\s+([\d,]+)', text)
                    if reg_res:
                        rank = int(reg_res.group(2).replace(',', ''))
                        name = reg_res.group(1).strip()
                        bsr_dct['cat_1_rank'] = rank
                        bsr_dct['cat_1_name'] = name
        elif tr_ls2:
            for tr in tr_ls2:
                k_ls = tr.xpath("./th/text()")
                if not k_ls:
                    continue
                k = k_ls[0].strip().strip(":").lower()
                if 'best sellers rank' in k:
                    span_ls = tr.xpath("./td/span/span")
                    for span in span_ls:
                        text = span.xpath("./text()")[0]
                        reg_res = re.search(r'(.+)\s+-\s+([\d,]+)', text)
                        if reg_res:
                            rank = int(reg_res.group(2).replace(',', ''))
                            name = reg_res.group(1).strip()
                            bsr_dct['cat_1_rank'] = rank
                            bsr_dct['cat_1_name'] = name
        return bsr_dct


    def get_relative_asin(self):
        """Extract asin from 'bought together' and 'also bought'
        """
        ls_1 = self.soup.xpath("//div[@id='sims-fbt-content']//input[contains(@name, 'discoveredAsins')]/@value")
        ls_2 = self.soup.xpath("//div[@id='purchase-sims-feature']//div[@data-a-carousel-options]/@data-a-carousel-options")
        if ls_2:
            ls_2 = json.loads(ls_2[0])['ajax']['id_list']

        return {'bought_together': ls_1, 'also_bought': ls_2}
