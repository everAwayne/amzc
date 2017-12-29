#encoding=utf-8

import re
import json
from .amazon_base import AMZProductInfo


class AMZESProductInfo(AMZProductInfo):
    """Extract product info from page
    """
    def get_review(self):
        """Extract review info
        """
        text_ls = self.soup.xpath("//div[@id='averageCustomerReviews_feature_div']//i[contains(@class,'a-icon-star')]//text()")
        review_info = ' '.join([i.strip() for i in text_ls if i.strip()])
        review_score_ls = re.findall(r'[\d.,]+', review_info)
        review_score = 0
        if review_score_ls:
            review_score = float(review_score_ls[0])
        text_ls = self.soup.xpath("//div[@id='averageCustomerReviews_feature_div']//a[@id='acrCustomerReviewLink']//text()")
        review_info = ' '.join([i.strip() for i in text_ls if i.strip()])
        review_num_ls = re.findall(r'[\d.,]+', review_info)
        review_num = 0
        if review_num_ls:
            review_num = int(review_num_ls[0].replace(',',''))
        review_statistics = {1:0,2:0,3:0,4:0,5:0}
        tr_ls = self.soup.xpath("//table[@id='histogramTable']//tr[@class='a-histogram-row']")
        for tr in tr_ls:
            text = ''.join(tr.xpath("./td[1]/a/text()"))
            reg_res = re.search(r"(\d+)", text)
            star = int(reg_res.group(1)) if reg_res else None
            text = ''.join(tr.xpath("./td[2]//div[@aria-label]/@aria-label"))
            reg_res = re.search(r"(\d+)", text)
            prc = int(reg_res.group(1)) if reg_res else None
            if star is not None and prc is not None:
                review_statistics[star] = prc

        return {'review_score': review_score, 'review_num': review_num, 'review_statistics': review_statistics}


    def is_fba(self):
        """Extract fba info
        """
        text_ls = self.soup.xpath("//*[@id='merchant-info']//text()")
        fba_info = ' '.join([i.strip() for i in text_ls if i.strip()]).lower()
        if 'gestionado por amazon' in fba_info:
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
            price_text = price_ls[0].replace('.','').replace(',','.')
            o_price = float(price_text)

        price_ls = self.soup.xpath("//div[@id='price_feature_div']//div[@id='price']//*[@id='priceblock_ourprice']//text()")
        if not price_ls:
            price_ls = self.soup.xpath("//div[@id='price_feature_div']//div[@id='price']//*[contains(@id,'priceblock_saleprice')]//text()")
        if not price_ls:
            price_ls = self.soup.xpath("//div[@id='price_feature_div']//div[@id='price']//*[contains(@id,'priceblock_dealprice')]//text()")
        price_text = ' '.join([i.strip() for i in price_ls if i.strip()])
        price_ls = re.findall(r'[\d.,]+', price_text)
        if price_ls:
            price_text = price_ls[0].replace('.','').replace(',','.')
            price = float(price_text)

        discount = 1
        if o_price and price:
            discount = round(price/o_price, 2)

        return {'o_price': o_price, 'price': price, 'discount': discount}


    def get_product_info(self):
        """Extract product info
        """
        bsr_dct = {
            'cat_1_rank': None,
            'cat_1_name': None,
            'cat_ls': [],
        }
        product_info_dct = {
            'product_dimensions': None,
            'shipping_weight': None,
            'date_first_available': None,
        }
        div1 = self.soup.xpath("//*[@id='detail-bullets' or @id='detail_bullets_id']")
        div2 = self.soup.xpath("//*[@id='detailBullets']")
        tr_ls1 = self.soup.xpath("//div[@id='prodDetails']//div[@class='pdTab']//table//tr")
        tr_ls2 = self.soup.xpath("//div[@id='prodDetails']//table[@role='presentation']//tr")
        if div1:
            div = div1[0]
            li_ls = div.xpath(".//div[@class='content']/ul/li[@id='SalesRank']")
            if li_ls:
                li = li_ls[0]
                text = ' '.join(li.xpath("./text()")).strip()
                reg_res = re.search(r'nº\s*([\d.]+)\s+en\s+(.+)\(', text)
                if reg_res:
                    rank = int(reg_res.group(1).replace('.', ''))
                    name = reg_res.group(2).strip()
                    bsr_dct['cat_1_rank'] = rank
                    bsr_dct['cat_1_name'] = name
                cat_li_ls = li.xpath("./ul[@class='zg_hrsr']/li[@class='zg_hrsr_item']")
                for li in cat_li_ls:
                    reg_res = re.search(r'n\.?°\s*([\d.]+)',''.join(li.xpath("./span[@class='zg_hrsr_rank']/text()")))
                    rank = int(reg_res.group(1).replace('.', '').strip()) if reg_res else None
                    name_ls = li.xpath("./span[@class='zg_hrsr_ladder']//a/text()")
                    bsr_dct['cat_ls'].append({"rank": rank, "name_ls": name_ls})

            li_ls = div.xpath(".//div[@class='content']/ul/li")
            for li in li_ls:
                name = ''.join(li.xpath("./b/text()")).replace(":", "").strip().lower()
                text = ''.join(li.xpath("./text()")).strip()
                if 'dimensiones del producto' == name:
                    product_info_dct['product_dimensions'] = text
                elif 'peso del producto' == name:
                    product_info_dct['shipping_weight'] = text
                elif 'producto en' in name:
                    product_info_dct['date_first_available'] = text
        elif div2:
            div = div2[0]
            li_ls = div.xpath(".//li[@id='SalesRank']")
            if li_ls:
                li = li_ls[0]
                text = ' '.join(li.xpath("./text()")).strip()
                reg_res = re.search(r'nº\s*([\d.]+)\s+en\s+(.+)\(', text)
                if reg_res:
                    rank = int(reg_res.group(1).replace('.', ''))
                    name = reg_res.group(2).strip()
                    bsr_dct['cat_1_rank'] = rank
                    bsr_dct['cat_1_name'] = name
                cat_li_ls = li.xpath("./ul[@class='zg_hrsr']/li[@class='zg_hrsr_item']")
                for li in cat_li_ls:
                    reg_res = re.search(r'n\.?°\s*([\d.]+)',''.join(li.xpath("./span[@class='zg_hrsr_rank']/text()")))
                    rank = int(reg_res.group(1).replace('.', '').strip()) if reg_res else None
                    name_ls = li.xpath("./span[@class='zg_hrsr_ladder']//a/text()")
                    bsr_dct['cat_ls'].append({"rank": rank, "name_ls": name_ls})

            span_ls = div.xpath(".//div[@id='detailBullets_feature_div']/ul/li/span")
            for span in span_ls:
                name = ''.join(span.xpath("./span[1]/text()")).replace(":", "").strip().lower()
                text = ''.join(span.xpath("./span[2]/text()")).strip()
                if 'dimensiones del producto' == name:
                    product_info_dct['product_dimensions'] = text
                elif 'peso del producto' == name:
                    product_info_dct['shipping_weight'] = text
                elif 'producto en' in name:
                    product_info_dct['date_first_available'] = text
        elif tr_ls1:
            for tr in tr_ls1:
                k_ls = tr.xpath("./td[@class='label']/text()")
                if not k_ls:
                    continue
                if 'SalesRank' in tr.xpath("./@id"):
                    text = ' '.join(tr.xpath("./td[@class='value']/text()")).strip()
                    reg_res = re.search(r'nº\s*([\d.]+)\s+en\s+(.+)\(', text)
                    if reg_res:
                        rank = int(reg_res.group(1).replace('.', ''))
                        name = reg_res.group(2).strip()
                        bsr_dct['cat_1_rank'] = rank
                        bsr_dct['cat_1_name'] = name
                    cat_li_ls = tr.xpath("./td[@class='value']/ul[@class='zg_hrsr']/li[@class='zg_hrsr_item']")
                    for li in cat_li_ls:
                        reg_res = re.search(r'n\.?°\s*([\d.]+)',''.join(li.xpath("./span[@class='zg_hrsr_rank']/text()")))
                        rank = int(reg_res.group(1).replace('.', '').strip()) if reg_res else None
                        name_ls = li.xpath("./span[@class='zg_hrsr_ladder']//a/text()")
                        bsr_dct['cat_ls'].append({"rank": rank, "name_ls": name_ls})
                else:
                    name = ''.join(tr.xpath("./td[@class='label']/text()")).replace(":", "").strip().lower()
                    text = ''.join(tr.xpath("./td[@class='value']/text()")).strip()
                    if 'dimensiones del producto' == name:
                        product_info_dct['product_dimensions'] = text
                    elif 'peso del producto' == name:
                        product_info_dct['shipping_weight'] = text
                    elif 'producto en' in name:
                        product_info_dct['date_first_available'] = text
        elif tr_ls2:
            for tr in tr_ls2:
                k_ls = tr.xpath("./th/text()")
                if not k_ls:
                    continue
                k = k_ls[0].replace(":", "").strip().lower()
                if 'best sellers rank' in k:
                    span_ls = tr.xpath("./td/span/span")
                    if len(span_ls):
                        text = span_ls[0].xpath("./text()")[0]
                        reg_res = re.search(r'nº\s*([\d.]+)\s+en\s+(.+)\(', text)
                        if reg_res:
                            rank = int(reg_res.group(1).replace('.', ''))
                            name = reg_res.group(2).strip()
                            bsr_dct['cat_1_rank'] = rank
                            bsr_dct['cat_1_name'] = name
                    span_ls = span_ls[1:]
                    for span in span_ls:
                        text = ' '.join(span.xpath("./text()")).strip()
                        reg_res = re.search(r'n\.?°\s*([\d.]+)', text)
                        rank = int(reg_res.group(1).replace('.', '').strip()) if reg_res else None
                        name_ls = span.xpath(".//a/text()")
                        bsr_dct['cat_ls'].append({"rank": rank, "name_ls": name_ls})
                else:
                    name = k
                    text = ''.join(tr.xpath("./td/text()")).strip()
                    if 'dimensiones del producto' == name:
                        product_info_dct['product_dimensions'] = text
                    elif 'peso del producto' == name:
                        product_info_dct['shipping_weight'] = text
                    elif 'producto en' in name:
                        product_info_dct['date_first_available'] = text
        return {"bsr_info": bsr_dct, "product_info": product_info_dct}
