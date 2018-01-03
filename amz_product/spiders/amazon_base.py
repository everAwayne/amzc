import re
import json
import js2py


class AMZProductInfo:
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
        p_asin_info = self.get_parent_asin()
        title_info = self.get_title()
        brand_info = self.get_brand()
        price_info = self.get_price()
        img_info = self.get_img_info()
        review_info = self.get_review()
        merchant_info = self.get_merchants_info()
        product_info = self.get_product_info()
        relative_info = self.get_relative_asin()
        sku_info = self.get_sku_info()

        return {
            'fba': 1 if is_fba else 0,
            'parent_asin': p_asin_info['asin'],
            'title': title_info['title'],
            'brand': brand_info['brand'],
            'price': price_info['price'],
            'discount': price_info['discount'],
            'img': img_info['img'],
            #'imgs': img_info['imgs'],
            'review': review_info['review_score'],
            'review_count': review_info['review_num'],
            'review_statistics': review_info['review_statistics'],
            'merchant': merchant_info['merchant'],
            'merchant_id': merchant_info['merchant_id'],
            'detail_info': product_info['bsr_info'],
            'product_info': product_info['product_info'],
            'relative_info': relative_info,
            'sku_info': sku_info,
            }


    def get_parent_asin(self):
        """Extract parent asin
        """
        asin_ls = self.soup.xpath("//span[@id='twisterNonJsData']/input[@name='ASIN']/@value")
        return {'asin': asin_ls[0] if asin_ls else ''}


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


    def get_merchants_info(self):
        """Extract merchant info
        """
        text_ls = self.soup.xpath("//*[@id='merchant-info']//a[contains(@href,'seller/at-a-glance.html/ref=dp_merchant_link')]/text()")
        merchant = ' '.join([i.strip() for i in text_ls if i.strip()]).strip()

        text_ls = self.soup.xpath("//form[@id='addToCart']//input[@id='merchantID']/@value")
        merchant_id = ' '.join([i.strip() for i in text_ls if i.strip()]).strip()
        return {"merchant": merchant, "merchant_id": merchant_id}


    def get_relative_asin(self):
        """Extract asin from 'bought together' and 'also bought'
        """
        ls_1 = self.soup.xpath("//div[@id='sims-fbt-content']//input[contains(@name, 'discoveredAsins')]/@value")
        ls_2 = self.soup.xpath("//div[@id='purchase-sims-feature']//div[@data-a-carousel-options]/@data-a-carousel-options")
        ls_3 = self.soup.xpath("//div[@id='session-sims-feature']//div[@data-a-carousel-options]/@data-a-carousel-options")
        ls_4 = self.soup.xpath("//div[@id='recommendations']/ul/li/span/div[1]/a/@href")
        if ls_2:
            ls_2 = json.loads(ls_2[0])['ajax']['id_list']
        if ls_3:
            ls_3 = json.loads(ls_3[0])['ajax']['id_list']
        if ls_4:
            ls = []
            for item in ls_4:
                reg_ret = re.search(r'/dp/([^/]+)/', item)
                ls.append(reg_ret.group(1))
            ls_4 = ls

        return {'bought_together': ls_1, 'also_bought': ls_2, 'also_viewed': ls_3,
                'viewed_also_bought': ls_4}


    def get_sku_info(self):
        """Extract all sku asin
        """
        sku_ls = []
        script_text_ls = self.soup.xpath("//script[contains(text(), 'twister-js-init-mason-data') and contains(text(), 'dataToReturn')]/text()")
        if script_text_ls:
            script_text = script_text_ls[0]
            reg_ret = re.search(r"var\s+dataToReturn\s*=\s*{.+?};", script_text, re.S)
            if reg_ret:
                js_obj = js2py.eval_js(reg_ret.group(0))
                dct = js_obj['asin_variation_values'].to_dict()
                sku_ls = list(dct)
        if not sku_ls:
            script_text_ls = self.soup.xpath("//script[contains(text(), 'twister-js-init-dpx-data') and contains(text(), 'dataToReturn')]/text()")
            if script_text_ls:
                script_text = script_text_ls[0]
                reg_ret = re.search(r"var\s+dataToReturn\s*=\s*{.+?};", script_text, re.S)
                if reg_ret:
                    js_obj = js2py.eval_js(reg_ret.group(0))
                    dct = js_obj['asinVariationValues'].to_dict()
                    sku_ls = list(dct)

        return sku_ls


    def get_offer_listing_id(self):
        offer_listing_id = ''
        ls = self.soup.xpath("//input[@id='offerListingID']/@value")
        if ls:
            offer_listing_id = ls[0]
        return offer_listing_id


    def get_ue_id(self):
        ue_id = ''
        text = self.soup.xpath("//script[contains(text(),'ue_id')]/text()")[0]
        reg_ret = re.search(r'''\s+ue_id\s*=\s*(?:"|')([^"']+)(?:"|')''', text, re.S)
        if reg_ret:
            ue_id = reg_ret.group(1)
        return ue_id


    def get_session_id(self):
        session_id = ''
        text = self.soup.xpath("//script[contains(text(),'ue_sid')]/text()")[0]
        reg_ret = re.search(r'''\s+ue_sid\s*=\s*(?:"|')([^"']+)(?:"|')''', text, re.S)
        if reg_ret:
            session_id = reg_ret.group(1)
        return session_id
