import json
import time
import pipeflow
from lxml import etree
from error import RequestError, CaptchaError, BannedError
from util.log import logger
from util.prrequest import GetPageSession
from util.task_protocal import TaskProtocal
from .spiders.dispatch import get_spider_by_platform, get_url_by_platform, get_domain_by_platform
from pipeflow import RabbitmqInputEndpoint, RabbitmqOutputEndpoint
from config import RABBITMQ_CONF


MAX_WORKERS = 40
COLLECT_COUPON = "https://{domain:s}/gp/collect-coupon/handler/applicable_promotion_list_hover_count.html?ref_=apl_desktop_imp"
ADD_TO_CART = "https://{domain:s}/gp/add-to-cart/json/ref=dp_start-bbf_1_glance"
ADD_TO_CART_DATA = "clientName=SmartShelf&ASIN={asin:s}&verificationSessionID={session_id:s}&offerListingID={offer_listing_id:s}&quantity={qty:d}"


async def handle_worker(group, task):
    """Handle amz_product task

    [input] task data format:
        JSON:
            {
                "platform": "amazon_us",
                "asin": "B02KDI8NID8",
                "with_qty": True,
            }
    [output] result data format:
        JSON:
            {
                'asin': 'B02KDI8NID8',
                'platform': 'amazon_us',
                'parent_asin': 'B02KDI8NID8',
                'title': 'Active Wow Teeth Whitening Charcoal Powder Natural',
                'brand': 'Active Wow',
                'price': 24.79,
                'discount': 0.83,
                'merchant_id': 'A3RJPJ9XCKYOM5',
                'merchant': 'MarketWeb',
                'description': [],
                'category': [],
                "product_info": {
                    "product_dimensions": "2 x 2 x 2 inches ; 0.6 ounces",
                    "shipping_weight": "3.2 ounces ()",
                    "date_first_available": null
                },
                'detail_info': {
                    'cat_1_rank': 5,
                    'cat_1_name': 'Beauty & Personal Care',
                    "cat_ls": [{"rank": 4, "name_ls": ["Health & Household", "Oral Care", "Teeth Whitening"]}],
                },
                'relative_info': {
                    'bought_together': [],
                    'also_bought': [],
                    'also_viewed': [],
                    'viewed_also_bought': [],
                    'sponsored_1': [],
                    'sponsored_2': [],
                    'compare_to_similar': [],
                },
                'sku_info': [],
                'fba': 1,
                'review': 4.6,
                'review_count': 9812,
                "review_statistics": {
                    "1": 6,
                    "2": 2,
                    "3": 3,
                    "4": 9,
                    "5": 80
                },
                'img': 'https://images-na.ssl-images-amazon.com/images/I/514RSPIJMKL.jpg',
                'imgs': [],
                'qty': 123, #None
            }
    """
    tp = TaskProtocal(task)
    from_end = tp.get_from()
    task_dct = tp.get_data()
    logger.info("%s %s %s" % (task_dct['platform'], task_dct['asin'], task_dct.get('with_qty', False)))

    handle_cls = get_spider_by_platform(task_dct['platform'])
    url = get_url_by_platform(task_dct['platform'], task_dct['asin'])
    qty_info = {}
    try:
        if not task_dct.get('with_qty'):
            sess = GetPageSession()
            html = await sess.get_page('get', url, timeout=60, captcha_bypass=True)
            soup = etree.HTML(html, parser=etree.HTMLParser(encoding='utf-8'))
            handle = handle_cls(soup)
        else:
            sess = GetPageSession()
            html = await sess.get_page('get', url, timeout=60, captcha_bypass=True)
            soup = etree.HTML(html, parser=etree.HTMLParser(encoding='utf-8'))
            handle = handle_cls(soup)
            offer_listing_id = handle.get_offer_listing_id()
            ue_id = handle.get_ue_id()
            session_id = handle.get_session_id()
            domain = get_domain_by_platform(task_dct['platform'])
            if offer_listing_id and ue_id and session_id:
                ### get ubid-main cookie
                collect_coupon_url = COLLECT_COUPON.format(domain=domain)
                data = {"pageReImpType": "aplImpressionPC"}
                headers = {'Referer': url, "X-Requested-With": "XMLHttpRequest", "Content-Type": "application/x-www-form-urlencoded"}
                cookies = {'csm-hit': 's-{ue_id:s}|{time:d}'.format(ue_id=ue_id, time=int(time.time()*1000))}
                await sess.get_page('post', collect_coupon_url, data=data, headers=headers, cookies=cookies, timeout=30)
                ### get qty
                add_to_cart_url = ADD_TO_CART.format(domain=domain)
                data = ADD_TO_CART_DATA.format(asin=task_dct['asin'], session_id=session_id, offer_listing_id=offer_listing_id, qty=999)
                headers = {'Referer': url, "X-Requested-With": "XMLHttpRequest", "Content-Type": "application/x-www-form-urlencoded"}
                cookies = {'csm-hit': '{ue_id:s}+s-{ue_id:s}|{time:d}'.format(ue_id=ue_id, time=int(time.time()*1000))}
                ret = await sess.get_page('post', add_to_cart_url, data=data, headers=headers, cookies=cookies, timeout=30)
                qty_info = json.loads(ret.decode('utf-8'))
    except BannedError as exc:
        tp.set_to('input_back')
        ban_tp = tp.new_task({'proxy': exc.proxy[7:]})
        ban_tp.set_to('ban')
        return [ban_tp, tp]
    except RequestError:
        tp.set_to('input_back')
        return tp
    except CaptchaError:
        tp.set_to('input_back')
        return tp
    except Exception as exc:
        exc_info = (type(exc), exc, exc.__traceback__)
        taks_info = ' '.join([task_dct['platform'], task_dct['asin']])
        logger.error('Get page handle error\n'+taks_info, exc_info=exc_info)
        exc.__traceback__ = None
        return

    is_product_page = handle.is_product_page()
    if not is_product_page:
        return

    try:
        info = handle.get_info()
        info['qty'] = int(qty_info['cartQuantity']) if qty_info.get('cartQuantity') else None
        # extra info
        info['asin'] = task_dct['asin']
        info['platform'] = task_dct['platform']
        new_tp = tp.new_task(info)
        new_tp.set_to('output')
        return new_tp
    except Exception as exc:
        exc_info = (type(exc), exc, exc.__traceback__)
        taks_info = ' '.join([task_dct['platform'], task_dct['asin']])
        logger.error('Get page info error\n'+taks_info, exc_info=exc_info)
        exc.__traceback__ = None
        return


def run():
    input_end = RabbitmqInputEndpoint('amz_product:input', **RABBITMQ_CONF)
    output_end = RabbitmqOutputEndpoint(['amz_product:input', 'amz_product:output',
                                         'amz_ip_ban:input'], **RABBITMQ_CONF)

    server = pipeflow.Server()
    group = server.add_group('main', MAX_WORKERS)
    group.set_handle(handle_worker)
    group.add_input_endpoint('input', input_end)
    group.add_output_endpoint('input_back', output_end, 'amz_product:input')
    group.add_output_endpoint('output', output_end, 'amz_product:output')
    group.add_output_endpoint('ban', output_end, 'amz_ip_ban:input')
    server.run()
