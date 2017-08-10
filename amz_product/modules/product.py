import json
import pipeflow

def process(handle, task_dct):
    info = {}
    info['fba'] = 1 if handle.is_fba() else 0
    info['title'] = handle.get_title()['title']
    info['brand'] = handle.get_brand()['brand']
    tmp_info = handle.get_price()
    info['price'] = tmp_info['price']
    info['discount'] = tmp_info['discount']
    info['img'] = handle.get_img_info()['img']
    tmp_info = handle.get_review()
    info['review'] = tmp_info['review_score']
    info['review_count'] = tmp_info['review_num']
    tmp_info = handle.get_merchants_info()
    info['merchant'] = tmp_info['merchant']
    info['merchant_id'] = tmp_info['merchant_id']
    # extra info
    info['asin'] = task_dct['asin']
    info['platform'] = task_dct['platform']
    info.update(task_dct.get('extra', {}))

    result_task = pipeflow.Task(json.dumps(info).encode('utf-8'))
    result_task.set_to('output_product')
    return result_task
