import math
import json
import pipeflow

curve_func = lambda x,a,b:math.log(a/x+1)/b

def process(handle, task_dct, popt_map):
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
    info['detail_info'] = handle.get_bsr_info()
    # extra info
    info['asin'] = task_dct['asin']
    info['platform'] = task_dct['platform']
    info.update(task_dct.get('extra', {}))
    # calculate sales
    popt_dct = popt_map.get(info['platform'], {})
    cat_name = info['detail_info'].get('cat_1_name', '').strip().lower()
    cat_rank = info['detail_info'].get('cat_1_rank', -1)
    info['detail_info']['cat_1_sales'] = -1
    if cat_name and cat_rank != -1 and popt_dct:
        info['detail_info']['cat_1_sales'] = curve_func(cat_rank, *popt_dct.get(cat_name, popt_dct['default']))

    relation_info = {}
    relation_info['asin'] = task_dct['asin']
    relation_info['platform'] = task_dct['platform']
    tmp_info = handle.get_relative_asin()
    relation_info['bought_together_ls'] = tmp_info['bought_together']
    relation_info['also_bought_ls'] = tmp_info['also_bought']
    relation_info['date'] = task_dct['extra']['date']

    task_ls = []
    task = pipeflow.Task(json.dumps(info).encode('utf-8'))
    task.set_to('output_bsr')
    task_ls.append(task)
    if relation_info['bought_together_ls'] or relation_info['also_bought_ls']:
        task = pipeflow.Task(json.dumps(relation_info).encode('utf-8'))
        task.set_to('output_rlts')
        task_ls.append(task)
    return task_ls
