import time
from .amazon_us import AMZUSProductInfo
from .amazon_uk import AMZUKProductInfo
from .amazon_jp import AMZJPProductInfo
from .amazon_fr import AMZFRProductInfo
from .amazon_de import AMZDEProductInfo
from .amazon_it import AMZITProductInfo
from .amazon_es import AMZESProductInfo
from .amazon_ca import AMZCAProductInfo
from .amazon_in import AMZINProductInfo

PLATFORM_MAP = {
    'amazon_us': AMZUSProductInfo,
    'amazon_uk': AMZUKProductInfo,
    'amazon_jp': AMZJPProductInfo,
    'amazon_fr': AMZFRProductInfo,
    'amazon_de': AMZDEProductInfo,
    'amazon_it': AMZITProductInfo,
    'amazon_es': AMZESProductInfo,
    'amazon_ca': AMZCAProductInfo,
    'amazon_in': AMZINProductInfo,
}

PLATFORM_DOMAIN_MAP = {
    'amazon_us': 'www.amazon.com',
    'amazon_uk': 'www.amazon.co.uk',
    'amazon_jp': 'www.amazon.co.jp',
    'amazon_fr': 'www.amazon.fr',
    'amazon_de': 'www.amazon.de',
    'amazon_it': 'www.amazon.it',
    'amazon_es': 'www.amazon.es',
    'amazon_ca': 'www.amazon.ca',
    'amazon_in': 'www.amazon.in',
}

def get_spider_by_platform(platform):
    return PLATFORM_MAP[platform]

def get_url_by_platform(platform, asin):
    domain = PLATFORM_DOMAIN_MAP[platform]
    return "https://{domain}/dp/{asin}/ref=sr_1_1?qid={time}&sr=1-1&keywords={asin}".format(
            domain=domain, asin=asin, time=int(time.time()))
