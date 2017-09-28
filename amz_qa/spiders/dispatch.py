from .amazon_us import AMZUSQAInfo
from .amazon_uk import AMZUKQAInfo
from .amazon_jp import AMZJPQAInfo
from .amazon_fr import AMZFRQAInfo
from .amazon_de import AMZDEQAInfo
from .amazon_es import AMZESQAInfo
from .amazon_it import AMZITQAInfo
from .amazon_ca import AMZCAQAInfo
from .amazon_in import AMZINQAInfo

PLATFORM_MAP = {
    'amazon_us': AMZUSQAInfo,
    'amazon_uk': AMZUKQAInfo,
    'amazon_jp': AMZJPQAInfo,
    'amazon_fr': AMZFRQAInfo,
    'amazon_de': AMZDEQAInfo,
    'amazon_es': AMZESQAInfo,
    'amazon_it': AMZITQAInfo,
    'amazon_ca': AMZCAQAInfo,
    'amazon_in': AMZINQAInfo,
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

def get_url_by_platform(platform, asin, page):
    domain = PLATFORM_DOMAIN_MAP[platform]
    return "https://{domain}/ask/questions/asin/{asin}/{page}?sort=SUBMIT_DATE&isAnswered=true".format(
            domain=domain, asin=asin, page=page)
