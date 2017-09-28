from .amazon_us import AMZUSReviewInfo
from .amazon_uk import AMZUKReviewInfo
from .amazon_jp import AMZJPReviewInfo
from .amazon_fr import AMZFRReviewInfo
from .amazon_de import AMZDEReviewInfo
from .amazon_es import AMZESReviewInfo
from .amazon_it import AMZITReviewInfo
from .amazon_ca import AMZCAReviewInfo
from .amazon_in import AMZINReviewInfo

PLATFORM_MAP = {
    'amazon_us': AMZUSReviewInfo,
    'amazon_uk': AMZUKReviewInfo,
    'amazon_jp': AMZJPReviewInfo,
    'amazon_fr': AMZFRReviewInfo,
    'amazon_de': AMZDEReviewInfo,
    'amazon_es': AMZESReviewInfo,
    'amazon_it': AMZITReviewInfo,
    'amazon_ca': AMZCAReviewInfo,
    'amazon_in': AMZINReviewInfo,
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
    return "https://{domain}/product-reviews/{asin}?reviewerType=all_reviews&pageNumber={page}&pageSize=50&sortBy=recent".format(
            domain=domain, asin=asin, page=page)
