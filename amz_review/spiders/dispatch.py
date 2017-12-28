from urllib import parse
from .amazon_us import AMZUSReviewInfo
from .amazon_uk import AMZUKReviewInfo
from .amazon_jp import AMZJPReviewInfo
from .amazon_fr import AMZFRReviewInfo
from .amazon_de import AMZDEReviewInfo
from .amazon_es import AMZESReviewInfo
from .amazon_it import AMZITReviewInfo
from .amazon_ca import AMZCAReviewInfo
from .amazon_in import AMZINReviewInfo
from .amazon_au import AMZAUReviewInfo

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
    'amazon_au': AMZAUReviewInfo,
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
    'amazon_au': 'www.amazon.com.au',
}

def get_spider_by_platform(platform):
    return PLATFORM_MAP[platform]

def formalize_url(platform, url):
    pr = parse.urlparse(url)
    if not pr.scheme or not pr.netloc:
        domain = PLATFORM_DOMAIN_MAP[platform]
        url = parse.urlunparse(parse.ParseResult(scheme=pr.scheme if pr.scheme else 'https',
                                                 netloc=pr.netloc if pr.netloc else domain,
                                                 path=pr.path, params=pr.params, query=pr.query,
                                                 fragment=pr.fragment))
    return url

def get_url_by_platform(platform, asin, path=None):
    domain = PLATFORM_DOMAIN_MAP[platform]
    if path:
        return "https://{domain}{path}?reviewerType=all_reviews&pageSize=50&sortBy=recent".format(
                domain=domain, path=path)
    else:
        return "https://{domain}/product-reviews/{asin}?reviewerType=all_reviews&pageSize=50&sortBy=recent".format(
                domain=domain, asin=asin)
