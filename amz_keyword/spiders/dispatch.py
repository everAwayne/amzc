from urllib import parse
from .amazon_us import AMZUSSearchInfo
from .amazon_uk import AMZUKSearchInfo
from .amazon_jp import AMZJPSearchInfo
from .amazon_fr import AMZFRSearchInfo
from .amazon_de import AMZDESearchInfo
from .amazon_it import AMZITSearchInfo
from .amazon_es import AMZESSearchInfo
from .amazon_ca import AMZCASearchInfo
from .amazon_in import AMZINSearchInfo

PLATFORM_MAP = {
    'amazon_us': AMZUSSearchInfo,
    'amazon_uk': AMZUKSearchInfo,
    'amazon_jp': AMZJPSearchInfo,
    'amazon_fr': AMZFRSearchInfo,
    'amazon_de': AMZDESearchInfo,
    'amazon_it': AMZITSearchInfo,
    'amazon_es': AMZESSearchInfo,
    'amazon_ca': AMZCASearchInfo,
    'amazon_in': AMZINSearchInfo,
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

def get_search_index_url(platform, keyword):
    domain = PLATFORM_DOMAIN_MAP[platform]
    return 'https://{domain}/s/ref=nb_sb_noss?url=search-alias=aps&field-keywords={keyword}'.format(
            domain=domain, keyword=parse.quote(keyword, safe=''))

def formalize_url(platform, url):
    pr = parse.urlparse(url)
    if not pr.scheme or not pr.netloc:
        domain = PLATFORM_DOMAIN_MAP[platform]
        url = parse.urlunparse(parse.ParseResult(scheme=pr.scheme if pr.scheme else 'https',
                                                 netloc=pr.netloc if pr.netloc else domain,
                                                 path=pr.path, params=pr.params, query=pr.query,
                                                 fragment=pr.fragment))
    return url

def get_host(platform):
    return 'https://'+PLATFORM_DOMAIN_MAP[platform]
