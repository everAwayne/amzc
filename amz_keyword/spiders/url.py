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

def get_search_index_url(platform, keyword):
    domain = PLATFORM_DOMAIN_MAP[platform]
    return 'https://{domain}/s/ref=nb_sb_noss?url=search-alias=aps&field-keywords={keyword}'.format(
        domain = domain,
        keyword = keyword)

def get_host(platform):
    return 'https://'+PLATFORM_DOMAIN_MAP[platform]