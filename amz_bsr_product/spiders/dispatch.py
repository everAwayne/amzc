from .amazon_us import AMZUSBSRCategoryInfo
#from .amazon_uk import AMZUKBSRCategoryInfo
#from .amazon_jp import AMZJPBSRCategoryInfo
#from .amazon_fr import AMZFRBSRCategoryInfo
#from .amazon_de import AMZDEBSRCategoryInfo
#from .amazon_it import AMZITBSRCategoryInfo
#from .amazon_es import AMZESBSRCategoryInfo
#from .amazon_ca import AMZCABSRCategoryInfo
#from .amazon_in import AMZINBSRCategoryInfo

PLATFORM_MAP = {
    'amazon_us': AMZUSBSRCategoryInfo,
    #'amazon_uk': AMZUKBSRCategoryInfo,
    #'amazon_jp': AMZJPBSRCategoryInfo,
    #'amazon_fr': AMZFRBSRCategoryInfo,
    #'amazon_de': AMZDEBSRCategoryInfo,
    #'amazon_it': AMZITBSRCategoryInfo,
    #'amazon_es': AMZESBSRCategoryInfo,
    #'amazon_ca': AMZCABSRCategoryInfo,
    #'amazon_in': AMZINBSRCategoryInfo,
}

def get_spider_by_platform(platform):
    return PLATFORM_MAP[platform]
