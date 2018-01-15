
class AMZCrawlerError(Exception):
    pass

class RequestError(AMZCrawlerError):
    pass

class StatusError(AMZCrawlerError):
    pass

class CaptchaError(AMZCrawlerError):
    pass

class BannedError(AMZCrawlerError):
    def __init__(self, proxy, *args, **kwargs):
        self.proxy = proxy
        super(BannedError, self).__init__(*args, **kwargs)
