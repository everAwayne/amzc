
class AMZCrawlerError(Exception):
    pass

class RequestError(AMZCrawlerError):
    pass

class StatusError(AMZCrawlerError):
    def __init__(self, code, *args, **kwargs):
        self.code = code
        super(StatusError, self).__init__(*args, **kwargs)

class CaptchaError(AMZCrawlerError):
    pass

class BannedError(AMZCrawlerError):
    def __init__(self, proxy, *args, **kwargs):
        self.proxy = proxy
        super(BannedError, self).__init__(*args, **kwargs)
