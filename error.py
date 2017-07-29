
class AMZCrawlerError(Exception):
    pass

class RequestError(AMZCrawlerError):
    pass

class CaptchaError(AMZCrawlerError):
    pass
