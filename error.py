
class AMZCrawlerError(Exception):
    pass

class RequestError(AMZCrawlerError):
    pass

class StatusError(AMZCrawlerError):
    pass

class CaptchaError(AMZCrawlerError):
    pass
