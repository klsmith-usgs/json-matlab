from logger import log


def retry(retries):
    def retry_dec(func):
        def wrapper(*args, **kwargs):
            count = 1

            while True:
                try:
                    return func(*args, **kwargs)
                except:
                    count += 1

                    if count > retries:
                        log.debug('Retry limit exceeded')
                        raise

        return wrapper
    return retry_dec
