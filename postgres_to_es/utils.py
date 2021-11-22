from functools import wraps
from time import sleep

from loguru import logger


def backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10):
    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            t = start_sleep_time
            count = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except Exception:
                    sleep(t)
                    if t < border_sleep_time:
                        t *= factor
                    if t >= border_sleep_time:
                        t = border_sleep_time
                    logger.info(f'Повторное подключение {count}')
                    count += 1
                    continue
                finally:
                    if count == border_sleep_time:
                        logger.info(f'Количество повторных подключений {count}. Попробуйте позже')
                        break
        return inner

    return func_wrapper