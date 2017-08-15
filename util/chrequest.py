import os
import subprocess
import asyncio
import aiohttp
import concurrent.futures
from error import RequestError
from .headers import get_header
from .log import logger


# Two events to sync handle workers and change_ip worker
# handle worker wait for finish_change_event and set start_change_event
# change_ip worker wait for start_change_event and set finish_change_event
start_change_event = asyncio.Event()
finish_change_event = asyncio.Event()
start_change_event.clear()
finish_change_event.clear()
event_wait = 0
in_request = 0
change_ip_cnt = 0


async def get_page_handle(handle_cls, url, timeout=60):
    """Get the page of url, and create a handle, then return it.

    If the page is a captcha page, change ip and request again.
    """
    global event_wait
    global in_request

    try:
        in_request += 1
        # if ther is any handle worker is waiting for changing ip,
        # or have received a captcha page,
        # then stop and wait for changing ip
        # trigger to change ip when all running handle worker have no IO operation
        if event_wait > 0:
            if event_wait+1 == in_request:
                start_change_event.set()
                start_change_event.clear()
            event_wait += 1
            await finish_change_event.wait()

        while True:
            headers = get_header()
            html = None
            try:
                async with aiohttp.ClientSession(headers=headers) as session:
                    async with session.get(url, timeout=timeout) as resp:
                        html = await resp.read()
                        if resp.status != 200:
                            logger.error('[%d] %s' % (resp.status, url))
            #except Exception as exc:
            except:
                #exc_info = (type(exc), exc, exc.__traceback__)
                #logger.error('Request page fail', exc_info=exc_info)
                logger.error('Request page fail')
                #exc.__traceback__ = None
                raise RequestError

            handle = handle_cls(html)
            is_captcha_page = handle.is_captcha_page()
            if is_captcha_page:
                if event_wait+1 == in_request:
                    start_change_event.set()
                    start_change_event.clear()
                event_wait += 1
                await finish_change_event.wait()
            else:
                return handle
    finally:
        in_request -= 1
        if event_wait > 0 and event_wait == in_request:
            start_change_event.set()
            start_change_event.clear()


def execute(command):
    ret = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return ret


def found_ppp0():
    ls = os.listdir('/sys/class/net')
    if 'ppp0' in ls:
        return True
    return False


async def change_ip(server):
    """Change machine IP(blocking)

    Wait for start_change_event.
    When all running handle workers are wait for finish_change_event,
    start_change_event will be set.
    """
    global event_wait
    global in_request
    global change_ip_cnt
    loop = server.get_event_loop()
    executor = concurrent.futures.ProcessPoolExecutor(1)
    # initial process pool, make it to fork at the beginning
    future = loop.run_in_executor(executor, execute, 'hostname')
    await future
    while True:
        change_ip_cnt += 1
        await start_change_event.wait()
        assert event_wait, "event_wait should more than 0"
        assert event_wait==in_request, "event_wait should be equal to running_cnt"
        logger.info("[%d]change ip start" % change_ip_cnt)
        while True:
            future = loop.run_in_executor(executor, execute, 'ifdown ppp0')
            ret = await future
            if ret.returncode == 0:
                break
            else:
                if not found_ppp0():
                    break
                logger.error('[%d]ppp0 stop error: [%d]' % (change_ip_cnt, ret.returncode))
                if ret.stdout:
                    logger.error('%s' % ret.stdout)
                if ret.stderr:
                    logger.error('%s' % ret.stderr)
                await asyncio.sleep(10)
        while True:
            future = loop.run_in_executor(executor, execute, 'ifup ppp0')
            ret = await future
            if ret.returncode == 0:
                break
            else:
                if found_ppp0():
                    break
                logger.error('[%d]ppp0 start error: [%d]' % (change_ip_cnt, ret.returncode))
                if ret.stdout:
                    logger.error('%s' % ret.stdout)
                if ret.stderr:
                    logger.error('%s' % ret.stderr)
                await asyncio.sleep(10)
        logger.info("[%d]change ip end" % change_ip_cnt)
        finish_change_event.set()
        finish_change_event.clear()
        event_wait = 0
