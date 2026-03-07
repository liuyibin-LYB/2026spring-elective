"""
@Author : xiaoce2025
@File   : loop.py
@Date   : 2025-08-30
"""

import os
import time
import random
from queue import Queue
from collections import deque
from itertools import combinations
from requests.compat import json
from requests.exceptions import RequestException
import numpy as np
from . import __version__, __date__
from .environ import Environ
from .config import AutoElectiveConfig
from .logger import ConsoleLogger, FileLogger
from .course import Course
from .captcha import TTShituRecognizer, Captcha
from .parser import get_tables, get_courses, get_courses_with_detail, get_elected_with_detail, get_sida
from .hook import _dump_request
from .iaaa import IAAAClient
from .elective import ElectiveClient
from .const import (
    CAPTCHA_CACHE_DIR,
    USER_AGENT_LIST,
    WEB_LOG_DIR,
    WECHAT_MSG,
    WECHAT_PREFIX,
)
from .exceptions import *
from ._internal import mkdir
from .notification.bark_push import Notify

environ = Environ()
config = AutoElectiveConfig()
cout = ConsoleLogger("loop")
ferr = FileLogger("loop.error")  # loop 的子日志，同步输出到 console

username = config.iaaa_id
password = config.iaaa_password
is_dual_degree = config.is_dual_degree
identity = config.identity
refresh_interval = config.refresh_interval
refresh_random_deviation = config.refresh_random_deviation
supply_cancel_pages = config.supply_cancel_pages
iaaa_client_timeout = config.iaaa_client_timeout
elective_client_timeout = config.elective_client_timeout
login_loop_interval = config.login_loop_interval
elective_client_pool_size = config.elective_client_pool_size
elective_client_max_life = config.elective_client_max_life
is_print_mutex_rules = config.is_print_mutex_rules
notify = Notify(
    _disable_push=config.disable_push,
    _token=config.wechat_token,
    _interval_lock=config.minimum_interval,
    _verbosity=config.verbosity,
)

config.check_identify(identity)
config.check_supply_cancel_page(supply_cancel_pages)

_USER_WEB_LOG_DIR = os.path.join(WEB_LOG_DIR, config.get_user_subpath())
mkdir(_USER_WEB_LOG_DIR)

# recognizer = CaptchaRecognizer()
recognizer = TTShituRecognizer()
RECOGNIZER_MAX_ATTEMPT = 15

electivePool = Queue(maxsize=elective_client_pool_size)
reloginPool = Queue(maxsize=elective_client_pool_size)

goals = environ.goals  # let N = len(goals);
ignored = environ.ignored
mutexes = np.zeros(0, dtype=np.uint8)  # uint8 [N][N];
delays = np.zeros(0, dtype=np.int32)  # int [N];
swaps = []  # list of (cix1, cix2, ...) tuples, each is a swap group in priority order

killedElective = ElectiveClient(-1)
NO_DELAY = -1

notify.send_bark_push(msg=WECHAT_MSG["s"], prefix=WECHAT_PREFIX[3])


# 刷新系统配置
def refreshsettings():
    global username, password, is_dual_degree, identity, refresh_interval
    global refresh_random_deviation, supply_cancel_pages, iaaa_client_timeout
    global elective_client_timeout, login_loop_interval, elective_client_pool_size
    global elective_client_max_life, is_print_mutex_rules, notify
    global electivePool, reloginPool, goals, ignored, mutexes, delays, swaps
    global recognizer

    username = config.iaaa_id
    password = config.iaaa_password
    is_dual_degree = config.is_dual_degree
    identity = config.identity
    refresh_interval = config.refresh_interval
    refresh_random_deviation = config.refresh_random_deviation
    supply_cancel_pages = config.supply_cancel_pages
    iaaa_client_timeout = config.iaaa_client_timeout
    elective_client_timeout = config.elective_client_timeout
    login_loop_interval = config.login_loop_interval
    elective_client_pool_size = config.elective_client_pool_size
    elective_client_max_life = config.elective_client_max_life
    is_print_mutex_rules = config.is_print_mutex_rules
    notify = Notify(
        _disable_push=config.disable_push,
        _token=config.wechat_token,
        _interval_lock=config.minimum_interval,
        _verbosity=config.verbosity,
    )

    recognizer = TTShituRecognizer()

    electivePool = Queue(maxsize=elective_client_pool_size)
    reloginPool = Queue(maxsize=elective_client_pool_size)

    goals = environ.goals  # let N = len(goals);
    ignored = environ.ignored
    mutexes = np.zeros(0, dtype=np.uint8)  # uint8 [N][N];
    delays = np.zeros(0, dtype=np.int32)  # int [N];
    swaps = []  # list of swap groups
    return




class _ElectiveNeedsLogin(Exception):
    pass


class _ElectiveExpired(Exception):
    pass


def _get_refresh_interval():
    if refresh_random_deviation <= 0:
        return refresh_interval
    delta = (random.random() * 2 - 1) * refresh_random_deviation * refresh_interval
    return refresh_interval + delta


def _ignore_course(course, reason):
    ignored[course.to_simplified()] = reason


def _add_error(e):
    clz = e.__class__
    name = clz.__name__
    key = "[%s] %s" % (e.code, name) if hasattr(clz, "code") else name
    environ.errors[key] += 1


def _format_timestamp(timestamp):
    if timestamp == -1:
        return str(timestamp)
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp))


def _dump_respose_content(content, filename):
    path = os.path.join(_USER_WEB_LOG_DIR, filename)
    with open(path, "wb") as fp:
        fp.write(content)


def run_iaaa_loop():
    # 刷新配置（不在此处不刷新，在启动时统一刷新）
    # refreshdata()

    elective = None

    while True:
        if elective is None:
            elective = reloginPool.get()
            if elective is killedElective:
                cout.info("Quit IAAA loop")
                return

        environ.iaaa_loop += 1
        user_agent = random.choice(USER_AGENT_LIST)

        cout.info("Try to login IAAA (client: %s)" % elective.id)
        cout.info("User-Agent: %s" % user_agent)

        try:
            iaaa = IAAAClient(timeout=iaaa_client_timeout)  # not reusable
            iaaa.set_user_agent(user_agent)

            # request elective's home page to get cookies
            r = iaaa.oauth_home()

            r = iaaa.oauth_login(username, password)

            try:
                token = r.json()["token"]
            except Exception as e:
                ferr.error(e)
                raise OperationFailedError(
                    msg="Unable to parse IAAA token. response body: %s" % r.content
                )

            elective.clear_cookies()
            elective.set_user_agent(user_agent)

            r = elective.sso_login(token)

            if is_dual_degree:
                sida = get_sida(r)
                sttp = identity
                referer = r.url
                r = elective.sso_login_dual_degree(sida, sttp, referer)

            if elective_client_max_life == -1:
                elective.set_expired_time(-1)
            else:
                elective.set_expired_time(int(time.time()) + elective_client_max_life)
            cout.info(
                "Login success (client: %s, expired_time: %s)"
                % (elective.id, _format_timestamp(elective.expired_time))
            )
            cout.info("")

            electivePool.put_nowait(elective)
            elective = None

        except (ServerError, StatusCodeError) as e:
            ferr.error(e)
            cout.warning("ServerError/StatusCodeError encountered")
            _add_error(e)

        except OperationFailedError as e:
            ferr.error(e)
            cout.warning("OperationFailedError encountered")
            _add_error(e)

        except RequestException as e:
            ferr.error(e)
            cout.warning("RequestException encountered")
            _add_error(e)

        except IAAAIncorrectPasswordError as e:
            cout.error(e)
            _add_error(e)
            raise e

        except IAAAForbiddenError as e:
            ferr.error(e)
            _add_error(e)
            raise e

        except IAAAException as e:
            ferr.error(e)
            cout.warning("IAAAException encountered")
            _add_error(e)

        except CaughtCheatingError as e:
            ferr.critical(e)  # 严重错误
            _add_error(e)
            raise e

        except ElectiveException as e:
            ferr.error(e)
            cout.warning("ElectiveException encountered")
            _add_error(e)

        except json.JSONDecodeError as e:
            ferr.error(e)
            cout.warning("JSONDecodeError encountered")
            _add_error(e)

        except KeyboardInterrupt as e:
            raise e

        except Exception as e:
            ferr.exception(e)
            _add_error(e)
            raise e

        finally:
            t = login_loop_interval
            cout.info("")
            cout.info("IAAA login loop sleep %s s" % t)
            cout.info("")
            time.sleep(t)


def run_elective_loop():
    # 刷新配置（不在此处不刷新，在启动时统一刷新）
    # refreshdata()

    elective = None
    noWait = False

    ## load courses

    cs = config.courses  # OrderedDict
    N = len(cs)
    cid_cix = {}  # { cid: cix }

    for ix, (cid, c) in enumerate(cs.items()):
        goals.append(c)
        cid_cix[cid] = ix

    ## load mutex

    ms = config.mutexes
    mutexes.resize((N, N), refcheck=False)

    for mid, m in ms.items():
        ixs = []
        for cid in m.cids:
            if cid not in cs:
                raise UserInputException(
                    "In 'mutex:%s', course %r is not defined" % (mid, cid)
                )
            ix = cid_cix[cid]
            ixs.append(ix)
        for ix1, ix2 in combinations(ixs, 2):
            mutexes[ix1, ix2] = mutexes[ix2, ix1] = 1

    ## load delay

    ds = config.delays
    delays.resize(N, refcheck=False)
    delays.fill(NO_DELAY)

    for did, d in ds.items():
        cid = d.cid
        if cid not in cs:
            raise UserInputException(
                "In 'delay:%s', course %r is not defined" % (did, cid)
            )
        ix = cid_cix[cid]
        delays[ix] = d.threshold

    ## load swap rules

    ss = config.swaps
    for sid, s in ss.items():
        swap_ixs = []
        for cid in s.cids:
            if cid not in cs:
                raise UserInputException(
                    "In 'swap:%s', course %r is not defined" % (sid, cid)
                )
            ix = cid_cix[cid]
            swap_ixs.append(ix)
        swaps.append(tuple(swap_ixs))
        # swap 组内的课程也自动加入 mutex 关系
        for ix1, ix2 in combinations(swap_ixs, 2):
            mutexes[ix1, ix2] = mutexes[ix2, ix1] = 1

    ## setup elective pool

    for ix in range(1, elective_client_pool_size + 1):
        client = ElectiveClient(id=ix, timeout=elective_client_timeout)
        client.set_user_agent(random.choice(USER_AGENT_LIST))
        electivePool.put_nowait(client)

    cout.info("欢迎使用严小希选课小助手！")
    cout.info("让时光的帷幕，牵动往昔的涟漪，自此汇入晨光！")
    cout.info("")

    line = "-" * 30

    cout.info("> User Agent")
    cout.info(line)
    cout.info("pool_size: %d" % len(USER_AGENT_LIST))
    cout.info(line)
    cout.info("")
    cout.info("> Config")
    cout.info(line)
    cout.info("is_dual_degree: %s" % is_dual_degree)
    cout.info("identity: %s" % identity)
    cout.info("refresh_interval: %s" % refresh_interval)
    cout.info("refresh_random_deviation: %s" % refresh_random_deviation)
    cout.info("supply_cancel_pages: %s" % supply_cancel_pages)
    cout.info("iaaa_client_timeout: %s" % iaaa_client_timeout)
    cout.info("elective_client_timeout: %s" % elective_client_timeout)
    cout.info("login_loop_interval: %s" % login_loop_interval)
    cout.info("elective_client_pool_size: %s" % elective_client_pool_size)
    cout.info("elective_client_max_life: %s" % elective_client_max_life)
    cout.info("is_print_mutex_rules: %s" % is_print_mutex_rules)
    cout.info(line)
    cout.info("")

    while True:
        noWait = False
        swap_rush_retry = False
        swap_pending_targets = set()  # 追踪已退课但尚未补选成功的 swap 目标，放在 try 外确保异常时也能检查

        if elective is None:
            elective = electivePool.get()

        environ.elective_loop += 1

        cout.info("")
        cout.info("======== Loop %d ========" % environ.elective_loop)
        cout.info("")

        ## print current plans

        current = [c for c in goals if c not in ignored]
        if len(current) > 0:
            cout.info("> Current tasks")
            cout.info(line)
            for ix, course in enumerate(current):
                cout.info("%02d. %s" % (ix + 1, course))
            cout.info(line)
            cout.info("")

        ## print ignored course

        if len(ignored) > 0:
            cout.info("> Ignored tasks")
            cout.info(line)
            for ix, (course, reason) in enumerate(ignored.items()):
                cout.info("%02d. %s  %s" % (ix + 1, course, reason))
            cout.info(line)
            cout.info("")

        ## print mutex rules

        if np.any(mutexes):
            cout.info("> Mutex rules")
            cout.info(line)
            ixs = [(ix1, ix2) for ix1, ix2 in np.argwhere(mutexes == 1) if ix1 < ix2]
            if is_print_mutex_rules:
                for ix, (ix1, ix2) in enumerate(ixs):
                    cout.info("%02d. %s --x-- %s" % (ix + 1, goals[ix1], goals[ix2]))
            else:
                cout.info("%d mutex rules" % len(ixs))
            cout.info(line)
            cout.info("")

        ## print delay rules

        if np.any(delays != NO_DELAY):
            cout.info("> Delay rules")
            cout.info(line)
            ds = [
                (cix, threshold)
                for cix, threshold in enumerate(delays)
                if threshold != NO_DELAY
            ]
            for ix, (cix, threshold) in enumerate(ds):
                cout.info("%02d. %s --- %d" % (ix + 1, goals[cix], threshold))
            cout.info(line)
            cout.info("")

        ## print swap rules

        if len(swaps) > 0:
            cout.info("> Swap rules (auto drop+reselect)")
            cout.info(line)
            for ix, swap_group in enumerate(swaps):
                names = [str(goals[cix]) for cix in swap_group]
                cout.info("%02d. %s" % (ix + 1, " -> ".join(names)))
            cout.info(line)
            cout.info("")

        if len(current) == 0:
            cout.info("No tasks")
            cout.info("Quit elective loop")
            reloginPool.put_nowait(killedElective)  # kill signal
            return

        ## print client info

        cout.info(
            "> Current client: %s (qsize: %s)" % (elective.id, electivePool.qsize() + 1)
        )
        cout.info(
            "> Client expired time: %s" % _format_timestamp(elective.expired_time)
        )
        cout.info("User-Agent: %s" % elective.user_agent)
        cout.info("")

        try:
            if not elective.has_logined:
                raise _ElectiveNeedsLogin  # quit this loop

            if elective.is_expired:
                try:
                    cout.info("Logout")
                    r = elective.logout()
                except Exception as e:
                    cout.warning("Logout error")
                    cout.exception(e)
                raise _ElectiveExpired  # quit this loop

            ## check supply/cancel page(s) - 支持多页面

            page_r = None
            all_plans = []   # 合并所有页面的可选课程列表
            elected = []     # 已选课程列表（各页面相同）

            for _page in supply_cancel_pages:
                if _page == 1:
                    cout.info("Get SupplyCancel page %s" % _page)

                    r = elective.get_SupplyCancel(username)
                    if page_r is None:
                        page_r = r

                    # --- 诊断：保存原始 HTML 用于分析页面结构 ---
                    if config.is_debug_dump_request:
                        dump_fn = "SupplyCancel_page%d_%d.html" % (_page, int(time.time() * 1000))
                        _dump_respose_content(r.content, dump_fn)
                        cout.info("[DEBUG] Page HTML dumped to %s" % dump_fn)

                    tables = get_tables(r._tree)
                    try:
                        elected = get_courses(tables[1])
                        page_plans = get_courses_with_detail(tables[0])
                        all_plans.extend(page_plans)
                    except IndexError as e:
                        filename = "elective.get_SupplyCancel_%d.html" % int(
                            time.time() * 1000
                        )
                        _dump_respose_content(r.content, filename)
                        cout.info("Page dump to %s" % filename)
                        raise UnexceptedHTMLFormat

                else:
                    #
                    # 刷新非第一页的课程，第一次请求会遇到返回空页面的情况
                    #
                    # 引入 retry 逻辑以防止因为某些特殊原因无限重试
                    # 正常情况下一次就能成功，但是为了应对某些偶发错误，这里设为最多尝试 3 次
                    #
                    retry = 3
                    while True:
                        if retry == 0:
                            raise OperationFailedError(
                                msg="unable to get normal Supplement page %s"
                                % _page
                            )

                        cout.info("Get Supplement page %s" % _page)
                        r = elective.get_supplement(
                            username, page=_page
                        )
                        if page_r is None:
                            page_r = r

                        # --- 诊断：保存原始 HTML 用于分析页面结构 ---
                        if config.is_debug_dump_request:
                            dump_fn = "SupplyCancel_page%d_%d.html" % (_page, int(time.time() * 1000))
                            _dump_respose_content(r.content, dump_fn)
                            cout.info("[DEBUG] Page HTML dumped to %s" % dump_fn)

                        tables = get_tables(r._tree)
                        try:
                            elected = get_courses(tables[1])
                            page_plans = get_courses_with_detail(tables[0])
                            all_plans.extend(page_plans)
                        except IndexError as e:
                            cout.warning("IndexError encountered on page %s" % _page)
                            cout.info(
                                "Get SupplyCancel first to prevent empty table returned"
                            )
                            _ = elective.get_SupplyCancel(
                                username
                            )  # 遇到空页面时请求一次补退选主页，之后就可以不断刷新
                        else:
                            break
                        finally:
                            retry -= 1

            plans = all_plans  # 合并后的所有页面的可选课程

            ## parse elected courses with cancel links (for swap feature)

            elected_with_cancel = []  # 已选课程带退选链接
            try:
                # 使用最后一次请求的页面来解析已选列表（所有页面的已选列表相同）
                last_tables = get_tables(r._tree)
                if len(last_tables) > 1:
                    if config.is_debug_dump_request:
                        from .parser import get_table_header
                        elected_header = get_table_header(last_tables[1])
                        cout.info("[DEBUG] Elected table headers: %s" % elected_header)
                    elected_with_cancel = get_elected_with_detail(last_tables[1])
                    if config.is_debug_dump_request and len(elected_with_cancel) > 0:
                        cout.info("[DEBUG] Parsed %d elected courses with cancel info:" % len(elected_with_cancel))
                        for ec in elected_with_cancel:
                            cout.info("[DEBUG]   %s  cancel_href=%s" % (ec, ec.href))
            except Exception as e:
                cout.warning("Failed to parse elected courses with cancel links: %s" % e)

            # 构建已选课程到退选链接的映射
            elected_cancel_map = {}  # { Course(simplified): cancel_href }
            for ec in elected_with_cancel:
                if ec.href:
                    elected_cancel_map[ec.to_simplified()] = ec.href

            ## check available courses

            cout.info("Get available courses")

            tasks = []  # [(ix, course)]
            swap_tasks = []  # [(target_ix, target_course, drop_ix, drop_course, cancel_href)]

            for ix, c in enumerate(goals):
                if c in ignored:
                    continue
                elif c in elected:
                    # 检查是否有 swap 规则：只有当同组存在更高优先级的未选课程时，才保持为 swap 候选
                    # swap 中课程按配置的优先级排列（位置越靠前优先级越高）
                    is_swap_target = False
                    for swap_group in swaps:
                        if ix in swap_group:
                            my_pos = swap_group.index(ix)
                            # 只有存在更高优先级的"未选上且未忽略"的课程时，才标记为 swap 候选
                            for higher_pos in range(0, my_pos):
                                higher_ix = swap_group[higher_pos]
                                higher_c = goals[higher_ix]
                                if higher_c not in elected and higher_c not in ignored:
                                    is_swap_target = True
                                    break
                            break

                    if not is_swap_target:
                        cout.info("%s is elected, ignored" % c)
                        _ignore_course(c, "Elected")
                        # is_swap_target=False 意味着该课程已是最高可达优先级
                        # 所有 mutex 伙伴（含同 swap 组低优先级课程）都应忽略
                        for (mix,) in np.argwhere(mutexes[ix, :] == 1):
                            mc = goals[mix]
                            if mc in ignored:
                                continue
                            cout.info("%s is simultaneously ignored by mutex rules" % mc)
                            _ignore_course(mc, "Mutex rules")
                    else:
                        cout.info("%s is elected (swap candidate, not ignored)" % c)
                        # swap 候选课程本身不忽略，但其不在同一 swap 组的 mutex 伙伴仍应被忽略
                        for (mix,) in np.argwhere(mutexes[ix, :] == 1):
                            mc = goals[mix]
                            if mc in ignored:
                                continue
                            in_same_swap = False
                            for swap_group in swaps:
                                if mix in swap_group and ix in swap_group:
                                    in_same_swap = True
                                    break
                            if not in_same_swap:
                                cout.info("%s is simultaneously ignored by mutex rules" % mc)
                                _ignore_course(mc, "Mutex rules")
                else:
                    for c0 in plans:  # c0 has detail
                        if c0 == c:
                            if c0.is_available():
                                delay = delays[ix]
                                if delay != NO_DELAY and c0.remaining_quota > delay:
                                    cout.info(
                                        "%s hasn't reached the delay threshold %d, skip"
                                        % (c0, delay)
                                    )
                                else:
                                    # 检查是否是 swap 场景：需要先退掉同组已选课程
                                    swap_found = False
                                    for swap_group in swaps:
                                        if ix in swap_group:
                                            # 找 swap 组中比 ix 优先级低且已选上的课程
                                            my_pos = swap_group.index(ix)
                                            for other_pos in range(my_pos + 1, len(swap_group)):
                                                other_ix = swap_group[other_pos]
                                                other_c = goals[other_ix]
                                                if other_c in elected:
                                                    # 找到需要退掉的课程
                                                    cancel_href = elected_cancel_map.get(other_c.to_simplified())
                                                    if cancel_href:
                                                        swap_tasks.append((ix, c0, other_ix, other_c, cancel_href))
                                                        cout.info("%s is AVAILABLE ! Will swap: drop %s -> elect %s" % (c0, other_c, c0))
                                                        swap_found = True
                                                    else:
                                                        cout.warning("Cannot find cancel link for %s, skip swap" % other_c)
                                                    break
                                            break
                                    if not swap_found:
                                        tasks.append((ix, c0))
                                        cout.info("%s is AVAILABLE now !" % c0)
                            break
                    else:
                        # 课程不在当前页面的计划中，可能在其他页面
                        # 如果启用了多页但课程不在任何页面中，才报错
                        cout.warning(
                            "%s is not found in current supply/cancel pages, "
                            "please check your config and supply_cancel_page setting."
                            % c
                        )

            # 对同一门被退课程，只保留最高优先级目标的 swap task
            # swap_tasks 按 goals 顺序生成（ix 越小优先级越高），所以先出现的优先级更高
            deduplicated_swap_tasks = []
            seen_drop_ixs = set()
            for task in swap_tasks:
                target_ix, target_course, drop_ix, drop_course, cancel_href = task
                if drop_ix not in seen_drop_ixs:
                    seen_drop_ixs.add(drop_ix)
                    deduplicated_swap_tasks.append(task)
                else:
                    cout.info("Skip redundant swap task: drop %s -> elect %s (already planned for higher priority)" % (drop_course, target_course))
            swap_tasks = deduplicated_swap_tasks

            tasks = deque(
                [(ix, c) for ix, c in tasks if c not in ignored]
            )  # filter again and change to deque

            swap_elect_queue = []  # 收集成功退课后待补选的目标，保持优先级顺序

            ## execute swap tasks (drop + reselect)

            for target_ix, target_course, drop_ix, drop_course, cancel_href in swap_tasks:
                if target_course in ignored:
                    continue

                cout.info("=== Swap: dropping %s to elect %s ===" % (drop_course, target_course))

                ## validate captcha before drop (退选也需要验证码)

                captcha_fail_count = 0
                max_captcha_fails = 5

                while True:
                    cout.info("Fetch a captcha (for drop)")
                    r = elective.get_DrawServlet()

                    captcha = recognizer.recognize(r.content)
                    cout.info("Recognition result: %s" % captcha.code)

                    r = elective.get_Validate(username, captcha.code)
                    try:
                        res = r.json()["valid"]
                    except Exception as e:
                        ferr.error(e)
                        raise OperationFailedError(msg="Unable to validate captcha")

                    if res == "2":
                        cout.info("Validation passed")
                        break
                    elif res == "0":
                        captcha_fail_count += 1
                        cout.info("Validation failed (attempt %d/%d)" % (captcha_fail_count, max_captcha_fails))
                        if captcha_fail_count >= max_captcha_fails:
                            cout.warning("Captcha validation failed %d times, skipping swap" % max_captcha_fails)
                            break
                        else:
                            cout.info("Try again")
                    else:
                        cout.warning("Unknown validation result: %s" % res)

                if captcha_fail_count >= max_captcha_fails:
                    cout.info("Skipping swap due to captcha validation failures")
                    continue

                ## try to drop the course

                try:
                    r = elective.get_CancelCourse(cancel_href)
                    # 如果没有抛出异常，说明退选可能成功（页面正常返回）
                    # 检查返回页面中该课程是否还在已选列表
                    cout.info("%s drop request completed (no exception)" % drop_course)
                    drop_verified = False
                    try:
                        cancel_tables = get_tables(r._tree)
                        if len(cancel_tables) > 1:
                            still_elected = get_courses(cancel_tables[1])
                            if drop_course.to_simplified() not in [c.to_simplified() for c in still_elected]:
                                cout.info("%s is DROPPED successfully !" % drop_course)
                                notify.send_bark_push(
                                    msg="退选成功：" + str(drop_course), prefix=WECHAT_PREFIX[1]
                                )
                                drop_verified = True
                            else:
                                cout.warning("%s is still in elected list, drop FAILED" % drop_course)
                        else:
                            cout.warning("Cannot verify drop result (no elected table), will still try to elect")
                            drop_verified = True  # 无法验证时假设成功，尝试补选（失败无害）
                    except Exception as e:
                        cout.warning("Error verifying drop result: %s, will still try to elect" % e)
                        drop_verified = True  # 解析失败时假设成功，尝试补选（失败无害）

                    if drop_verified:
                        swap_elect_queue.append((target_ix, target_course))
                        swap_pending_targets.add(target_ix)
                        cout.info("Now will try to elect %s" % target_course)

                except CancelSuccess as e:
                    cout.info("%s is DROPPED successfully !" % drop_course)
                    notify.send_bark_push(
                        msg="退选成功：" + str(drop_course), prefix=WECHAT_PREFIX[1]
                    )
                    # 退选成功后，将目标课程加入选课任务
                    swap_elect_queue.append((target_ix, target_course))
                    swap_pending_targets.add(target_ix)
                    cout.info("Now will try to elect %s" % target_course)

                except CancelFailedError as e:
                    ferr.error(e)
                    cout.warning("CancelFailedError encountered for %s" % drop_course)
                    _add_error(e)

                except ElectionSuccess as e:
                    # 退选接口可能返回的是补选成功（不太可能，但防御性处理）
                    cout.info("Unexpected ElectionSuccess during cancel: %s" % e)

                except TipsException as e:
                    ferr.error(e)
                    cout.warning("TipsException during cancel: %s" % e)
                    _add_error(e)

                except Exception as e:
                    ferr.error(e)
                    cout.warning("Exception during cancel: %s" % e)
                    _add_error(e)

            # 将 swap 补选目标按优先级顺序插入 tasks 前端（退课已执行，补选优先于普通选课）
            for item in reversed(swap_elect_queue):
                tasks.appendleft(item)

            ## elect available courses

            if len(tasks) == 0:
                cout.info("No course available")
                continue

            elected = []  # cache elected courses dynamically from `get_ElectSupplement`

            while len(tasks) > 0:
                ix, course = tasks.popleft()

                is_mutex = False

                # dynamically filter course by mutex rules
                for (mix,) in np.argwhere(mutexes[ix, :] == 1):
                    mc = goals[mix]
                    if mc in elected:  # ignore course in advanced
                        is_mutex = True
                        cout.info("%s --x-- %s" % (course, mc))
                        cout.info("%s is ignored by mutex rules in advance" % course)
                        _ignore_course(course, "Mutex rules")
                        break

                if is_mutex:
                    continue

                cout.info("Try to elect %s" % course)

                ## validate captcha first

                captcha_fail_count = 0
                max_captcha_fails = 5
                
                while True:
                    cout.info("Fetch a captcha")
                    r = elective.get_DrawServlet()

                    captcha = recognizer.recognize(r.content)
                    cout.info("Recognition result: %s" % captcha.code)

                    r = elective.get_Validate(username, captcha.code)
                    try:
                        res = r.json()["valid"]  # 可能会返回一个错误网页
                    except Exception as e:
                        ferr.error(e)
                        raise OperationFailedError(msg="Unable to validate captcha")

                    if res == "2":
                        cout.info("Validation passed")
                        break
                    elif res == "0":
                        captcha_fail_count += 1
                        cout.info("Validation failed (attempt %d/%d)" % (captcha_fail_count, max_captcha_fails))
                        # notify.send_bark_push(msg=WECHAT_MSG[2], prefix=WECHAT_PREFIX[2])
                        cout.info("Auto error caching skipped for good")
                        
                        if captcha_fail_count >= max_captcha_fails:
                            cout.warning("Captcha validation failed %d times, skipping this course" % max_captcha_fails)
                            break
                        else:
                            cout.info("Try again")
                    else:
                        cout.warning("Unknown validation result: %s" % res)
                
                # 如果验证码失败次数达到上限，跳过当前课程
                if captcha_fail_count >= max_captcha_fails:
                    cout.info("Skipping course %s due to captcha validation failures" % course)
                    continue

                ## try to elect

                try:
                    r = elective.get_ElectSupplement(course.href)

                except ElectionRepeatedError as e:
                    ferr.error(e)
                    cout.warning("ElectionRepeatedError encountered")
                    notify.send_bark_push(msg=WECHAT_MSG[3], prefix=WECHAT_PREFIX[3])
                    _ignore_course(course, "Repeated")
                    _add_error(e)

                except TimeConflictError as e:
                    ferr.error(e)
                    cout.warning("TimeConflictError encountered")
                    notify.send_bark_push(
                        msg=WECHAT_MSG[4] + str(course), prefix=WECHAT_PREFIX[3]
                    )
                    _ignore_course(course, "Time conflict")
                    _add_error(e)

                except ExamTimeConflictError as e:
                    ferr.error(e)
                    cout.warning("ExamTimeConflictError encountered")
                    notify.send_bark_push(
                        msg=WECHAT_MSG[5] + str(course), prefix=WECHAT_PREFIX[3]
                    )
                    _ignore_course(course, "Exam time conflict")
                    _add_error(e)

                except ElectionPermissionError as e:
                    ferr.error(e)
                    cout.warning("ElectionPermissionError encountered")
                    _ignore_course(course, "Permission required")
                    _add_error(e)

                except CreditsLimitedError as e:
                    ferr.error(e)
                    cout.warning("CreditsLimitedError encountered")
                    _ignore_course(course, "Credits limited")
                    _add_error(e)

                except MutexCourseError as e:
                    ferr.error(e)
                    cout.warning("MutexCourseError encountered")
                    _ignore_course(course, "Mutual exclusive")
                    _add_error(e)

                except MultiEnglishCourseError as e:
                    ferr.error(e)
                    cout.warning("MultiEnglishCourseError encountered")
                    _ignore_course(course, "Multi English course")
                    _add_error(e)

                except MultiPECourseError as e:
                    ferr.error(e)
                    cout.warning("MultiPECourseError encountered")
                    _ignore_course(course, "Multi PE course")
                    _add_error(e)

                except ElectionFailedError as e:
                    ferr.error(e)
                    cout.warning(
                        "ElectionFailedError encountered"
                    )  # 具体原因不明，且不能马上重试
                    _add_error(e)

                except QuotaLimitedError as e:
                    ferr.error(e)
                    # 选课网可能会发回异常数据，本身名额 180/180 的课会发 180/0，这个时候选课会得到这个错误
                    if course.used_quota == 0:
                        cout.warning(
                            "Abnormal status of %s, a bug of 'elective.pku.edu.cn' found"
                            % course
                        )
                    else:
                        ferr.critical("Unexcepted behaviour")  # 没有理由运行到这里
                        _add_error(e)

                except ElectionSuccess as e:
                    # 不从此处加入 ignored，而是在下回合根据教学网返回的实际选课结果来决定是否忽略
                    cout.info("%s is ELECTED !" % course)
                    swap_pending_targets.discard(ix)  # swap 补选成功，移除安全追踪
                    notify.send_bark_push(
                        msg=WECHAT_MSG[1] + str(course), prefix=WECHAT_PREFIX[1]
                    )
                    # --------------------------------------------------------------------------
                    # Issue #25
                    # --------------------------------------------------------------------------
                    # 但是动态地更新 elected，如果同一回合内有多门课可以被选，并且根据 mutex rules，
                    # 低优先级的课和刚选上的高优先级课冲突，那么轮到低优先级的课提交选课请求的时候，
                    # 根据这个动态更新的 elected 它将会被提前地忽略（而不是留到下一循环回合的开始时才被忽略）
                    # --------------------------------------------------------------------------
                    r = e.response  # get response from error ... a bit ugly
                    tables = get_tables(r._tree)
                    # use clear() + extend() instead of op `=` to ensure `id(elected)` doesn't change
                    elected.clear()
                    elected.extend(get_courses(tables[1]))

                except RuntimeError as e:
                    ferr.critical(e)
                    ferr.critical(
                        "RuntimeError with Course(name=%r, class_no=%d, school=%r, status=%s, href=%r)"
                        % (
                            course.name,
                            course.class_no,
                            course.school,
                            course.status,
                            course.href,
                        )
                    )
                    # use this private function of 'hook.py' to dump the response from `get_SupplyCancel` or `get_supplement`
                    file = _dump_request(page_r)
                    ferr.critical(
                        "Dump response from 'get_SupplyCancel / get_supplement' to %s"
                        % file
                    )
                    raise e

                except Exception as e:
                    raise e  # don't increase error count here

        except UserInputException as e:
            cout.error(e)
            _add_error(e)
            raise e

        except (ServerError, StatusCodeError) as e:
            ferr.error(e)
            cout.warning("ServerError/StatusCodeError encountered")
            _add_error(e)

        except OperationFailedError as e:
            ferr.error(e)
            cout.warning("OperationFailedError encountered")
            _add_error(e)

        except UnexceptedHTMLFormat as e:
            ferr.error(e)
            cout.warning("UnexceptedHTMLFormat encountered")
            _add_error(e)

        except RequestException as e:
            ferr.error(e)
            cout.warning("RequestException encountered")
            _add_error(e)

        except IAAAException as e:
            ferr.error(e)
            cout.warning("IAAAException encountered")
            _add_error(e)

        except _ElectiveNeedsLogin as e:
            cout.info("client: %s needs Login" % elective.id)
            reloginPool.put_nowait(elective)
            elective = None
            noWait = True

        except _ElectiveExpired as e:
            cout.info("client: %s expired" % elective.id)
            reloginPool.put_nowait(elective)
            elective = None
            noWait = True

        except (
            SessionExpiredError,
            InvalidTokenError,
            NoAuthInfoError,
            SharedSessionError,
        ) as e:
            ferr.error(e)
            _add_error(e)
            cout.info("client: %s needs relogin" % elective.id)
            reloginPool.put_nowait(elective)
            elective = None
            noWait = True

        except CaughtCheatingError as e:
            ferr.critical(e)  # critical error !
            _add_error(e)
            raise e

        except SystemException as e:
            ferr.error(e)
            cout.warning("SystemException encountered")
            _add_error(e)

        except TipsException as e:
            ferr.error(e)
            cout.warning("TipsException encountered")
            _add_error(e)

        except OperationTimeoutError as e:
            ferr.error(e)
            cout.warning("OperationTimeoutError encountered")
            _add_error(e)

        except json.JSONDecodeError as e:
            ferr.error(e)
            cout.warning("JSONDecodeError encountered")
            _add_error(e)

        except KeyboardInterrupt as e:
            raise e

        except Exception as e:
            ferr.exception(e)
            _add_error(e)
            raise e

        finally:
            if elective is not None:  # change elective client
                electivePool.put_nowait(elective)
                elective = None

            # swap 安全检查：退课成功但补选失败时，缩短等待间隔加速重试
            # 放在 finally 中确保即使 swap 执行中途抛异常也能触发安全恢复
            if swap_pending_targets:
                failed_names = ", ".join(str(goals[ix]) for ix in swap_pending_targets if ix < len(goals))
                cout.warning("SWAP SAFETY: 已退课但补选未成功 [%s]，缩短等待间隔加速重试！" % failed_names)
                swap_rush_retry = True

            if noWait:
                cout.info("")
                cout.info("======== END Loop %d ========" % environ.elective_loop)
                cout.info("")
            elif swap_rush_retry:
                # 退课成功但补选失败，用固定的短间隔 2.5*(1.0±0.1)s 加速重试
                t = 2.5 * (1.0 + random.uniform(-0.1, 0.1))
                cout.info("")
                cout.info("======== END Loop %d ========" % environ.elective_loop)
                cout.info("SWAP SAFETY: sleep %.2f s (shorter interval for recovery)" % t)
                cout.info("")
                time.sleep(t)
            else:
                t = _get_refresh_interval()
                cout.info("")
                cout.info("======== END Loop %d ========" % environ.elective_loop)
                cout.info("Main loop sleep %s s" % t)
                cout.info("")
                time.sleep(t)
