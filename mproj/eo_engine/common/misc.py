import os
import re
from collections import defaultdict
from datetime import datetime, date as dt_date
from pathlib import Path
from typing import List

from eo_engine.errors import AfriCultuReSError


def get_spider_loader():
    """Returns a list of all the available spiders."""
    # requires SCRAPY_SETTINGS_MODULE env variableX

    from scrapy.spiderloader import SpiderLoader
    from scrapy.utils.project import get_project_settings
    # currently it's set in DJ's manage.py
    scrapy_settings = get_project_settings()
    spider_loader = SpiderLoader.from_settings(scrapy_settings)

    return spider_loader


def get_crawler_process():
    from scrapy.crawler import CrawlerProcess
    from scrapy.utils.project import get_project_settings
    scrapy_settings = get_project_settings()
    return CrawlerProcess(scrapy_settings)


def list_spiders() -> List[str]:
    spider_loader = get_spider_loader()

    return spider_loader.list()


# recursive default_dict
# x = rec_dd()
# x['a']['b'] = {a:{b:{}}
rec_dd = defaultdict(lambda: rec_dd)


def str_to_date(token: str, regex_string: str, re_flags=re.IGNORECASE) -> dt_date:
    from eo_engine.errors import AfriCultuReSError
    pat = re.compile(regex_string, re_flags)
    try:
        match = pat.match(token)
        groupdict = match.groupdict()
        YYMMDD_str = groupdict.get('YYMMDD', None)
        YYYYMM_str = groupdict.get('YYYYMM', None)
        YYYYMMDD_str = groupdict.get('YYYYMMDD', None)
        YYKK_str = groupdict.get('YYKK', None)
        YYYYDOY_str = groupdict.get('YYYYDOY', None)
        if len(list(filter(lambda x: x is not None, [YYMMDD_str, YYYYMMDD_str, YYKK_str, YYYYDOY_str]))) > 1:
            raise AfriCultuReSError(
                'More than one date token was captured, please change the regEx to only capture one.')
        if YYYYMMDD_str:
            return datetime.strptime(YYYYMMDD_str, '%Y%m%d').date()
        if YYMMDD_str:
            return datetime.strptime(YYYYMMDD_str, '%y%m%d').date()
        if YYKK_str:
            from eo_engine.common.time import runningdekad2date
            year = int(YYKK_str[2:])
            rdekad = int(YYKK_str[:2])
            return runningdekad2date(year, rdekad)[0]
        if YYYYDOY_str:
            return datetime.strptime(YYYYDOY_str, '%Y%j').date()
        if YYYYMM_str:
            return datetime.strptime(YYYYMM_str, '%Y%m').date()
        raise AfriCultuReSError()

    except (AfriCultuReSError, AttributeError) as e:
        raise AfriCultuReSError(
            f'BUG_REPORT:SHOULD_NOT_END_HERE:No date tokens found in string {token} and regEx string +{regex_string}+.'
            f'\nDid you forget to add YYYY/MM/DD tokens?') from e


def write_line_to_file(file_path: Path, token: str, echo=False):
    """Append  token string to file. if File does not exist it will be made."""
    if file_path.is_dir():
        raise AfriCultuReSError('file is a directory. It should be a path')
    if not file_path.exists():
        file_path.touch(exist_ok=True)

    # open in append mode
    with file_path.open(mode='a') as fh:
        fh.write(token)
        fh.write(os.linesep)
    if echo:
        print(token)

    return
