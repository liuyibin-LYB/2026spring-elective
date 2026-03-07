#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# filename: parser.py
# modified: 2019-09-09

import re
from lxml import etree
from .course import Course

_regexBzfxSida = re.compile(r'\?sida=(\S+?)&sttp=(?:bzx|bfx)')


def get_tree_from_response(r):
    return etree.HTML(r.text) # 不要用 r.content, 否则可能会以 latin-1 编码

def get_tree(content):
    return etree.HTML(content)

def get_tables(tree):
    return tree.xpath('.//table//table[@class="datagrid"]')

def get_table_header(table):
    return table.xpath('.//tr[@class="datagrid-header"]/th/text()')

def get_table_trs(table):
    return table.xpath('.//tr[@class="datagrid-odd" or @class="datagrid-even"]')

def get_title(tree):
    title = tree.find('.//head/title')
    if title is None: # 双学位 sso_login 后先到 主修/辅双 选择页，这个页面没有 title 标签
        return None
    return title.text

def get_errInfo(tree):
    tds = tree.xpath(".//table//table//table//td")
    assert len(tds) == 1
    td = tds[0]
    strong = td.getchildren()[0]
    assert strong.tag == 'strong' and strong.text in ('出错提示:', '提示:')
    return "".join(td.xpath('./text()')).strip()

def get_tips(tree):
    tips = tree.xpath('.//td[@id="msgTips"]')
    if len(tips) == 0:
        return None
    td = tips[0].xpath('.//table//table//td')[1]
    return "".join(td.xpath('.//text()')).strip()

def get_sida(r):
    return _regexBzfxSida.search(r.text).group(1)

def get_courses(table):
    header = get_table_header(table)
    trs = get_table_trs(table)
    ixs = tuple(map(header.index, ["课程名","班号","开课单位"]))
    cs = []
    for tr in trs:
        t = tr.xpath('./th | ./td')
        name, class_no, school = map(lambda ix: t[ix].xpath('.//text()')[0], ixs)
        c = Course(name, class_no, school)
        cs.append(c)
    return cs

def get_courses_with_detail(table):
    header = get_table_header(table)
    trs = get_table_trs(table)
    ixs = tuple(map(header.index, ["课程名","班号","开课单位","限数/已选","补选"]))
    cs = []
    for tr in trs:
        t = tr.xpath('./th | ./td')
        name, class_no, school, status, _ = map(lambda ix: t[ix].xpath('.//text()')[0], ixs)
        status = tuple(map(int, status.split("/")))
        href = t[ixs[-1]].xpath('./a/@href')[0]
        c = Course(name, class_no, school, status, href)
        cs.append(c)
    return cs


def get_elected_with_detail(table):
    """解析已选上列表，提取退选链接"""
    header = get_table_header(table)
    trs = get_table_trs(table)
    # 已选上列表中有"退选"列
    try:
        cancel_ix = header.index("退选")
    except ValueError:
        # 如果没有"退选"列，返回空列表
        return []
    ixs = tuple(map(header.index, ["课程名","班号","开课单位"]))
    cs = []
    for tr in trs:
        t = tr.xpath('./th | ./td')
        name, class_no, school = map(lambda ix: t[ix].xpath('.//text()')[0], ixs)
        cancel_hrefs = t[cancel_ix].xpath('./a/@href')
        cancel_href = cancel_hrefs[0] if cancel_hrefs else None
        c = Course(name, class_no, school, href=cancel_href)
        cs.append(c)
    return cs

