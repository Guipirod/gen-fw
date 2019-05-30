# -*- coding: utf-8 -*-
"""
Created on Mon Feb 18 03:46:30 2019

@author: guipirod
"""


def __bool_int(bitlist):
    out = 0
    for bit in bitlist:
        out = (out << 1) | int(bit)
    return out


def __bitlist_to_intlist(ilist, n):
    # splits ilist on n intervals
    divi = int(len(ilist) / n)
    return [__bool_int(ilist[i * n:(i + 1) * n]) for i in range(divi)]


def evaluate_500(element):
    negative = False
    res = 0
    for i in __bitlist_to_intlist(element, 20):
        if negative:
            res -= i
        else:
            res += i
        negative = i % 2 != 0 or i > 2048
    return res


def evaluate_100c(element, control):
    return control.evaluate(__bitlist_to_intlist(element, 10))


def memoize(f):
    memo = {}

    def helper(x):
        if x not in memo:
            memo[x] = f(x)
        return memo[x]

    return helper


def evaluate_paths(element, control):
    # int_list = pc.bitlist_to_intlist(element, pc.bits+1)
    return control.evaluate(element)
