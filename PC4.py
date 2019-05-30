# -*- coding: utf-8 -*-
"""
Created on Thu Apr 11 08:09:21 2019

@author: guipirod
"""

from random import sample
from random import random as rd
from random import randint as rdi


def mutatePC4(element, ratio, control=None):
    upper_limit = control.num_packages - 1
    for i in range(len(element)):
        if rd() < ratio:
            if rd() < .34:
                element[i] = -1
            else:
                element[i] = rdi(0, upper_limit)
    return element


def mutatePC4lite(control, element, ratio):
    upper_limit = control.num_packages - 1
    total_mutations = int(control.num_packages * ratio)
    for pointer in sample(range(control.num_packages), total_mutations):
        if rd() < .34:
            element[pointer]
        else:
            element[pointer] = rdi(0, upper_limit)
    return element


def generationPC4(control):
    return [rdi(0, control.num_packages - 1) if rd() > .33 else -1 for _ in range(control.num_paths)]