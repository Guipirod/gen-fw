from random import uniform
from random import random as rd
from random import randint as rdi


# selection functions


def pseudo_selection(population, num: int):
    len1 = len(population) - 1
    len4 = int(len(population) / 4)
    len42 = len4 * 2
    len43 = len4 * 3
    aux = [] if num % 2 == 0 else [__pseudo_weighted(len1, len4, len42, len43)[1]]
    return aux + [(population[__pseudo_weighted(len1, len4, len42, len43)][1],
                   population[__pseudo_weighted(len1, len4, len42, len43)][1])
                  for _ in range(num)]


def roulette_selection(population, num: int):
    """
    selects 2 parents based on their fitness value
    fails with negative fitness
    """
    total_fitness = sum([x for x, _ in population])
    aux = [] if num % 2 == 0 else [__roulette(population, total_fitness)]
    return aux + [(__roulette(population, total_fitness),
                   __roulette(population, total_fitness)) for _ in range(int(num/2))]


def ranked_selection(population, num: int):
    total_rank = sum(range(1, len(population)))
    aux = [] if num % 2 == 0 else [__ranked_roulette(population, total_rank)]
    return aux + [(__ranked_roulette(population, total_rank),
                   __ranked_roulette(population, total_rank)) for _ in range(int(num/2))]


def tournament_selection(population, num: int):
    """
    By default it will compare 4 elements
    """
    aux = [] if num % 2 == 0 else [__tournament(population)]
    return aux + [(__tournament(population),
                  __tournament(population)) for _ in range(int(num/2))]


def random_selection(population, num: int):
    # selects 2 parents at random, simple but fail
    len1 = len(population)-1
    aux = [] if num % 2 == 0 else [population[rdi(0, len1)]]
    return aux + [(population[rdi(0, len1)], population[rdi(0, len1)]) for _ in range(num)]


def __roulette(population, total_fitness):
    pick = uniform(0, total_fitness)
    accumulated = 0
    for chromosome in population:
        accumulated += chromosome[0]
        if accumulated > pick:
            return chromosome[1]


def __ranked_roulette(population, total_rank):
    pick = uniform(1, total_rank)
    accumulated = 0
    for x, y in zip(reversed(range(1, len(population))), population):
        accumulated += x
        if accumulated >= pick:
            return y[1]


def __tournament(population):
    best = population[rdi(0, len(population)-1)]
    for _ in range(3):
        contender = population[rdi(0, len(population)-1)]
        if contender[0] > best[0]:
            best = contender
    return best[1]


def __pseudo_weighted(len1, len4, len42, len43):
    # returns a random pointer using a non-linear probability
    aux = rd()
    if aux < 0.5:
        return rdi(0, len4)
    elif aux < 0.75:
        return rdi(0, len4)+len4
    elif aux < 0.90:
        return rdi(0, len4)+len42
    else:
        return min(rdi(0, len4)+len43, len1)


# crossing functions


def single_point_cross(parents):
    if len(parents) == 1:
        return parents[0],
    point = rdi(1, len(parents[0]) - 1)
    return [parents[0][:point] + parents[1][point:], parents[1][:point] + parents[0][point:]]


def two_point_cross(parents):
    if len(parents) == 1:
        return parents[0],
    rd1 = rdi(1, len(parents[0]) - 1)
    rd2 = rdi(1, len(parents[0]) - 1)
    while rd1 == rd2:
        rd2 = rdi(1, len(parents[0]) - 1)
    point1 = rd1 if rd1 < rd2 else rd2
    point2 = rd2 if rd1 < rd2 else rd1
    return [parents[0][:point1] + parents[1][point1:point2] + parents[0][point2:],
            parents[1][:point1] + parents[0][point1:point2] + parents[1][point2:]]


def uniform_cross(parents):
    if len(parents) == 1:
        return parents[0],
    son1, son2 = [], []
    for index in range(len(parents[0])):
        if rd() < .5:
            son1.append(parents[0][index])
            son2.append(parents[1][index])
        else:
            son1.append(parents[1][index])
            son2.append(parents[0][index])
    if isinstance(parents[0], str):
        return [''.join(son1), ''.join(son2)]
    else:
        return [son1, son2]


# mutation functions


def string_mutation(son, prob, **kargs):
    # version of binary mutation for strings
    return ''.join([x if rd()>prob else '1' if x == '0' else '0' for x in son])


def binary_mutation(son, prob, **kargs):
    # default mutation function, used for binary lists
    return [x if rd()>prob else 1 if x == 0 else 0 for x in son]


def boolean_mutation(son, prob, **kargs):
    # mutation function for boolean-based lists
    return [x if rd() > prob else not x for x in son]


def experimental_mutation(son, prob, **kargs):
    # experimental fast string boolean mutation
    num_mutations = int(prob*len(son))
    for _ in range(num_mutations):
        pointer = rdi(0, len(son)-1)
        son[pointer] = 1 if son[pointer] == 0 else 0
    return son


# survival functions


def simple_survival(population, size):
    # WARNING: this method needs a sorted population
    return population[:size]


def ranked_survival(population, size):
    # WARNING: this method needs a sorted population
    total_rank = sum(range(1, len(population)))
    while len(population) > size:
        pick = uniform(1, total_rank)
        accumulated = 0
        for i in range(1, len(population)):
            accumulated += i
            if accumulated >= pick:
                total_rank -= population[i][0]
                del population[i-1]
    return population


def pseudo_survival(population, size):
    # WARNING: this method needs a sorted population
    while len(population) > size:
        len1 = len(population)-1
        len4 = int(len(population)/4)
        len42 = len4*2
        len43 = len4*3
        selected = __pseudo_weighted_death(len1, len4, len42, len43)
        del population[selected]


def __pseudo_weighted_death(len1, len4, len42, len43):
    # returns a random pointer using a non-linear probability
    aux = rd()
    if aux < 0.5:
        return min(rdi(0, len4)+len43, len1)
    elif aux < 0.75:
        return rdi(0, len4)+len42
    elif aux < 0.90:
        return rdi(0, len4)+len4
    else:
        return rdi(0, len4)