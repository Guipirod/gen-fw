from random import uniform
from random import random as rd
from random import randint as rdi


""" GENERATION """


def generation_boolean(length: int) -> list:
    # creates a list of random 0 and 1 values of a given length
    return [rdi(0, 1) for _ in range(length)]


def generation_string(length: int) -> str:
    # creates a string of random 0 and 1 values of a given length
    return ''.join([str(rdi(0, 1)) for _ in range(length)])


""" SELECTION """


def selection_ranked(population: list, num: int, total_rank: int) -> list:
    return [(__ranked_aux(population, total_rank), __ranked_aux(population, total_rank)) for _ in range(num)]


def __ranked_aux(population: list, total_rank: int) -> list:
    pick = uniform(1, total_rank)
    accumulated = 0
    for x, y in zip(reversed(range(1, len(population))), population):
        accumulated += x
        if accumulated >= pick:
            return y


def selection_pseudo(population: list, num: int) -> list:
    # selects 'num' groups of 2 parents giving a 50% chance to the 25% best,
    # a 25% chance the 2nd 25% best, a 15% chance to the 3rd 25% best
    # and 10% chance to the rest. Needs a sorted list.
    # WARNING: this method needs a sorted population
    len1 = len(population)-1
    len4 = int(len(population)/4)
    len42 = len4*2
    len43 = len4*3
    return [(population[__pseudo_aux(len1, len4, len42, len43)],
             population[__pseudo_aux(len1, len4, len42, len43)]) for _ in range(num)]


def __pseudo_aux(len1: int, len4: int, len42: int, len43: int) -> int:
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


def selection_random(population: list, num: int) -> list:
    # selects 2 parents at random, simple but fail
    len1 = len(population)-1
    return [(population[rdi(0, len1)], population[rdi(0, len1)]) for _ in range(num)]


def selection_roulette(population: list, total_fitness: float, num: int) -> list:
    # selects 2 parents based on their fitness value
    # fails with negative fitness
    return [(__roulette_aux(population, total_fitness), __roulette_aux(population, total_fitness)) for _ in range(num)]


def __roulette_aux(population: list, total_fitness: float) -> list:
    pick = uniform(0, total_fitness)
    current = 0
    for chromosome in population:
        current += chromosome[0]
        if current > pick:
            return chromosome


""" CROSSING """


def cross_single_point(parent1: list, parent2: list) -> list:
    # creates 2 new elements splitting the 2 parents in 1 point
    point = rdi(1, len(parent1)-1)
    return [parent1[:point]+parent2[point:], parent2[:point]+parent1[point:]]


def cross_two_point(parent1: list, parent2: list) -> list:
    # creates 2 new elements splitting the 2 parents in 2 points
    rd1 = rdi(1, len(parent1)-1)
    rd2 = rdi(1, len(parent1)-1)
    while rd1 == rd2:
        rd2 = rdi(1, len(parent1)-1)
    point1 = rd1 if rd1 < rd2 else rd2
    point2 = rd2 if rd1 < rd2 else rd1
    return [parent1[:point1] + parent2[point1:point2] + parent1[point2:],
            parent2[:point1] + parent1[point1:point2] + parent2[point2:]]


def cross_uniform(parent1: list, parent2: list, ratio: float = .5) -> list:
    # creates 2 new elements splitting the parents at random
    # ratio: probability of any given value to end up in son 1, default 0.5
    son1, son2 = [], []
    for index in range(len(parent1)):
        if rd() < ratio:
            son1.append(parent1[index])
            son2.append(parent2[index])
        else:
            son1.append(parent2[index])
            son2.append(parent1[index])
    if isinstance(parent1, str):
        return [''.join(son1), ''.join(son2)]
    else:
        return [son1, son2]


""" MUTATION """


def mutation_string(son: str, prob: float) -> str:
    # version of binary mutation for strings
    return ''.join([x if rd() > prob else '1' if x == '0' else '0' for x in son])


def mutation_binary(son: list, prob: float) -> list:
    # default mutation function, used for binary lists
    return [x if rd() > prob else 1 if x == 0 else 0 for x in son]


def mutation_boolean(son: list, prob: float) -> list:
    # mutation function for boolean-based lists
    return [x if rd() > prob else not x for x in son]


""" SURVIVAL """


def survival_roulette(population: list, size: int) -> list:
    survivors = []
    total_fitness = sum([x for x, _ in population])
    while len(survivors) < size:
        pointer = __roulette_surv_aux(population, total_fitness)
        total_fitness -= population[pointer][0]
        survivors.append(population[pointer])
        del population[pointer]
    return survivors


def __roulette_surv_aux(population: list, total_fitness: float) -> int:
    pick = uniform(0, total_fitness)
    current = 0
    for i in range(len(population)):
        current += population[i][0]
        if current > pick:
            return i


def simple_survival(population: list, size: int) -> list:
    # WARNING: this method needs a sorted population
    return population[:size]


def survival_pseudo(population: list, size: int) -> list:
    # WARNING: this method needs a sorted population
    survivors = []
    while len(survivors) < size:
        len1 = len(population) - 1
        len4 = int(len(population) / 4)
        len42 = len4 * 2
        len43 = len4 * 3
        pointer = __pseudo_aux(len1, len4, len42, len43)
        survivors.append(population[pointer])
        del population[pointer]
    return survivors


def survival_ranked(population: list, size: int) -> list:
    # WARNING: this method needs a sorted population
    survivors = []
    while len(survivors) < size:
        size = len(population)
        total_rank = int((size * (size + 1) / 2) - size)
        pointer = __ranked_surv_aux(population, total_rank)
        survivors.append(population[pointer])
        del population[pointer]
    return survivors


def __ranked_surv_aux(population: list, total_rank: int) -> int:
    pick = uniform(1, total_rank)
    accumulated = 0
    for x, y in zip(reversed(range(1, len(population))), population):
        accumulated += x
        if accumulated >= pick:
            return x-1
