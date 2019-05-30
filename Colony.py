
import copy
from math import inf
from time import time
import colony_functions as f


class Colony:

    def __init__(self, evaluation, **kargs):
        # generation function
        # this function can be either an int (generate lists of n [0,1] values) or
        # an int written as a string (generating strings of n ['0','1'] values)
        # any problem-specific generation function must be provided by the user
        self.generation_f = kargs['generation'] if 'generation' in kargs else None
        # colony functions
        self.__evaluation_f = evaluation
        self.cross_f = kargs['cross'] if 'cross' in kargs else 'singlepoint'
        self.mutation_f = kargs['mutation'] if 'mutation' in kargs else 'binary'
        self.selection_f = kargs['selection'] if 'selection' in kargs else 'roulette'
        self.death_f = kargs['death'] if 'death' in kargs else 'weighted'  # BORRAR
        self.survival_f = kargs['survival'] if 'survival' in kargs else 'pseudo'  # get survivors if elitism is on
        # ratios (0...1)
        self.mut_ratio = kargs['mut_ratio'] if 'mut_ratio' in kargs else .05  # allele mutation probability
        self.elitism = kargs['elitism'] if 'elitism' in kargs else 0  # generational survivors
        # control object, it can be of any given nature
        self.control_obj = copy.deepcopy(kargs['control_obj']) if 'control_obj' in kargs else None
        # control variables
        self.__best_fitness = -inf  # best fitness result
        self.__sorted = False  # controls if the colony is sorted
        self.__population = []  # colony list population, each element is a tuple: (fitness, individual)
        self.__colony_size = 0
        # debug timers
        self.total_time = 0
        self.fitness_time = 0
        self.selection_time = 0
        self.crossover_time = 0
        self.mutation_time = 0
        self.extintion_time = 0

    # print debug timers

    def init_time(self):
        # BORRA ESTO: DEBUG TIMER
        self.total_time = 0
        self.fitness_time = 0
        self.selection_time = 0
        self.crossover_time = 0
        self.mutation_time = 0
        self.extintion_time = 0

    def timer(self):
        print('Total time: {:.7f}'.format(self.total_time))
        print('+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-')
        print('Fitness: {:.5f} {:.5f}%'.format(self.fitness_time, 100 * self.fitness_time / self.total_time))
        print('Selection: {:.5f} {:.5f}%'.format(self.selection_time, 100 * self.selection_time / self.total_time))
        print('Crossover: {:.5f} {:.5f}%'.format(self.crossover_time, 100 * self.crossover_time / self.total_time))
        print('Mutation: {:.5f} {:.5f}%'.format(self.mutation_time, 100 * self.mutation_time / self.total_time))
        print('Extintion: {:.5f} {:.5f}%'.format(self.extintion_time, 100 * self.extintion_time / self.total_time))

    # object functions

    def __getitem__(self, i):
        return self.__population[i]

    def __setitem__(self, i, j):
        self.__population[i] = (self.__evaluation_f(j), j)
        self.__sorted = False
        return self

    def __delitem__(self, i):
        del self.__population[i]
        self.__colony_size -= 1
        return self

    def __len__(self):
        return self.__colony_size

    def __list__(self):
        return self.__population

    def __contains__(self, i):
        return i in [y for _, y in self.__population]

    def append(self, element):
        self.__population.append((self.__evaluation_f(element), element))
        self.__colony_size += 1
        self.__sorted = False
        return self

    def remove(self, element):
        index = [y for _, y in self.__population].index(element)
        del self.__population[index]
        return self

    def _cmp(self, other):
        return self.__colony_size - len(other)

    def __lt__(self, other):
        return self._cmp(other) < 0

    def __le__(self, other):
        return self._cmp(other) <= 0

    def __eq__(self, other):
        return self._cmp(other) == 0

    def __ne__(self, other):
        return self._cmp(other) != 0

    def __gt__(self, other):
        return self._cmp(other) > 0

    def __ge__(self, other):
        return self._cmp(other) >= 0

    # setters and getters

    def is_sorted(self) -> bool:
        return self.__sorted

    def best_fitness(self) -> float:
        return self.__best_fitness

    def set_population(self, population, evaluate=True):
        if evaluate:
            self.evaluate_input(population)
        else:
            self.__population = population
        self.__colony_size = len(population)
        self.__sorted = False
        self.__best_fitness = -inf
        return self

    def set_evaluation(self, evaluation):
        self.__evaluation_f = evaluation
        self.evaluate()
        self.__sorted = False
        self.__best_fitness = -inf
        return self

    ''' the following functions allow the master to map a change on all colonies'''

    def set_generation(self, generation):
        self.generation_f = generation
        return self

    def set_cross(self, cross):
        self.cross_f = cross
        return self

    def set_mutation(self, mutation):
        self.mutation_f = mutation
        return self

    def set_selection(self, selection):
        self.selection_f = selection
        return self

    def set_death(self, death):
        self.death_f = death
        return self

    def set_survival(self, survival):
        self.survival_f = survival
        return self

    def set_control(self, control):
        self.control_obj = copy.deepcopy(control)  # due to clustering, the object must be copied not referenced
        return self

    def set_mut_ratio(self, ratio: float):
        self.mut_ratio = ratio
        return self

    def set_elitism(self, ratio: float):
        self.elitism = ratio
        return self

    # colony functions

    def evaluate(self):
        tevaluate = time()  # DELETE THIS
        if self.control_obj:
            self.__population = [(self.__evaluation_f(y, self.control_obj), y) for _, y in self.__population]
        else:
            self.__population = [(self.__evaluation_f(y), y) for _, y in self.__population]
        self.fitness_time += time() - tevaluate  # DELETE THIS

    def evaluate_input(self, ilist):
        tevaluate = time()  # DELETE THIS
        if self.control_obj:
            self.__population = [(self.__evaluation_f(y, self.control_obj), y) for y in ilist]
        else:
            self.__population = [(self.__evaluation_f(y), y) for y in ilist]
        self.__sorted = False
        self.fitness_time += time() - tevaluate  # DELETE THIS

    def __evaluate_return(self, ilist):
        if self.control_obj:
            return [(self.__evaluation_f(i, self.control_obj), i) for i in ilist]
        else:
            return [(self.__evaluation_f(i), i) for i in ilist]

    def sort_population(self):
        if not self.__sorted:
            self.__population.sort(reverse=True)
            self.__sorted = True
            self.__best_fitness = self.__population[0][0]

    def get_population(self, fitness: bool = True):
        if fitness:
            return self.__population
        else:
            return list(map(lambda x: x[1], self.__population))

    def populate(self, members):
        if self.generation_f:
            if isinstance(self.generation_f, int):
                self.evaluate_input(['' for _ in range(members)])
            elif isinstance(self.generation_f, str):
                self.evaluate_input(['' for _ in range(members)])
            elif callable(self.generation_f):
                if self.control_obj:
                    self.evaluate_input([self.generation_f(self.control_obj) for _ in range(members)])
                else:
                    self.evaluate_input([self.generation_f() for _ in range(members)])
            else:
                raise Exception('Wrong generation function')
            self.__colony_size = members
            self.__sorted = False
            self.__best_fitness = -inf
        else:
            raise Exception('You must set a generation function in order to populate')
        return self

    def get_best(self, *args):
        if self.__colony_size == 0:
            return -inf, None
        self.sort_population()
        if args:
            return self.__population[:min(args[0], self.__colony_size)]
        else:
            return self.__population[0]

    # evolution functions

    def evolve(self, *args, **kargs):
        self.init_time()  # DELETE THIS
        tevolve = time()  # DELETE THIS
        objective = kargs['objective'] if 'objective' in kargs else inf  # target value
        wait = kargs['wait'] if 'wait' in kargs else -1  # iteration to wait if no improvement is noticed
        iterations = args[0] if args else 1
        if self.__colony_size == 0:
            raise Exception('You must initialize a population in order to evolve')
        elif self.__best_fitness < objective:
            new_pool_size = int(self.__colony_size * (1-self.elitism))  # how many are born each generation
            generational_survivors = self.__colony_size - new_pool_size  # how many survive from previous generation
            best = -inf
            end_countdown = 0
            for _ in range(iterations):
                # 1. select parents
                parents = self.__select_parents(new_pool_size)
                # 2. reproduction
                newborns = self.__cross_parents(parents)
                newborns = [item for sublist in newborns for item in sublist]
                # 3. mutations
                newborns = self.__mutate_newborns(newborns)
                # 4. evaluation
                tevaluate = time()  # DELETE THIS
                newborns = self.__evaluate_return(newborns)
                self.fitness_time += time() - tevaluate  # DELETE THIS
                # 5. survivors TODO
                self.__choose_survivors(generational_survivors)
                # 6. store new population
                self.__population += newborns
                self.__sorted = False
                # 7. stop control
                if self.__best_fitness >= objective:
                    break
                elif self.__best_fitness > best:
                    best = self.__best_fitness
                    end_countdown = 0
                else:
                    end_countdown += 1
                    if end_countdown == wait:
                        break
        self.total_time += time() - tevolve  # DELETE THIS
        return self

    def __select_parents(self, new_pool_size):
        tselect = time()  # DELETE THIS
        if isinstance(self.selection_f, str):
            if self.selection_f == 'roulette':
                parents = f.roulette_selection(self.__population, new_pool_size)
            elif self.selection_f == 'ranked':
                self.sort_population()
                parents = f.ranked_selection(self.__population, new_pool_size)
            elif self.selection_f == 'tournament':
                parents = f.tournament_selection(self.__population, new_pool_size)
            elif self.selection_f == 'random':
                parents = f.tournament_selection(self.__population, new_pool_size)
            elif self.selection_f == 'pseudo':
                self.sort_population()
                parents = f.pseudo_selection(self.__population, new_pool_size)
            else:
                raise Exception('Unknown selection function {}'.format(self.selection_f))
        elif callable(self.selection_f):
            parents = self.selection_f(self.__population, new_pool_size)
        else:
            raise Exception('Wrong selection function')
        self.selection_time += time() - tselect  # DELETE THIS
        return parents

    def __cross_parents(self, parents):
        tcross = time()  # DELETE THIS
        if isinstance(self.cross_f, str):
            if self.cross_f == 'singlepoint':
                newborns = [f.single_point_cross(pair) for pair in parents]
            elif self.cross_f == 'twopoint':
                newborns = [f.two_point_cross(pair) for pair in parents]
            elif self.cross_f == 'uniform':
                newborns = [f.uniform_cross(pair) for pair in parents]
            else:
                raise Exception('Unknown selection function {}'.format(self.cross_f))
        elif callable(self.cross_f):
            newborns = [self.cross_f(pair) for pair in parents]
        else:
            raise Exception('Wrong crossing function')

        self.crossover_time += time() - tcross  # DELETE THIS
        return newborns

    def __mutate_newborns(self, newborns):
        tmutate = time()  # DELETE THIS
        if isinstance(self.mutation_f, str):
            if self.mutation_f == 'binary':
                mutants = [f.binary_mutation(n, self.mut_ratio, control=self.control_obj) for n in newborns]
            elif self.mutation_f == 'string':
                mutants = [f.string_mutation(n, self.mut_ratio, control=self.control_obj) for n in newborns]
            elif self.mutation_f == 'boolean':
                mutants = [f.boolean_mutation(n, self.mut_ratio, control=self.control_obj) for n in newborns]
            else:
                raise Exception('Unknown mutation function {}'.format(self.mutation_f))
        elif callable(self.mutation_f):
            mutants = [self.mutation_f(n, self.mut_ratio, control=self.control_obj) for n in newborns]
        else:
            raise Exception('Wrong mutation function')
        self.mutation_time += time() - tmutate  # DELETE THIS
        return mutants

    def __choose_survivors(self, generational_survivors):
        tdeath = time()  # DELETE THIS
        if isinstance(self.survival_f, str):
            if self.survival_f == 'ranked':
                self.sort_population()
                f.ranked_survival(self.__population, generational_survivors)
            elif self.survival_f == 'pseudo':
                self.sort_population()
                f.pseudo_survival(self.__population, generational_survivors)
            elif self.survival_f == 'simple':
                self.sort_population()
                self.__population = f.simple_survival(self.__population, generational_survivors)
            else:
                raise Exception('Unknown mutation function {}'.format(self.survival_f))
        elif callable(self.survival_f):
            self.__population = self.survival_f(self.__population)
        else:
            raise Exception('Wrong survival function')
        self.extintion_time += time() - tdeath  # DELETE THIS
