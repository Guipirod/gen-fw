
import copy
from math import inf
from time import time
import colony_functions as f


class Colony:

    def __init__(self, evaluation, generation, cross='singlepoint', mutation='binary', selection='roulette',
                 survival='roulette', mut_ratio=0.05, survival_ratio=0.2, population=None, control_obj=None):
        # auxiliar counter
        # allows the colony to avoid the sort function if the population is already sorted
        self.__sorted = False
        # best fitness result
        self.__best_fitness = -inf
        # user-defined function
        self.__evaluation_f = evaluation
        # optional definition function, generation can be either a function
        # or a 'n' int, meaning it will be 'n' boolean values
        self.__generation_f = generation
        # optional definition functions, can be eiter a function or a str,
        # the latter case meaning it will use a pre-made function
        self.__cross_f = cross
        self.__mutation_f = mutation
        self.__selection_f = selection
        self.__survival_f = survival
        # ratios (0..1)
        self.__mut_ratio = mut_ratio
        self.__survival = survival_ratio
        # some problems may need a control object to manage
        # their functions. That object can be of any given nature
        self.__control_obj = copy.deepcopy(control_obj)
        # content list
        # population inicialization either by direct asignation (list)
        # or generation (None)
        if population:
            self.__colony_size = len(population)
            self.__evaluate(ilist=population)
            self.__rankval = int((self.__colony_size * (self.__colony_size + 1) / 2) - self.__colony_size)
        else:
            self.__colony_size = 0
            self.__population = []
            self.__rankval = 0
        # DEBUG VARIABLE
        self.last_iters = 0

    """ OBJECT FUNCTIONS """

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

    """ SETTERS AND GETTERS """

    def set_population(self, population):
        self.__colony_size = len(population)
        self.__evaluate(ilist=population)
        self.__sorted = False
        self.__best_fitness = -inf
        return self

    def set_evaluation(self, evaluation):
        self.__evaluation_f = evaluation
        self.__evaluate()
        self.__sorted = False
        self.__best_fitness = -inf
        return self

    def set_generation(self, generation):
        self.__generation_f = generation
        return self

    def set_cross(self, cross):
        self.__cross_f = cross
        return self

    def set_mutation(self, mutation):
        self.__mutation_f = mutation
        return self

    def set_selection(self, selection):
        self.__selection_f = selection
        return self

    def set_mutation_ratio(self, mut_ratio):
        self.__mut_ratio = mut_ratio
        return self

    def set_survival(self, survival):
        self.__survival = survival
        return self

    def set_death(self, death):
        self.__death_f = death
        return self

    def set_control(self, control):
        self.__control_obj = copy.deepcopy(control)
        return self

    """ COLONY FUNCTIONS """

    def __sort_population(self):
        if not self.__sorted and self.__population:
            self.__population.sort(reverse=True)
            self.__sorted = True
            self.__best_fitness = self.__population[0][0]

    def __evaluate(self, ilist=None):
        if self.__control_obj:
            if ilist:
                self.__population = [(self.__evaluation_f(i, self.__control_obj), i)
                                     for i in ilist]
            else:
                self.__population = [(self.__evaluation_f(y, self.__control_obj), y)
                                     for _, y in self.__population]
        else:
            if ilist:
                self.__population = [(self.__evaluation_f(i), i)
                                     for i in ilist]
            else:
                self.__population = [(self.__evaluation_f(y), y)
                                     for _, y in self.__population]

    def __evaluate_return(self, ilist):
        if self.__control_obj:
            return [(self.__evaluation_f(i, self.__control_obj), i) for i in ilist]
        else:
            return [(self.__evaluation_f(i), i) for i in ilist]

    def get_population(self, fitness=True):
        if fitness:
            return self.__population
        else:
            return list(map(lambda x: x[1], self.__population))

    def populate(self, members):
        if isinstance(self.__generation_f, int):
            self.__evaluate(ilist=[f.generation_boolean(self.__generation_f)
                                   for _ in range(members)])
        elif isinstance(self.__generation_f, str):
            lenght = int(self.__generation_f)
            self.__evaluate(ilist=[f.generation_string(lenght)
                                   for _ in range(members)])
        else:
            if self.__control_obj:
                self.__evaluate([self.__generation_f(self.__control_obj) for _ in range(members)])
            else:
                self.__evaluate([self.__generation_f() for _ in range(members)])
        self.__colony_size = members
        self.__rankval = int((self.__colony_size * (self.__colony_size + 1) / 2) - self.__colony_size)
        self.__sorted = False
        self.__best_fitness = -inf
        return self

    def get_best(self, *args):
        if self.__colony_size == 0:
            return -inf, None
        self.__sort_population()
        if args:
            return self.__population[:min(args[0], self.__colony_size)]
        else:
            return self.__population[0]

    """ EVOLUTION FUNCTIONS """

    def evolve_reverse(self, iterations, objective=inf, wait=-1):
        tevolve = time()
        if self.__colony_size > 0 and self.__best_fitness<objective:
            # calculate how generation works
            cut_point = int(self.__survival*self.__colony_size)
            num_sons_pairs = int((1-self.__survival)*self.__colony_size/2)
            if 2*num_sons_pairs+cut_point < self.__colony_size:
                num_sons_pairs += 1
            # control variables
            best = -inf
            static = 0
            self.last_iters = 0
            for _ in range(iterations):
                parents = self.__select_parents(num_sons_pairs)
                newborns = self.__cross_parents(parents)
                # flat newborns list
                newborns = [item for sublist in newborns for item in sublist]
                newborns = self.__mutate_newborns(newborns)
                # first we extinct the population, then add newborns
                self.__population_survival(cut_point)
                # after population death append evaluated newborns
                self.__population += self.__evaluate_return(newborns)
                self.__sorted = False
                # stop control
                self.last_iters += 1
                if self.__best_fitness >= objective:
                    break
                if self.__best_fitness > best:
                    best = self.__population[0][0]
                    static = 0
                if static == wait:
                    break
        return self

    def __select_parents(self, new_pool_size: int) -> list:
        # parents: list of tuples (parent1, parent2),
        # where parents = (fitness, element)
        parents = None
        if isinstance(self.__selection_f, str):
            if self.__selection_f == 'roulette':
                total_fitness = sum([x[0] for x in self.__population])
                parents = f.selection_roulette(self.__population, total_fitness, new_pool_size)
            elif self.__selection_f == 'ranked':
                self.__sort_population()
                parents = f.selection_ranked(self.__population, new_pool_size, self.__rankval)
            elif self.__selection_f == 'random':
                parents = f.selection_random(self.__population, new_pool_size)
            elif self.__selection_f == 'pseudo':
                self.__sort_population()
                parents = f.selection_pseudo(self.__population, new_pool_size)
        elif callable(self.__selection_f):
            parents = self.__selection_f(self.__population, new_pool_size)
        return parents

    def __cross_parents(self, parents: list) -> list:
        # parents: list of tuples (parent1, parent2),
        # where parents = (fitness, element)
        # newborns: list of tuples (element1, element2)
        newborns = None
        if isinstance(self.__cross_f, str):
            if self.__cross_f == 'singlepoint':
                newborns = [f.cross_single_point(parent1[1], parent2[1]) for parent1, parent2 in parents]
            elif self.__cross_f == 'twopoint':
                newborns = [f.cross_two_point(parent1[1], parent2[1]) for parent1, parent2 in parents]
            elif self.__cross_f == 'uniform':
                newborns = [f.cross_uniform(parent1[1], parent2[1]) for parent1, parent2 in parents]
        elif callable(self.__cross_f):
            newborns = [self.__cross_f(parent1[1], parent2[1]) for parent1, parent2 in parents]
        return newborns

    def __mutate_newborns(self, newborns: list) -> list:
        # newborns: they come in a simple list
        mutants = None
        if newborns:
            if isinstance(self.__mutation_f, str):
                if self.__mutation_f == 'binary':
                    if newborns and isinstance(newborns[0], str):
                        mutants = [f.mutation_string(n, self.__mut_ratio) for n in newborns]
                    else:
                        mutants = [f.mutation_binary(n, self.__mut_ratio) for n in newborns]
                elif self.__mutation_f == 'boolean':
                    mutants = [f.mutation_boolean(n, self.__mut_ratio) for n in newborns]
            elif callable(self.__mutation_f):
                if self.__control_obj:
                    mutants = [self.__mutation_f(self.__control_obj, n, self.__mut_ratio) for n in newborns]
                else:
                    mutants = [self.__mutation_f(n, self.__mut_ratio) for n in newborns]
        return mutants

    def __population_survival(self, target_size: int) -> None:
        if isinstance(self.__survival_f, str):
            if self.__survival_f == 'roulette':
                self.__population = f.survival_roulette(self.__population, target_size)
            elif self.__survival_f == 'ranked':
                self.__sort_population()
                self.__population = f.survival_ranked(self.__population, target_size)
            elif self.__survival_f == 'simple':
                self.__sort_population()
                self.__population = f.simple_survival(self.__population, target_size)
            elif self.__survival_f == 'pseudo':
                self.__sort_population()
                self.__population = f.survival_pseudo(self.__population, target_size)
        elif callable(self.__survival_f):
            self.__population = self.__survival_f(self.__population, target_size)

    """

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
            print('elitism {} new_pool_size {} generational_survivors {}'.format(self.elitism,new_pool_size,generational_survivors))
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
        """