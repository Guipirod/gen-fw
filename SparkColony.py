
from math import inf
import colony_functions as f


class SparkColony:

    def __init__(self, spark_context, evaluation, generation, cross='singlepoint', mutation='binary',
                 selection='roulette', survival='roulette', mut_ratio=0.05, survival_ratio=0.2, population=None,
                 control_obj=None):
        # spark context
        global sc
        sc = spark_context
        # control object
        global control
        control = spark_context.broadcast(control_obj) if control_obj else None
        # rank value
        global rankval
        rankval = 0
        # auxiliar counter
        # allows the colony to avoid the sort function if it is already sorted
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

    def spark_map(self, function, population):
        global sc
        return sc.parallelize(population).map(function).collect()

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

    def set_survival_ratio(self, survival):
        self.__survival = survival
        return self

    def set_survival(self, survival):
        self.__survival_f = survival
        return self

    def set_control(self, control_obj):
        global control
        global sc
        control = sc.broadcast(control_obj) if control_obj else None
        return self

    """ COLONY FUNCTIONS """

    def __evaluate(self, ilist=None):
        global sc
        global control
        if control:
            if ilist:
                self.__population = sc.parallelize(ilist).map(
                    lambda x: (self.__evaluation_f(x, control.value), x)).collect()
            else:
                self.__population = sc.parallelize(self.__population).map(
                    lambda x: (self.__evaluation_f(x, control.value), x)).collect()
        else:
            if ilist:
                self.__population = sc.parallelize(ilist).map(lambda x: (self.__evaluation_f(x), x)).collect()
            else:
                self.__population = sc.parallelize(self.__population).map(
                    lambda x: (self.__evaluation_f(x), x)).collect()

    def __evaluate_return(self, ilist):
        global sc
        global control
        if control:
            return sc.parallelize(ilist).map(lambda x: (self.__evaluation_f(x, control.value), x)).collect()
        else:
            return sc.parallelize(ilist).map(lambda x: (self.__evaluation_f(x), x)).collect()

    def __sort_population(self):
        if not self.__sorted:
            self.__population.sort(reverse=True)
            self.__sorted = True
            self.__best_fitness = self.__population[0][0]

    def get_population(self, fitness=True):
        global sc
        if fitness:
            return self.__population
        else:
            return sc.parallelize(self.__population).map(lambda x: x[1]).collect()

    def populate(self, members):
        global control
        if isinstance(self.__generation_f, int):
            self.__evaluate(ilist=[f.generation_boolean(self.__generation_f)
                                   for _ in range(members)])
        elif isinstance(self.__generation_f, str):
            lenght = int(self.__generation_f)
            self.__evaluate(ilist=[f.generation_string(lenght)
                                   for _ in range(members)])
        else:
            if control:
                self.__evaluate([self.__generation_f(control.value) for _ in range(members)])
            else:
                self.__evaluate([self.__generation_f() for _ in range(members)])
        self.__colony_size = members
        global rankval
        rankval = int((self.__colony_size * (self.__colony_size + 1) / 2) - self.__colony_size)
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

    def evolve(self, iterations, objective=inf, wait=-1):
        if self.__colony_size > 0 and self.__best_fitness < objective:
            # calculate how generation works
            cut_point = int(self.__survival * self.__colony_size)
            num_sons_pairs = int((1 - self.__survival) * self.__colony_size / 2)
            if 2 * num_sons_pairs + cut_point < self.__colony_size:
                num_sons_pairs += 1
            # control variables
            best = -inf
            static = 0
            for _ in range(iterations):
                parents = self.__select_parents(num_sons_pairs)
                newborns = self.__cross_parents(parents)
                # flat newborns list
                newborns = [item for sublist in newborns for item in sublist]
                newborns = self.__mutate_newborns(newborns)
                # in this version first we extinct the population, then add newborns
                self.__population_survival(cut_point)
                # after population death append evaluated newborns
                self.__population += self.__evaluate_return(newborns)
                self.__sorted = False
                # stop control
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
                global control
                if control:
                    mutants = [self.__mutation_f(control, n, self.__mut_ratio) for n in newborns]
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
