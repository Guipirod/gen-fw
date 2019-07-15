
from time import time
from math import inf
from Colony import Colony
from pyspark.mllib.clustering import KMeans
from pyspark.mllib.clustering import GaussianMixture


class Master:

    def __init__(self, sc, cluster_method='migration', cluster_iterations=30, clustering=90, cache=50, migration=10):
        # OJO: cluster_method y clustering pueden ser empleadas para definir migracion
        # spark variables
        self.__sc = sc
        # clustering method
        self.__cluster_method = cluster_method
        # clustering variables
        self.__cluster_iterations = cluster_iterations
        self.__clustering = clustering
        self.__cache = cache
        # resilient distributed dataset
        self.__rdd = None
        # best fitness result
        self.__best_fitness = -inf
        # experimental
        self.clustering_time = 0
        self.__migration = migration  # total moving population
        # empty, will be initialized on the populate function
        self.__evaluation = None
        self.__generation = None
        self.__population = None
        self.__colonies = None
        self.__cross = None
        self.__mutation = None
        self.__selection = None
        self.__survival = None
        self.__mut_ratio = None
        self.__survival_ratio = None
        self.__control_obj = None

    """ CLASS METHODS """

    def __getitem__(self, i):
        if self.__rdd:
            return self.__rdd.collect()[i]
        return None

    def __len__(self):
        if self.__rdd:
            return self.__rdd.count()
        return None

    """ CONTROL FUNCTIONS """

    def populate(self, evaluation, generation, population, colonies,
                 cross='singlepoint', mutation='binary', selection='roulette',
                 survival='roulette', migration=10,
                 mut_ratio=0.05, survival_ratio=0.2, control_obj=None):
        # stored for clustering methods
        self.__evaluation = evaluation
        self.__generation = generation
        self.__population = population
        self.__colonies = colonies
        self.__cross = cross
        self.__mutation = mutation
        self.__selection = selection
        self.__survival = survival
        self.__mut_ratio = mut_ratio
        self.__survival_ratio = survival_ratio
        self.__control_obj = control_obj
        self.__migration = migration
        if control_obj:
            self.__sc.broadcast(control_obj)
        # creates a rdd with a given number of colonies and individuals
        self.__rdd = self.__sc.parallelize([Colony(evaluation, generation, cross=cross, mutation=mutation,
                                                   selection=selection, mut_ratio=mut_ratio,
                                                   survival_ratio=survival_ratio, survival=survival,
                                                   control_obj=control_obj) for _ in range(colonies)])
        self.__rdd = self.__rdd.map(lambda x: x.populate(population))

    def update(self, control_obj, population):
        self.__rdd = self.__rdd.map(lambda x: x.set_control(control_obj)).map(lambda x: x.populate(population))

    def get_best(self, *args):
        t = time()  # [DEBUG]
        result = None
        if self.__rdd:
            if args:
                result = self.__rdd.map(lambda x: x.get_best(*args)).collect()
                result = [item for sublist in result for item in sublist]
                result = sorted(result, reverse=True)[:args[0]]
                self.__best_fitness = result[0][0]
            else:
                result = sorted(self.__rdd.map(lambda x: x.get_best()).collect(), reverse=True)[0]
                self.__best_fitness = result[0]
        print("[DEBUG] - Time in seconds: " + str(time() - t))  # [DEBUG]
        return result

    def evolve(self, iterations, objective=inf, wait=-1, granular=False, collect=False):
        if isinstance(iterations, int):
            iterations = self.__get_execution_list(iterations)
        for instruction in iterations:
            if self.__best_fitness < objective:
                print(str(instruction) + '  ' + str(type(instruction)))
                if isinstance(instruction, int):
                    intval = instruction
                    if granular:
                        print('[DEBUG] - evolve granular')  # [DEBUG]
                        for _ in range(instruction):
                            self.__rdd = self.__rdd.map(lambda x: x.evolve(1, objective=objective, wait=wait))
                    else:
                        print('[DEBUG] - evolve iterate')  # [DEBUG]
                        self.__rdd = self.__rdd.map(lambda x: x.evolve(intval, objective=objective, wait=wait))
                    if collect:
                        self.__best_fitness = sorted(self.__rdd.map(lambda x: x.get_best()[0]).collect(),
                                                     reverse=True)[0]
                elif isinstance(instruction, str):
                    if instruction == 'migration':
                        t1 = time()
                        print('[DEBUG] - migration')  # [DEBUG]
                        self.__migration_function()
                        self.clustering_time += time() - t1
                    elif instruction == 'kmeans':
                        t1 = time()
                        print('[DEBUG] - kmeans_clustering')  # [DEBUG]
                        self.__kmeans_clustering()
                        self.clustering_time += time() - t1
                    elif instruction == 'gauss':
                        t1 = time()
                        print('[DEBUG] - gauss_clustering')  # [DEBUG]
                        self.__gauss_clustering()
                        self.clustering_time += time() - t1
                    elif instruction == 'cache':
                        self.__rdd.cache()

    def cache(self):
        self.__rdd.cache()

    def __get_execution_list(self, iterations):
        tickets = [[0, 'init']]
        for i in range(1, iterations):
            if self.__clustering > 0 and i % self.__clustering == 0:
                tickets.append([i, self.__cluster_method])
            if self.__cache > 0 and i % self.__cache == 0:
                tickets.append([i, 'cache'])
        i = iterations
        result = []
        for ticket in list(reversed(tickets)):
            result.append([ticket[1], i - ticket[0]])
            i = ticket[0]
        return [y for x in reversed(result) for y in x]

    def __migration_function(self):
        tmp_migration = self.__migration
        best_ones = self.__rdd.flatMap(lambda x: x.get_best(tmp_migration)).collect()
        best_ones = sorted(best_ones, reverse=True)[:self.__migration]
        self.__rdd = self.__rdd.map(lambda x: x.income(best_ones))

    def __kmeans_clustering(self):
        # get the whole population without fitness value, then flat it
        rdd_aux = self.__rdd.flatMap(lambda x: x.get_population(fitness=False))
        # train the kmeans
        kmeans_cluster = KMeans.train(rdd_aux, self.__colonies,
                                      maxIterations=self.__cluster_iterations,
                                      initializationMode="random")  # acepta "k-means||"
        # create a new rdd with the labels
        rdd_labels = kmeans_cluster.predict(rdd_aux)
        # zip each result with its class
        rdd_aux = rdd_labels.zip(rdd_aux)
        # input serialization
        cols = self.__colonies
        self.__sc.broadcast(cols)
        # divide into partitions
        rdd_aux = rdd_aux.partitionBy(cols,
                                      partitionFunc=lambda x: x).glom()
        # remove the index of each element
        rdd_aux = rdd_aux.map(lambda x: [y[1] for y in x])
        # input serialization
        evaluation = self.__evaluation
        generation = self.__generation
        cross = self.__cross
        mutation = self.__mutation
        selection = self.__selection
        survival = self.__survival
        mut_ratio = self.__mut_ratio
        survival_ratio = self.__survival_ratio
        control_obj = self.__control_obj
        # create the new colonies
        self.__rdd = rdd_aux.map(lambda x: Colony(evaluation, generation, cross=cross, mutation=mutation,
                                                  selection=selection, mut_ratio=mut_ratio,
                                                  survival_ratio=survival_ratio, survival=survival,
                                                  control_obj=control_obj, population=x))

    def __gauss_clustering(self):
        # get the whole population without fitness value, then flat it
        rdd_aux = self.__rdd.flatMap(lambda x: x.get_population(fitness=False))
        # train the gauss cluster
        gauss_cluster = GaussianMixture.train(rdd_aux, self.__colonies,
                                              maxIterations=self.__cluster_iterations)
        # create a new rdd with the labels
        rdd_labels = gauss_cluster.predict(rdd_aux)
        # zip each result with its class
        rdd_aux = rdd_labels.zip(rdd_aux)
        # input serialization
        cols = self.__colonies
        self.__sc.broadcast(cols)
        # divide into partitions
        rdd_aux = rdd_aux.partitionBy(cols,
                                      partitionFunc=lambda x: x).glom()
        # remove the index of each element
        rdd_aux = rdd_aux.map(lambda x: [y[1] for y in x])
        # input serialization
        evaluation = self.__evaluation
        generation = self.__generation
        cross = self.__cross
        mutation = self.__mutation
        selection = self.__selection
        survival = self.__survival
        mut_ratio = self.__mut_ratio
        survival_ratio = self.__survival_ratio
        control_obj = self.__control_obj
        # create the new colonies
        self.__rdd = rdd_aux.map(lambda x: Colony(evaluation, generation, cross=cross, mutation=mutation,
                                                  selection=selection, mut_ratio=mut_ratio,
                                                  survival_ratio=survival_ratio, survival=survival,
                                                  control_obj=control_obj, population=x))
