# -*- coding: utf-8 -*-
"""
Created on Thu Apr 11 07:38:32 2019

@author: guipirod
"""

from random import randint as rdi
from random import random as rd
from random import choice


class PathControl4:

    def __init__(self, *args, **kargs):
        consistency = kargs['consistency'] if 'consistency' in kargs else 0
        depth = kargs['depth'] if 'depth' in kargs else 4
        if len(args) == 3:
            file_name, in_paths, in_packages = args
            self.__max_depth = depth
            self.__take_n_paths(file_name, in_paths, consistency)
            self.num_paths = in_paths
            self.__add_time()
            self.__get_links()
            self.create_packages(in_packages)
        else:
            raise Exception('You must initialize a population in order to evolve')

    """ INITIALIZE PATHS """

    def __take_n_paths(self, file_name, num_paths, consistency):
        path_list = open('pathmaker/' + file_name, 'r').read().split('\n')
        limit = len(path_list) - 1
        # take a random group of paths
        paths = [path_list[rdi(0, limit)].split(',') for _ in range(num_paths)]
        # convert str lists to int lists
        self.paths = [list(map(int, p)) for p in paths]
        '''TO DO : ADD CONSISTENCY CHECK'''

    def __add_time(self):
        for i in range(self.num_paths):
            arrival, departure = self.__get_time(len(self.paths[i]), 15)
            self.paths[i] = (self.paths[i], arrival, departure)

    def __get_time(self, n, interval):
        # hour = rdi(6,20)*100 #minute = rdi(0,59)
        # a day has 1440 minutes, the minute 360 marks 6 am, and the
        # minute 1200 marks 8 pm
        itime = rdi(360, 480)
        arrival, departure = [], []
        for _ in range(n):
            arrival.append(itime)
            itime += interval
            departure.append(itime)
        return (arrival, departure)

    """ LINK INITIALIZATION """

    def __get_links(self):
        self.links = {}
        for i in range(self.num_paths):
            for j in range(i + 1, self.num_paths):
                # print('crossing {} with {}'.format(i,j))
                self.__cross(i, j)

    def __cross(self, p1, p2):
        # compares two paths determinated by the pointers
        for i in [value for value in self.paths[p1][0] if value in self.paths[p2][0]]:
            ip1 = self.paths[p1][0].index(i)
            ip2 = self.paths[p2][0].index(i)
            if self.paths[p1][2][ip1] >= self.paths[p2][1][ip2] and self.paths[p2][2][ip2] >= self.paths[p1][1][ip1]:
                # print('camino {} cruza con {} en {}'.format(p1,p2,ip1))
                # print('camino {} cruza con {} en {}'.format(p2,p1,ip2))
                self.__introduce_result(p1, p2, ip1)
                self.__introduce_result(p2, p1, ip2)

    def __introduce_result(self, path1, path2, point):
        if path1 in self.links:
            if path2 in self.links[path1]:
                self.links[path1][path2].append(point)
            else:
                self.links[path1][path2] = [point]
        else:
            self.links[path1] = {path2: [point]}

    def get_consistency(self):
        count = 0
        for path_pointer in self.links:
            for connection in self.links[path_pointer]:
                if connection > path_pointer:
                    count += 1
        return count / self.num_paths

    """ PACKAGE CREATION """

    def create_packages(self, num_packages):
        self.num_packages = num_packages
        self.in_points = {}
        self.end_points = {}
        # helper
        self.__taken_paths = []
        self.pack_routes = []  # DEBUG
        self.package = []
        for i in range(num_packages):
            package_paths = self.__build_route()
            self.pack_routes.append(package_paths)  # DEBUG
            # TOMAMOS PRIMER Y ULTIMO PUNTO DE LOS CAMINOS GENERADOS
            deliver_from = self.paths[package_paths[0]][0][0]
            deliver_to = self.paths[package_paths[-1]][0][-1]
            self.__init_points(i, deliver_from, deliver_to)
            self.package.append((deliver_from, deliver_to))  # DEBUG
        del self.__taken_paths

    def __init_points(self, package_index, deliver_from, deliver_to):
        self.in_points[package_index] = {}
        self.end_points[package_index] = {}
        for paths_index in range(len(self.paths)):
            if deliver_from in self.paths[paths_index][0]:
                index = self.paths[paths_index][0].index(deliver_from)
                self.in_points[package_index][paths_index] = index
            if deliver_to in self.paths[paths_index][0]:
                index = self.paths[paths_index][0].index(deliver_to)
                self.end_points[package_index][paths_index] = index

    def __build_route(self):
        current = [self.__get_random_path()]
        initial_point = 0
        stop = .20
        depth = 0
        while rd() > stop and depth < self.__max_depth:
            # keep going
            random_path, initial_point = self.__get_continue_path(current[-1], initial_point)
            if initial_point > -1:
                current.append(random_path)
                stop += .15
                depth += 1
            else:
                stop += 1
        return current

    def __get_random_path(self):
        random_path = rdi(0, self.num_paths - 1)
        while random_path in self.__taken_paths:
            random_path = rdi(0, self.num_paths - 1)
        self.__taken_paths.append(random_path)
        return random_path

    def __get_continue_path(self, initial_path, initial_point):
        if initial_path in self.links.keys():
            path_keys = list(filter(lambda x: x not in self.__taken_paths and
                                              self.links[initial_path][x][-1] > initial_point,
                                    self.links[initial_path].keys()))
            if path_keys:
                # se puede continuar
                random_path = choice(path_keys)
                self.__taken_paths.append(random_path)
                # HE DE VER POR DONDE CARAJO ENTRA
                intersection_point = self.__get_intersection_point(initial_path, random_path, initial_point)
                return random_path, intersection_point  # CAMBIAR
        return None, -1

    def __get_intersection_point(self, path1, path2, start_point):
        # gets the entrance from path1 into path2 closest to 0

        for i in [value for value in self.paths[path1][0] if value in self.paths[path2][0]]:
            ip1 = self.paths[path1][0].index(i)
            if ip1 > start_point:
                ip2 = self.paths[path2][0].index(i)
                if self.paths[path1][2][ip1] >= self.paths[path2][1][ip2] and self.paths[path2][2][ip2] >= \
                        self.paths[path1][1][ip1]:
                    return ip2
        return -1

    """ PACKAGE ARRIVES METHOD """

    def arrives(self, package, paths):
        """
        package - int index of the checked package
        paths - int list of pointers to the self.paths and self.links dicts
        """
        in_paths = [p for p in paths if p in self.in_points[package]]
        end_paths = [p for p in paths if p in self.end_points[package]]
        # BEST CASE: no carrier goes to init or end
        if not (in_paths and end_paths):
            return False
        # SECOND CASE: check if any path takes the package from init to end
        for path in [p for p in in_paths if p in end_paths]:
            if self.in_points[package][path] < self.end_points[package][path]:
                return True
        # WORST CASE: check if it arrives with the given roads
        # middle_paths = [p for p in paths if p not in self.in_points[package] and p not in self.end_points[package]]
        open_paths = [p for p in paths if p not in self.in_points[package]]
        for ip in in_paths:
            if self.__explore(ip, package, open_paths):
                return True
        return False

    def __explore(self, from_path, package, open_paths, start_point=0, depth=0):
        if from_path in self.end_points[package].keys() and self.end_points[package][from_path] > start_point:
            return True
        if depth < self.__max_depth:
            i = 0
            for op in open_paths:
                if from_path in self.links and op in self.links[from_path]:
                    intersection = self.__get_intersection_point(from_path, op, start_point)
                    if self.__explore(op, package, open_paths[:i] + open_paths[i + 1:],
                                      depth=depth + 1, start_point=intersection):
                        return True
                i += 1
        return False

    """ SHORTEST PATH FUNCTION """

    def evolve(self, element):
        cdict = self.__get_carry_dict(element)
        rdict = {}
        taken_paths = []
        for key in range(self.num_packages):
            if key in cdict.keys():
                if self.arrives(key, cdict[key]):
                    sp = self.shortest_path(key, cdict[key])
                    if sp:
                        print("Package {} arrives".format(key))
                        rdict[key] = (self.package[key], [self.paths[i] for i in sp])
                        taken_paths += sp
        # mutate myself

        for key in sorted(taken_paths, reverse=True):
            del self.paths[key]
            self.num_paths -= 1
        self.__get_links()
        for key in sorted(rdict.keys(), reverse=True):
            del self.package[key]
            self.num_packages -= 1

        for i in range(len(self.package)):
            self.__init_points(i, self.package[i][0], self.package[i][1])
        return rdict

    def __who_carries(self, from_path, package, open_paths, start_point=0, depth=0):
        if from_path in self.end_points[package].keys() and self.end_points[package][from_path] > start_point:
            return [from_path]
        if depth < self.__max_depth:
            i = 0
            for op in open_paths:
                if from_path in self.links and op in self.links[from_path]:
                    intersection = self.__get_intersection_point(from_path, op, start_point)
                    carry_aux = self.__who_carries(op, package, open_paths[:i]+open_paths[i+1:],
                                                   depth=depth+1, start_point=intersection)
                    if carry_aux:
                        return [from_path] + carry_aux
        return None

    def shortest_path(self, package, paths):
        # this method considers that the package arrives
        in_paths = [p for p in paths if p in self.in_points[package]]
        # end_paths = [p for p in paths if p in self.end_points[package]]
        open_paths = [p for p in paths if p not in self.in_points[package]]
        shortest = None
        min_depth = self.__max_depth + 1
        for ip in in_paths:
            carriers = self.__who_carries(ip, package, open_paths)
            if carriers and len(carriers) < min_depth:
                min_depth = len(carriers)
                shortest = carriers
        return shortest

    # EVALUATE FUNCTION

    def __get_carry_dict(self, element):
        cdict = {}
        i = 0
        for intval in element:
            if intval > -1:
                if intval in cdict.keys():
                    cdict[intval].append(i)
                else:
                    cdict[intval] = [i]
            i += 1
        return cdict

    def evaluate(self, element):
        # element is a list of int values, where the n-element having a value x
        # translates to the user n carrying the package x
        cdict = self.__get_carry_dict(element)
        suma = 0
        for key in range(self.num_packages):
            if key in cdict.keys():
                if self.arrives(key, cdict[key]):
                    suma += 1
                elif len(cdict[key]) > 5:
                    suma -= .1 * min(10, len(cdict[key]) - 5)
            else:
                suma -= 1
        return suma
