
import os

import inspect
import hashlib
import pickle

from proc_data.utils import write_log

class TaskMemory(object):
    def __init__(self, cache_dir='cache'):
        # Store outputs of tasks
        self._outputs = {}

        # Generate cache dir if not exist
        self._cache_dir = cache_dir
        if not os.path.exists(cache_dir):
            os.system('mkdir -p {}'.format(cache_dir))

        # Cache information
        self.load_cache_info()

    def get_cache_info_path(self):
        return os.path.join(self._cache_dir, 'cache.p_info')

    def load_cache_info(self):
        cache_info_path = self.get_cache_info_path()

        if not os.path.exists(cache_info_path):
            self._cache_info = {
                'signitures': {},
            }
            return

        with open(cache_info_path, 'rb') as f_info:
            self._cache_info = pickle.load(f_info)

    def update_cache_info(self, node):
        # update signiture of the node
        self._cache_info['signitures'][node.name] = self.get_node_signiture(node)

        # update cache info file
        cache_info_path = self.get_cache_info_path()
        with open(cache_info_path, 'wb') as f_info:
            pickle.dump(self._cache_info, f_info)
    
    def remove_cache_info(self, node):
        if node.name in self._cache_info['signitures']:
            del self._cache_info['signitures'][node.name]

        # update cache info file
        cache_info_path = self.get_cache_info_path()
        with open(cache_info_path, 'wb') as f_info:
            pickle.dump(self._cache_info, f_info)

    def is_output_loaded(self, node):
        return node.name in self._outputs

    def get_cache_path(self, node):
        return os.path.join(self._cache_dir, node.name + '.p')

    def is_valid_cache(self, node):
        cache_path = self.get_cache_path(node)
        if not os.path.exists(cache_path):
            return False

        if self._cache_info['signitures'].get(node.name, '') != self.get_node_signiture(node):
            return False

        is_valid = True
        for prev_node in node.prev_nodes:
            if not self.is_valid_cache(prev_node):
                is_valid = False
                break

        return is_valid

    def remove_cache(self, node):
        cache_path = self.get_cache_path(node)

        if os.path.exists(cache_path):
            os.system('rm -rf {}'.format(cache_path))

        self.remove_cache_info(node)

    def load_cache(self, node):
        if not self.is_valid_cache(node):
            self.remove_cache(node)
            return False

        cache_path = self.get_cache_path(node)

        with open(cache_path, 'rb') as f_cache:
            self._outputs[node.name] = pickle.load(f_cache)

        return True

    def get_node_signiture(self, node):
        return hashlib.md5(inspect.getsource(node._func).encode('utf-8')).hexdigest()

    def store_output(self, node, output):
        cache_path = self.get_cache_path(node)

        with open(cache_path, 'wb') as f_cache:
            pickle.dump(output, f_cache)
        
        self.update_cache_info(node)

        self._outputs[node.name] = output


class TaskManager(object):
    def __init__(self):
        self._task_memory = TaskMemory()

        self._be_executed = {}

    def exec_node(self, node):
        self._generate_proc_graph(node)

        while len(self._be_executed) > 0:
            next_node = self._select_undependent_node()

            # An undependent node must exist
            assert(next_node)

            output = next_node(self._task_memory._outputs)
            self._task_memory.store_output(next_node, output)

            del self._be_executed[next_node.name]

    def _select_undependent_node(self):
        selected_node = None
        for node in self._be_executed.values():

            check_dependents = True
            for prev_node in node.prev_nodes:
                if self._task_memory.is_output_loaded(prev_node):
                    continue

                check_dependents = False
                break

            if check_dependents:
                selected_node = node
                break

        return selected_node

    def _generate_proc_graph(self, target_node):
        if self._task_memory.load_cache(target_node):
            return

        # register all dependent nodes
        for node in target_node.prev_nodes:
            if self._be_executed.get(node.name, None):
                continue

            self._generate_proc_graph(node)

        self._be_executed[target_node.name] = target_node


