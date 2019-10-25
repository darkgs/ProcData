
import inspect
import hashlib

class TaskNode(object):
    def __init__(self, name, prev_nodes, func, func_param):
        self.name = name
        self.prev_nodes = prev_nodes

        self._func = func
        self._func_param = func_param

    def get_func_signiture(self):
        prev_nodes_str = '_'.join(sorted([node.name for node in self.prev_nodes]))
        func_str = inspect.getsource(self._func)

        hash_key = prev_nodes_str + '__' + func_str

        return hashlib.md5(hash_key.encode('utf-8')).hexdigest()

    def get_param_signiture(self):
        def param2str(param):
            if isinstance(param, dict):
                return '_'.join([str(key) + '-' + param2str(param[key]) \
                        for key in sorted(param.keys())])
            elif isinstance(param, list):
                return '_'.join([str(val) for val in param])
            else:
                return str(param)

        args = self._func_param.get('args', [])
        kwargs = self._func_param.get('kwargs', {})

        hash_key = param2str(args) + '__' + param2str(kwargs)

        return hashlib.md5(hash_key.encode('utf-8')).hexdigest()

    def __repr__(self):
        return self.name

    def __call__(self, tm):
        args = self._func_param.get('args', ())
        kwargs = self._func_param.get('kwargs', {}).copy()

        output = self._func(tm, *args, **kwargs)

        return output


