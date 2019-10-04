

class TaskNode(object):
    def __init__(self, name, prev_nodes, func, func_param):
        self.name = name
        self.prev_nodes = prev_nodes

        self._func = func
        self._func_param = func_param

    def __repr__(self):
        return self.name

    def __call__(self, prev_outputs):
        # check output of dependent nodes
        for prev_node in self.prev_nodes:
            if prev_node.name in prev_outputs:
                continue
            assert('Should not reach here' and False)

        args = self._func_param.get('args', ())
        kwargs = self._func_param.get('kwargs', {})

        assert('prev_outputs' not in kwargs)
        kwargs['prev_outputs'] = prev_outputs

        output = self._func(*args, **kwargs)

        return output


