

class TaskNode(object):
    def __init__(self, name, prev_nodes, func, func_param):
        self.name = name
        self.prev_nodes = prev_nodes

        self._func = func
        self._func_param = func_param


    def __call__(self, memory):
        # check output of dependent nodes
        for prev_node in self.prev_nodes:
            if memory.check_executed(prev_node.name):
                continue

            assert('Should not reach here' and False)

        args = self._func_param.get('args', ())
        kwargs = self._func_param.get('kwargs', {})

        output = self._func(*args, **kwargs)
        memory.set_output(self.name, output)


