
class TaskMemory(object):
    def __init__(self):
        self._outputs = {}

    def set_output(self, name, output):
        self._outputs[name] = output

    def get_output(self, name):
        return self._outputs.get(name, {})

    def check_executed(self, name):
        return self._outputs.get(name, None) is not None


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

            next_node(self._task_memory)

            del self._be_executed[next_node.name]

    def _select_undependent_node(self):
        selected_node = None
        for node in self._be_executed.values():

            check_dependents = True
            for prev_node in node.prev_nodes:
                if self._task_memory.check_executed(prev_node.name):
                    continue

                check_dependents = False
                break

            if not check_dependents:
                continue
            
            selected_node = node
            break

        return selected_node

    def _generate_proc_graph(self, target_node):
        # register all dependent nodes
        for node in target_node.prev_nodes:
            if self._be_executed.get(node.name, None):
                continue

            self._generate_proc_graph(node)

        self._be_executed[target_node.name] = target_node


