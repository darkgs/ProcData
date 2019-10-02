
from proc_data.task_node import TaskNode
from proc_data.task_manager import TaskManager

def foo(*args, **kwargs):
    print('Do task {}!'.format(args[0]))

    return args[0]


def main():
    t1 = TaskNode('task1', [], foo, {'args':[1]})
    t2 = TaskNode('task2', [], foo, {'args':[2]})
    t3 = TaskNode('task3', [t1, t2], foo, {'args':[3]})

    tm = TaskManager()
    tm.exec_node(t3)


if __name__ == '__main__':
    main()
