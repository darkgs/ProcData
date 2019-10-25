
from proc_data.task_node import TaskNode
from proc_data.task_manager import TaskManager

def foo1(tm, *args, **kwargs):
    print(tm)
    print('Do task {}!'.format(args[0]))

    return args[0]

def foo2(tm, *args, **kwargs):
    print('Do task {}!'.format(args[0]))

    return args[0]

def foo3(tm, *args, **kwargs):
    prev_outputs = tm.get_prev_outputs()
    print('Output of t1: {}'.format(prev_outputs['task1']))
    print('Output of t2: {}'.format(prev_outputs['task2']))

    print('Do task {}!'.format(args[0]))
    return args[0]


def main():
    t1 = TaskNode('task1', [], foo1, {'args':[1], 'kwargs':{'b':1, 'a':100}})
    t2 = TaskNode('task2', [], foo2, {'args':[2]})
    t3 = TaskNode('task3', [t1, t2], foo3, {'args':[3]})

    tm = TaskManager()
    tm.exec_node(t3)


if __name__ == '__main__':
    main()
