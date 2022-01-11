from collections import deque


# 列表实现栈的功能
def list_to_stack():
    stack = [3, 4, 5]
    stack.append(6)
    stack.append(7)
    print(stack)

    stack.pop()
    stack.pop()
    print(stack)


# 列表实现队列功能
def list_to_queue():
    queue = deque(["Eric", "John", "Michael"])
    queue.append("terry")
    queue.append("gmi")
    print(queue)

    queue.popleft()
    queue.popleft()
    print(queue)


# 列表推导式
def list_derivative():
    squares = []
    for x in range(10):
        squares.append(x)

    print(squares)


# 列表推导式综合使用
def list_derivative1():
    squares = [(x, y) for x in [1, 2, 3] for y in [3, 1, 4] if x != y]
    print(squares)


if __name__ == '__main__':
    # list_to_stack()
    # list_to_queue()
    list_derivative1()
