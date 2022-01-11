a = ['Mary', 'had', 'a', 'little', 'lamb']


def range_case():
    for i in range(len(a)):
        print(i, a[i])


def break_case():
    for n in range(2, 10):
        for x in range(2, n):
            if n % x == 0:
                print(n, 'equals', x, '*', n // x)
                break
        else:
            print(n, 'is a prime number')


def continue_case():
    for num in range(2, 10):
        if num % 2 == 0:
            print("Found an even number", num)
            continue
        print("Found an odd number", num)


def fib(n):
    c, b = 0, 1
    while c < n:
        print(c, end=' ')
        # 理解：其实就是=右边的两个值的结果处理成二元组赋值给左边的二元组，一一对应的关系
        c, b = b, c + b
    print()


if __name__ == '__main__':
    # range_case()
    # break_case()
    # continue_case()
    fib(10)
