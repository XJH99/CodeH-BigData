# 指定默认值,但是也可以通过传递参数的方式覆盖默认值
def ask_ok(prompt, retries=4, reminder='Please try again!'):
    while True:
        ok = input(prompt)
        if ok in ('y', 'ye', 'yes'):
            return True
        if ok in ('n', 'no', 'nop', 'nope'):
            return False
        retries = retries - 1
        print(retries)
        if retries < 0:
            # 抛出一个指定异常
            raise ValueError('invalid user response')
        print(reminder)


def parrot(voltage, state='a stiff', action='voom', type='Norwegian Blue'):
    print("-- This parrot wouldn't", action, end=' ')
    print("if you put", voltage, "volts through it.")
    print("-- Lovely plumage, the", type)
    print("-- It's", state, "!")


# 可以传入列表和字典
def cheeseshop(kind, *arguments, **keywords):
    print("-- Do you have any", kind, "?")
    print("-- I'm sorry, we're all out of", kind)
    for arg in arguments:
        print(arg)
    print("-" * 40)
    for kw in keywords:
        print(kw, ":", keywords[kw])


# lambad表达式
def make_increment(n):
    return lambda x: x + n


# 文档字符串
def doc():
    """

    :return:
    """


if __name__ == '__main__':
    # ask_ok('Do you really want to quit?')
    # ask_ok('OK to overwrite the file?', 2)
    # ask_ok('OK to overwrite the file?', 2, 'Come on, only yes or no!')
    # parrot(1000)
    # parrot(voltage=1000)
    # parrot(voltage=1000000, action='VOOOOOM')
    # parrot('a million', 'bereft of life', 'jump')
    # parrot('a thousand', state='pushing up the daisies')
    # cheeseshop("Limburger", "It's very runny, sir.",
    #            "It's really very, VERY runny, sir.",
    #            shopkeeper="Michael Palin",
    #            client="John Cleese",
    #            sketch="Cheese Shop Sketch")

    f = make_increment(10)
    print(f(10))
