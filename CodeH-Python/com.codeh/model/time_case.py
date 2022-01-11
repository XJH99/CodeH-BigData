from datetime import datetime, timedelta


def getTime():
    """
    utc time conversion
    :return:
    """
    time = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=0)
    print(time)


if __name__ == '__main__':
    getTime()
