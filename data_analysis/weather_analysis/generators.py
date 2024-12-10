from typing import Iterator


def lag_generator() -> Iterator[int]:
    """
    Генератор, производит последовательные целые числа, начиная с 1.

    Returns:
        Iterator[int]: Генератор, который возвращает последовательные целые числа
    """
    i = 1
    while True:
        yield i
        i += 1
