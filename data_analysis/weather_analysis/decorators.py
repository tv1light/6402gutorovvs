def time_execution(func):
    """
    Декоратор для измерения времени выполнения функции.

    Args:
        func (callable): Функция для декорирования.

    Returns:
        callable: Обернутая функция с измерением времени выполнения.
    """
    import time

    def wrapper(*args, **kwargs):
        """
        Обёртка для функции с замером времени выполнения.

        Args:
            *args: Аргументы для функции.
            **kwargs: Именованные аргументы для функции.

        Returns:
            Any: Результат выполнения функции.
        """
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print(f'Функция {func.__name__} выполнена за {end_time - start_time:.4f} секунд')
        return result

    return wrapper
