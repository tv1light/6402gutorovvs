import math
import numpy as np
import argparse


def parse_args_from_file(file_path: str) -> list[float]:
    """
    Parsing arguments from file

    :param file_path: path to file with arguments
    :return: list of floats, where
    list[0] - left border of calculations;
    list[1] - step of calculations;
    list[2] - right border of calculations;
    list[3] - 'a' param of function;
    list[4] - 'b' param of function;
    list[5] - 'c' param of function
    """
    # Opening file in reading mode
    with open(file_path, 'r') as f:
        # Splitting string and conversion each number to float
        arguments = list(map(float, f.read().split()))
    return arguments


def function(n0: float, h: float, nk: float, a: float, b: float, c: float) -> list[tuple[float, float]]:
    """
    Calculating function of y(x) = a / (1 + e ** (-b * x + c))

    :param n0: Left border of calculations
    :param h: Step of calculations
    :param nk: Right border of calculations
    :param a: 'a' function parameter
    :param b: 'b' function parameter
    :param c: 'c' function parameter
    :return: list of tuples, where ['x' point, calculated 'y' point]
    """
    result = []
    # Walk through the range between n0 and nk with step h
    # and calculating function
    for i in np.arange(n0, nk, h):
        y = a / (1 + math.exp(-b * i + c))
        result.append((i, y))
    return result


def main(args: list[float]) -> None:
    """
    Everything starts and ends here :-)

    :param args: List of arguments where
    args[0] - left border of calculations;
    args[1] - step of calculations;
    args[2] - right border of calculations;
    args[3] - 'a' param of function;
    args[4] - 'b' param of function;
    args[5] - 'c' param of function
    :return: None
    """
    # Splitting args
    n0, h, nk, a, b, c = args[0], args[1], args[2], args[3], args[4], args[5]
    results = function(n0, h, nk, a, b, c)
    # Opening/creating file and writing our results
    with open('output.txt', 'w') as f:
        for x, y in results:
            f.write(f"x: {x:.3f}, y: {y:.3f}\n")


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Function calculation')

    parser.add_argument("--from-file", type=str, help="File containing arguments")
    args, unknown = parser.parse_known_args()

    if args.from_file:
        # Parsing args from file
        print("Parsing arguments from file")

        args = parse_args_from_file(args.from_file)
    else:
        # Parsing arguments from command line
        print("Parsing arguments from command line")

        parser.add_argument('n0', help='Input left border')
        parser.add_argument('h', help='Input step')
        parser.add_argument('nk', help='Input right border')
        parser.add_argument('a', help='Input a')
        parser.add_argument('b', help='Input b')
        parser.add_argument('c', help='Input c')

        args = parser.parse_args()
        args = list(map(float, (args.n0, args.h, args.nk, args.a, args.b, args.c)))

    main(args)
