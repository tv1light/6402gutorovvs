from setuptools import setup, find_packages

setup(
    name='data_analysis',
    version='0.1.0',
    author='twilight',
    author_email='vlodik.guterian@gmail.com',
    description='Пакет для анализа временных рядов погоды',
    packages=find_packages(),
    install_requires=[
        'pandas',
        'meteostat',
        'matplotlib',
        'openpyxl',
    ],
    classifiers=[
        'Programming Language :: Python :: 3',
        'Operating System :: OS Independent',
    ],)