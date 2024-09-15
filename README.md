# Лабораторная работа №1 #


**Задание** <br />
Вычислить значение функции *y* в диапазоне <br />
*x* = [от n0, с шагом h, до nk], результаты записать в файл _results_. 
Переменные должны считываться из файла config, формат файла определяется согласно варианту задания.<br />
Для считывания данных из config реализовать парсер файла.<br />
Предусмотреть возможность задание параметров 
как аргументов запускаемого py файла через консоль.<br />
Считываемые данные из конфиг файла: n0, h, nk, a, b, c. Можете дополнить по своему усмотрению.<br />

**Вариант №6**
```
Функция: y(x) = a / (1 + e ** (-b * x + c))
Формат конфига: .txt
```


**Инструкция по работе с файлом config.txt** <br />
В файл, через разделитель (пробел или новая строка) записываются следующие параметры:<br />
n0 - левая граница вычислений; <br />
h - шаг вычислений; <br />
nk - правая граница вычислений; <br />
a - парметр функции "a"; <br />
b - парметр функции "b"; <br />
c - парметр функции "с" <br />

 