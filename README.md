# Краткое описание решений
Представлено два решения задачи:
1) FindNearestPlace - сопоставление персоны к ближайшему месту, которое она могла бы посетить
2) SuggestUnvisitedPlacesFromSimilarPersons - предложить для каждой персоны места, которые были посещены другими персонами.
В расчете участвуют только те персоны, у которых число посещенных мест >= minimalCountOfPlacesToCalcSimilarityWithOtherPersons
Персона А считаются схожей с В по посещенным местам, если >= 60 процентов мест, посещенных персоной А совпадает с местами персоны В.
Пороговое значение процента схожести определяется параметром similarityThreshold

# Описание задачи
Необходимо написать приложение, которое может предложить информацию о местах. Примеры мест. Архитектурные памятники, музеи, места, где жили известные люди, парки и т.д. 
## Входные данные
### Описание местоположения. 
Данные разбиты по месяцам и большим географическим областям (города).
* Идентификатор персоны
* Время (ггггммдд_чч) (Время с точностью до часа, где прибывала персона)
* Широта (latitude) (Часть координаты пребывания персоны в течении часа. Усредненная)
* Долгота (longitude) (Часть координаты пребывания персоны в течении часа. Усредненная)
* Идентификатор области
* Дата (ггггммдд) (Первый день месяца, за который есть данные)

| Идентификатор персоны | Время (ггггммдд_чч) | Широта | Долгота | Идентификатор области | Дата |
| --- | --- | --- | --- | --- | --- |
| 1 | 20190202_01 | 55.752161 | 37.590964 | 1 | 20190201 |
| 1 | 20190202_10 | 55.747496 | 37.601826 | 1 | 20190201 |
| 2 | 20190201_07 | 55.757904 | 37.597563 | 1 | 20190201 |
### Описание мест. 
Данные разбиты по месяцам и большим географическим областям (города). 
Нужно учесть, что в данных могут быть дубликаты. От дубликатов нужно избавится. К примеру, в названиях могут быть опечатки.
* Идентификатор места
* Название
* Категория
* Описание
* Широта (latitude)
* Долгота (longitude)
* Идентификатор области
* Дата (ггггммдд) (Первый день месяца, за который есть данные)

| Идентификатор места | Название | Категория | Описание | Широта | Долгота | Идентификатор области | Дата |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | Государственный академический малый театр | Театр | Государственный академический малый театр… | 55.760176 | 37.619699 | 1 | 20190201 |
| 2 | Музей Ю. В. Никулина | Музей | Музей Ю. В. Никулина… | 55.757666 | 37.634706 | 1 | 20190201 |
## Выходные данные
* Идентификатор персоны
* Идентификатор места
* Рекомендация места*
* Название места
* Описание места
* Широта
* Долгота
* Идентификатор области
* Дата

Рекомендация места – это оценка того на сколько данные место подходит к персоне. Необходимо предложить и реализовать свой вариант расчета оценки.  
Пример. Можно объединить персоны в группы, выявить для каждой группы множество мест. Далее предложить персонам из одной группы места для этой группы отсортированные тем или иным способом.
## Требования
* Реализация с использованием Apache Spark (scala/java)
* Приложение должно запускаться/собраться локально для демонстрации (maven/sbt)
* Предусмотреть, что объем данных может превышать несколько терабайт
* Комментарии и объяснения
* Качество кода
* Расчеты с «*» - будут плюсом
* Тесты – желательно
## Дополнительно
* Входные данные можно сгенерировать.
* Можно делать предположения и упрощения. Это необходимо описать.
* Расчет можно делать только при совпадении дат и областей.