from pyspark import *
from itertools import permutations
from queue import Queue

import json

dictJsonPath = "/Users/junyan/Downloads/Spark-Excercise/freq_dict.json"
conf = SparkConf().setAppName("Jumble")
sc = SparkContext(conf=conf)


def main():
    dict_json = json.load(open(dictJsonPath, "r"))
    jumble(dict_json, [("NAGLD", "01011"), ("RAMOJ", "00110"), ("CAMBLE", "110100"), ("WRALEY", "101010")], [3, 4, 4])


def jumble(dict_json, jumbles, answer_blank):
    # convert to dict RDD
    word_list = []
    for key in dict_json:
        # if dict_json[key] > 0:  # only add frequent word
        word_list.append(key)

    candidate_letter_set = sc.parallelize([""])
    dict_rdd = sc.parallelize(word_list)
    for pair in jumbles:
        permutation_rdd = sc.parallelize([''.join(p) for p in permutations(pair[0].lower())])
        valid_rdd = dict_rdd.intersection(permutation_rdd)
        mask_rdd = valid_rdd.map(lambda x: mask_string(x, pair[1]))
        candidate_letter_set = candidate_letter_set.cartesian(sc.parallelize(mask_rdd.collect()))
        candidate_letter_set = sc.parallelize(candidate_letter_set.map(lambda x: x[0] + x[1]).collect())

    candidate_letter_list = candidate_letter_set.map(lambda x: ''.join(sorted(x))).distinct().collect()

    result_list = sc.parallelize(candidate_letter_list).map(lambda x: bfs(x, answer_blank, dict_json)).collect()

    for result in result_list:
        print(*result, sep=' ')


def bfs(string, answer_blank, dict_json):
    queue = Queue()
    result = list()
    for p in permutations(string, answer_blank[0]):
        if ''.join(p) in dict_json:
            l = list()
            l.append(list_subtract(list(p), list(string)))
            l.append(list(p))
            queue.put(l)
    max_freq = 0
    while not queue.empty():
        l = queue.get()
        if len(l) < len(answer_blank) + 1:
            for p in permutations(l[0], answer_blank[len(l) - 1]):
                if ''.join(p) in dict_json:
                    l_copy = l[:]
                    l_copy[0] = list_subtract(list(p), l[0])
                    l_copy.append(list(p))
                    queue.put(l_copy)
        else:
            current = 0
            for i in range(1, len(l)):
                current += dict_json[''.join(l[i])]
                if current > max_freq:
                    result = l[1:]
                    max_freq = current

    return result


def list_subtract(short_list, long_list):
    result = []
    short_list_copy = short_list[:]
    for c in long_list:
        if c not in short_list_copy:
            result.append(c)
        else:
            short_list_copy.remove(c)

    return result


def mask_string(string, mask):
    result = ""
    for i in range(len(mask)):
        if mask[i] == '1':
            result += string[i]
    return result


if __name__ == '__main__':
    main()
    # bfs([["1", "2"], ["3", "4"], ["5", "6"]])
