from pyspark import *
from itertools import permutations
from queue import Queue

import json

dictJsonPath = "/Users/junyan/Downloads/Spark-Excercise/freq_dict.json"
conf = SparkConf().setAppName("Jumble")
sc = SparkContext(conf=conf)
sc.setLogLevel('ERROR')

frequency_sum_limit = 2000


def main():
    dict_json = json.load(open(dictJsonPath, "r"))
    # jumble(dict_json, [("NAGLD", "01011"), ("RAMOJ", "00110"), ("CAMBLE", "110100"), ("WRALEY", "101010")], [3, 4, 4])

    # answer: RASH DECISION
    jumble(dict_json, [("SHAST", "10011"), ("DOORE", "11010"), ("DITNIC", "111000"), ("CATILI", "101001")], [4, 8])


def jumble(dict_json, jumbles, answer_blank):
    # convert to dict RDD
    word_list = []
    for key in dict_json:
        word_list.append(key)

    candidate_letter_set = sc.parallelize([""])
    dict_rdd = sc.parallelize(word_list)
    for pair in jumbles:
        # generate the permutation of given letters, store it into RDD
        permutation_rdd = sc.parallelize([''.join(p) for p in permutations(pair[0].lower())])
        # permutation RDD intersect with dict RDD, result is valid guess of this Jumble
        valid_rdd = dict_rdd.intersection(permutation_rdd).persist()
        print("input word: " + pair[0] + "; valid filling: " + ' '.join(valid_rdd.collect()))
        # mask the word: "STASH" + "10011" => "SSH"
        mask_rdd = valid_rdd.map(lambda x: mask_string(x, pair[1])).persist()
        # since each letters input might have multiple valid guess, cartesian combination generating the result set
        candidate_letter_set = candidate_letter_set.cartesian(mask_rdd)
        candidate_letter_set = sc.parallelize(candidate_letter_set.map(lambda x: x[0] + x[1]).collect())

    # till here, candidate_letter_set looks like below, each is possible letter series
    # 0 = {str} 'sshroeindcia'
    # 1 = {str} 'sshroeindiac'
    # 2 = {str} 'sshroeindcia'
    # 3 = {str} 'sshroeindiac'
    print()

    # sort the letters and remove duplicate, then call BFS method give all possible answer(base on answer letter format)
    result_list = candidate_letter_set \
        .map(lambda x: ''.join(sorted(x))) \
        .distinct()\
        .persist()\
        .flatMap(lambda x: bfs(x, answer_blank, dict_json)) \
        .distinct() \
        .collect()

    print()
    for result in result_list:
        print(*result, sep=' ')


# this method using BFS search give a possible answer list
# for example: "sshroeindiac" + [4, 8]=> "rash" "decision" ... ...
# frequency_sum_limit (defined at top) is to avoid letter combination is too strange(frequency rank to high)
# for example: "rash"(frequency: 0), "decision"(frequency: 1124). The sum is 1124 less than limit, it is a valid guess
def bfs(string, answer_blank, dict_json):
    queue = Queue()
    result_list = list()
    for p in permutations(string, answer_blank[0]):
        if ''.join(p) in dict_json:
            l = list()
            l.append(list_subtract(list(p), list(string)))
            l.append(list(p))
            queue.put(l)

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
            if (current > 0) and (current < frequency_sum_limit):
                answer = '| '
                for i in range(1, len(l)):
                    answer += ''.join(l[i])
                    answer += " | "
                result_list.append(answer)

    return result_list


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

