Python 3.12.3 (v3.12.3:f6650f9ad7, Apr  9 2024, 08:18:47) [Clang 13.0.0 (clang-1300.0.29.30)] on darwin
Type "help", "copyright", "credits" or "license()" for more information.
>>> 
... In [1]: data_rdd = sc.textFile("pulsar.dat") #read data
... 
... In [2]: line_split_rdd = data_rdd.map(lambda x: x.split()) #split each line into a list 
... 
... In [3]: float_line_split_rdd = line_split_rdd.map(lambda x: [float(x[0]), float(x[1]), float(x[2]), float(x[3])]) #convert list of strings into  a list of floats
... 
... In [4]: round_float_line_split_rdd = float_line_split_rdd.map(lambda x: [round(x[0], 0), round(x[1], 0), round(x[2], 0), round(x[3],0)]) #round float values to zero decimal places. 
... 
... In [5]: key_value_rdd = round_float_line_split_rdd.map(lambda x: ((x[0], x[1], x[3]), x[2])).groupByKey() # map creates tuples where key is tuple of ascension, declination, frequency and value is time
... 
... In [6]: result_rdd = key_value_rdd.map(lambda x: (x[0], list(x[1]))) #first value is key, second value is list of times the blip is occuring
... 
... In [7]: def calculate_period(times)"
...   File "<ipython-input-7-c917ff987db3>", line 1
...     def calculate_period(times)"
...                                 ^
... SyntaxError: EOL while scanning string literal
... 
... 
... In [8]: def calculate_period(times):
...    ...:     if len(times) < 2:
...    ...:         return None
...    ...:     sorted_times = sorted(times)
...    ...:     periods = [sorted_times[i] - sorted_times[i-1] for i in range(1, len(sorted_times))] # to     get periodicity of emissions. 
...    ...:     return round(sum(periods)/ len(periods)) #average period
...    ...: 
... 
In [9]: blips_rdd = result_rdd.map(lambda x: (x[0], (len(x[1]), calculate_period(list(x[1]))))) #key with blip counts, x[0] is the key ascension, declination, frequency and x[1] is the count of blips 

In [10]: final_answer_rdd = blips_rdd.sortBy(lambda x: x[1][0], ascending = False) #sorts by descending order, x[1][0] refers to the count of blips
                                                                                
In [11]: print(final_answer_rdd.take(10))
[((87.0, 68.0, 4448.0), (18, 1)), ((86.0, 68.0, 4448.0), (18, 1)), ((105.0, 111.0, 3031.0), (16, 4)), ((57.0, 115.0, 3509.0), (15, 3)), ((59.0, 58.0, 3782.0), (14, 8)), ((82.0, 96.0, 4631.0), (13, 8)), ((77.0, 89.0, 8245.0), (12, 26)), ((77.0, 89.0, 8246.0), (10, 32)), ((103.0, 67.0, 6957.0), (8, 9)), ((65.0, 102.0, 8172.0), (8, 9))]
