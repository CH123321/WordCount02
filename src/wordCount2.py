from pyspark import SparkContext
import re, sys

# define a function which convert lines to word pairs
def line2wordsPair(line):
	# use regular expression to find all symbols which are not included in alphabet
    symbols = re.compile(r'[^\w\ ]|[\d]')
	# remove the symbols and transfer the string to lower-case
    cleanLine = symbols.sub("",line).lower()
	# using space to split the new line to words
    words = cleanLine.split()
	# set a list to store the words which have been counted
    temp=[]
	# set a list to store the word pairs
    keyPairs=[]
	### The following for loops could be optimized to while loops which could delet the words has been mapped
	### However, this method may need us to change the index of iterator which makes the code looks a little mess
	### Moreover, the number of words in each line is not too much, so it won't bring too much extra computation
	# use for loop to map each word in the line
    for word1 in words:
		# ignore the word which has been mapped
        if word1 not in temp:
			# find other words in the line
            for word2 in words:
				# ignore the same word
                if word2 != word1:
					# ignore the word which has been mapped
                    if word2 not in temp:
						# creat two tuples to store the word pairs. eg: (a,b) and (b,a)
                        key1 = (word1, word2)
                        key2 = (word2, word1)
						# append the key pairs to the list
                        keyPairs.append(key1)
                        keyPairs.append(key2)
		# remember the word which has been mapped
        temp.append(word1)
    return keyPairs

# define a function to save the result in a file with a format compose
def printResult(result):
	# open the file
    f= open("/home/huang/Spark/HW2/result.txt","w")
	# get the lenth of the result list
    length = len(result)
	# excute the following code while length is not 0
    while length:
		# get the firt element of the list
        each = result[0]
		# write the information of the element to the file
        f.write("With the word {},\n".format(each[0][0]))
        f.write ("\t{} occurs {} time(s).\n".format(each[0][1], each[1]))
		# remove the element which has been used
        result.remove(result[0])
        length -= 1
		# find other element which has the same word as the first word of word pair
        index=0
        while index< length:
            other = result[index]
			# only need the element which has the same head word
            if other[0][0]==each[0][0]:
                f.write ("\t{} occurs {} time(s).\n".format(other[0][1], other[1]))
				# remove the fitted element 
                result.remove(other)
                index -= 1
                length -= 1
            index +=1  
        f.write("--------------------------------\n")
    f.close()   

# set the master addres and Application name
sc = sc = SparkContext("spark://localhost:7077","Word Count 2")
# load the file
textfile = sc.textFile(sys.argv[1])
# get the the output
wordCounts = textfile.flatMap(line2wordsPair).map(lambda pair: (pair,1)).reduceByKey(lambda a,b:a+b)
results = wordCounts.collect()
# print the result to file
printResult(results)