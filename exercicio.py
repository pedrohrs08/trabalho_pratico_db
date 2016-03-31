import mincemeat
import csv
import glob

text_files = glob.glob('source_files/*')

def file_contents(file_name):
    f = open(file_name)
    try:
        return f.read()
    finally:
        f.close()
        
source = dict((file_name, file_contents(file_name))for file_name in text_files)


def map_function(k, v):
    punctuations = '''!()-[]{};:'"\,<>./?@#$%^&*_~'''
    from stopwords import allStopWords
    for line in v.splitlines():
        authors_list = line.split(":::")[1]
        terms = line.split(":::")[2]
	for author in authors_list.split("::"):
	        for word in terms.split():
                        word = word.lower()
			no_punctuation_word = ''
			for char in word:
				if char not in punctuations:
					no_punctuation_word = no_punctuation_word + char
			if(no_punctuation_word not in allStopWords):
				yield author,{no_punctuation_word: 1}
            

def reduce_function(k, v):
    print "Reducing for Author: " + k
    dictionary = {}
    for index, item in enumerate(v):
	for key,value in item.iteritems():
        	value_old = dictionary.get(key,0)
		dictionary[key] = value_old + value 
	
    return sorted(dictionary.iteritems(), key=lambda (k,v): (v,k),reverse=True)


s = mincemeat.Server()
s.datasource = source
s.mapfn = map_function
s.reducefn = reduce_function

results = s.run_server(password="changeme")

print "As duas palavras que mais aparecem para Grzegorz Rozenberg  sao:"
print results['Grzegorz Rozenberg'][0][0]
print results['Grzegorz Rozenberg'][1][0]

print "As duas palavras que mais aparecem para Philip S. Yu sao:"
print results['Philip S. Yu'][0][0]
print results['Philip S. Yu'][1][0]


w = csv.writer(open("results.csv", "w"))
for k, v in results.items():
    w.writerow([k, v])
