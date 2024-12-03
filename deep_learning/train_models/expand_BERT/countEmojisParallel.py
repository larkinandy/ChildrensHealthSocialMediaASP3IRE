# countEmjoisParallel.py
#Author: Andrew Larkin

# Summary: for each social media post, identify emojis present in the post.
#          each emoji can only be counted one time in each post

# import libraries
from multiprocessing import Pool
import os
import emoji
import regex

# import secrets
from mySecrets import secrets


# create data tuples to count emojis in multiple files in parallel
# INPUTS:
#    inFolder (str) - absolute folderpath where text records are stored
#    outFolder (str) - absolute folderpath where emoji counts will be stored
# OUTPUTS:
#    list of tuples, one tuple for each task that can be launched in parallel
def prepParallel(inFolder,outFolder):
    parallelTuples = []
    index = 0
    tweetFiles = os.listdir(inFolder)
    for file in tweetFiles:
        parallelTuples.append((inFolder + file,outFolder + file[:-4] + "_emojis.txt"))
        index+=1
    return(parallelTuples)

# get emojis present in a single social media post
# INPUTS:
#    text (str) - text from a single social media post
# OUTPUTS:
#    total_emojis (str list) - emojis present in the social media post
def splitCount(text):
    total_emoji = []

    # get all words and emojis present in the post
    data = regex.findall(r'\X',text)

    # create list of emojis present in the post
    for word in data:
        if any(char in emoji.EMOJI_DATA for char in word):  
            total_emoji += [word] # total_emoji is a list of all emojis

    return total_emoji

# 
def writeToText(data,outputFile):
    with open(outputFile, "w",encoding='utf-8') as txt_file:
        for line in data:
            txt_file.write(line) # works with any number of elements in a line

# given a text file containing a set of social media posts, extract emojis from the 
# entire text document
# INPUTS:
#    dataTuple (tuple) - information needed for worker thread to count emojis 
#        index 0 - (str) - absolute filepath of text document to analyze
#        index 1 - (str) - absolute filepath for writing emoji counts to disk
def processSingleFile(dataTuple):
    inFilepath = dataTuple[0]
    outFilepath = dataTuple[1]

    # read input text file and split into individual social media posts (one post for each line)
    with open(inFilepath,'r',encoding='utf-8') as f:
        file = f.read().split("\n")
    # for each post, get the emojis present in the post
    emojiList = list(map(splitCount,file))
    # flatten emoji lists and write to txt file
    flat_list = [item for sublist in emojiList for item in sublist]
    writeToText(flat_list,outFilepath)


# main function
if __name__ == '__main__':

    # create tuples to count emojis from multiple files in parallel
    parallelTuples = prepParallel(
        secrets['BERT_EMOJI_INPUT_FOLDER'],
        secrets['BERT_EMOJI_OUTPUT_FOLDER']
    )
    pool = Pool(processes=secrets['N_CPUS'])

    # count emojis 
    res = pool.map_async(processSingleFile,parallelTuples)
    res.get()    

# end of countEmojisParallel.py