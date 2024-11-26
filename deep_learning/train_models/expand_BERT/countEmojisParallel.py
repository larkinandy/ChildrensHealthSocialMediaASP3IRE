from multiprocessing import Pool
import glob
import os
import emoji
import re
import regex


def prepParallel(inFolder,outFolder):
    parallelTuples = []
    index = 0
    tweetFiles = os.listdir(inFolder)
    for file in tweetFiles:
        parallelTuples.append((inFolder + file,outFolder + file[:-4] + "_emojis.txt"))
        index+=1
    return(parallelTuples)


def split_count(text):
    total_emoji = []
    counter = {}
    data = regex.findall(r'\X',text)
    flag = False
    for word in data:
        if any(char in emoji.EMOJI_DATA for char in word):  
            total_emoji += [word] # total_emoji is a list of all emojis

    # Remove from the given text the emojis
    for current in total_emoji:
        text = text.replace(current, '') 
    return total_emoji
    #return Counter(total_emoji)

def writeToText(data,outputFile):
    with open(outputFile, "w",encoding='utf-8') as txt_file:
        for line in data:
            txt_file.write(line) # works with any number of elements in a line

def processSingleFile(dataTuple):
    inFilepath = dataTuple[0]
    outFilepath = dataTuple[1]
    with open(inFilepath,'r',encoding='utf-8') as f:
        file = f.read().split("\n")
    emojiList = list(map(split_count,file))
    flat_list = [item for sublist in emojiList for item in sublist]
    writeToText(flat_list,outFilepath)




folder = 'H:/Aspire/BERT/BERT_text/'
outFolder = "H:/Aspire/BERT/"


if __name__ == '__main__':
    parallelTuples = prepParallel(folder,outFolder)
    print(parallelTuples)
    pool = Pool(processes=3)
    res = pool.map_async(processSingleFile,parallelTuples)
    res.get()    