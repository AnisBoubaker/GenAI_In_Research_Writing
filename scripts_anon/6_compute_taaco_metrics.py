from TAACOnoGUI import runTAACO

#set processing options
sampleVars = {"sourceKeyOverlap" : False, "sourceLSA" : False, "sourceLDA" : False, "sourceWord2vec" : False, "wordsAll" : True, "wordsContent" : True, "wordsFunction" : True, "wordsNoun" : True, "wordsPronoun" : True, "wordsArgument" : True, "wordsVerb" : True, "wordsAdjective" : True, "wordsAdverb" : True, "overlapSentence" : True, "overlapParagraph" : True, "overlapAdjacent" : True, "overlapAdjacent2" : True, "otherTTR" : True, "otherConnectives" : True, "otherGivenness" : True, "overlapLSA" : True, "overlapLDA" : True, "overlapWord2vec" : True, "overlapSynonym" : True, "overlapNgrams" : True, "outputTagged" : False, "outputDiagnostic" : False}

# Run TAACO on a folder of texts ("ELLIPSE_Sample/"), give the output file a name ("packageTest.csv), provide output for particular indices/options (as defined in sampleVars)
runTAACO("/media/anis/TAACO/ArxivPapersTxt","taaco_metrics.csv",sampleVars)
