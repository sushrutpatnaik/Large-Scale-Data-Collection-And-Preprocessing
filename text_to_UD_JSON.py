from UD_to_JSON import conllu2json
import sys
from ufal.udpipe import Model, Pipeline, ProcessingError

def texttoUDJson(text):
    model = Model.load('spanish-ancora-ud-2.3-181115.udpipe')
    if not model:
        sys.stderr.write("Cannot load model from file ")
        sys.exit(1)
    sys.stderr.write('done\n')

    pipeline = Pipeline(model,"tokenize",Pipeline.DEFAULT,Pipeline.DEFAULT, "conllu")
    error = ProcessingError()

    processed = pipeline.process(text,error)

    if error.occurred():
        sys.stderr.write("An error occurred when running run_udpipe: ")
        sys.stderr.write(error.message)
        sys.stderr.write("\n")
        sys.exit(1)

    tree = conllu2json(processed)

    return tree
