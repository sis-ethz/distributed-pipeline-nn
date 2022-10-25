import re
from itertools import islice


def chunk_by_paragraph(text: str):
    no_newlines = text.strip("\n")  # remove leading and trailing "\n"
    split_text = (re.compile(r"\n{1, }")).split(no_newlines)

    paragraphs = [p + "\n" for p in split_text if p.strip()]
    # p + "\n" ensures that all lines in the paragraph end with a newline
    # p.strip() == True if paragraph has other characters than whitespace

    return {k: v for k, v in enumerate(paragraphs)}


def chunk_by_sentence_and_len(text: str, threshold: int = 200):
    out = []
    for chunk in text.split('. '):
        if out and len(chunk) + len(out[-1]) < threshold:
            out[-1] += ' ' + chunk + '.'
        else:
            out.append(chunk + '.')
    return {k: v for k, v in enumerate(out)}
    # return out

def chunk_by_overlap_windows(text: str, threshold: int = 200):
    def chunk(text: str, threshold: int = 200):
        itr = iter(text)
        res = tuple(islice(itr, threshold))
        if len(res) == threshold:
            yield res
        for ele in itr:
            res = res[1:] + (ele,)
            yield res
    res = ["".join(ele) for ele, i in zip(chunk(text=text, threshold=threshold))]
    return {k: v for k, v in enumerate(res)}


def chunk_by_sentance(text: str):
    splits = text.split('.')
    return {k: v for k, v in enumerate(splits)}


sample_text = 'Albert Einstein was a German-born theoretical physicist who developed the theory of relativity, ' \
              'one of the two pillars of modern physics. ' \
              'His work is also known for its influence on the philosophy of science.'

# sample_text = chunk_by_sentence_and_len(sample_text)
# print(sample_text)
