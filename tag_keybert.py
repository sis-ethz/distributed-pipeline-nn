from keybert import KeyBERT


def get_keywords(doc):
    kw_model = KeyBERT()
    kw_model_multi = KeyBERT('paraphrase-multilingual-MiniLM-L12-v2')
    keywords = kw_model.extract_keywords(doc, top_n=10)
    print(keywords)
