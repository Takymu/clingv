import re
import numpy as np
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity

model = SentenceTransformer('sentence-transformers/paraphrase-multilingual-mpnet-base-v2')

def get_chunks(text, mode='sentence'):
    if mode == 'sentence':
        return re.split(r'(?<=[.!?])\s+', text)
    elif mode == 'word':
        return re.findall(r'\w+', text)
    elif mode == 'paragraph':
        return [p for p in text.split('\n') if p.strip()]
    elif mode == 'fixed_size':
        words = text.split()
        return [' '.join(words[i:i+10]) for i in range(0, len(words), 10)]
    return [text]

def get_embeddings(texts):
    return model.encode(texts)

def cos_compare(vec1, vec2):
    vec1 = np.array(vec1).reshape(1, -1)
    vec2 = np.array(vec2).reshape(1, -1)
    return cosine_similarity(vec1, vec2)[0][0]

if __name__ == "__main__":
    text1 = "Косинусное сходство является метрикой, используемой для определения степени схожести. Оно измеряет косинус угла между векторами."
    text2 = "Мама мыла раму, а папа клеил обои."
    text3 = "Мама мыла папу, а папа клеил обои."
    
    print("sentence chunks:", get_chunks(text1, 'sentence'))
    print("word chunks:", get_chunks(text1, 'word'))
    print("paragraph chunks:", get_chunks(text1, 'paragraph'))
    
    emb1 = get_embeddings(get_chunks(text1, 'sentence'))
    emb2 = get_embeddings(get_chunks(text2, 'sentence'))
    emb3 = get_embeddings(get_chunks(text3, 'sentence'))
    
    similarity12 = cos_compare(emb1[0], emb2[0])
    similarity13 = cos_compare(emb1[0], emb3[0])
    similarity23 = cos_compare(emb2[0], emb3[0])
    print(f"similarity12: {similarity12:.4f}")
    print(f"similarity13: {similarity13:.4f}")
    print(f"similarity23: {similarity23:.4f}")