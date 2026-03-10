import re
import numpy as np
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity

model = SentenceTransformer('sentence-transformers/paraphrase-multilingual-mpnet-base-v2')

def get_chunks(text):
    return re.split(r'(?<=[.!?])\s+', text)

def get_embeddings(texts):
    return model.encode(texts)

def cos_compare(vec1, vec2):
    vec1 = np.array(vec1).reshape(1, -1)
    vec2 = np.array(vec2).reshape(1, -1)
    return cosine_similarity(vec1, vec2)[0][0]

if __name__ == "__main__":
    text1 = "Косинусное сходство является метрикой, используемой для определения степени схожести. Оно измеряет косинус угла между векторами."
    text2 = "Мама мыла раму, а папа клеил обои."
    
    chunks1 = get_chunks(text1)
    chunks2 = get_chunks(text2)
    
    print("Фрагменты текста 1:", chunks1)
    print("Фрагменты текста 2:", chunks2)
    
    emb1 = get_embeddings(chunks1)
    emb2 = get_embeddings(chunks2)
    
    similarity = cos_compare(emb1[0], emb2[0])
    print(f"\nСходство между первым предложением из текста 1 и текста 2: {similarity:.4f}")