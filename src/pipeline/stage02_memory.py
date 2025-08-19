import sys
from src.config import CONFIG, PARMS
from src.components.memory_for_llm import PDFVectorStore
from src.exception import MyException

class Vector_DB_Pipeline:
    def __init__(self):
        pass

    @staticmethod
    def main():
        config = CONFIG.get("CONFIG FOR MEMO", {})

        data = config.get("DATA_PATH")
        db_faiss_path = config.get("DB_FAISS_PATH")
        model_name = config.get("MODEL_NAME")

        parms = PARMS.get("PARMS FOR MEMO", {})

        chunk_size = parms.get('CHUNK_SIZE')
        chunk_overlap = parms.get("CHUNK_OVERLAP")

        
        vector_store = PDFVectorStore()
        extracted_data = vector_store.load_pdfs(data=data)
        text_chunk = vector_store.create_chunks(extracted_data=extracted_data, chunk_size=chunk_size, chunk_overlap=chunk_overlap)
        model = vector_store.load_embedding_model(model_name=model_name)
        vector_store.build_faiss_index(text_chunks=text_chunk, embedding_model=model, db_faiss_path=db_faiss_path)


if __name__ == '__main__':
    try:
        obj = Vector_DB_Pipeline()
        obj.main()
    except MyException as e:
            raise MyException(e, sys)