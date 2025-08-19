from pathlib import Path
from langchain_community.document_loaders import PyPDFLoader, DirectoryLoader
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_community.vectorstores import FAISS
from src.logger import logging

class PDFVectorStore:
    """
    A pipeline for processing PDF documents, generating text embeddings, 
    and storing them in a FAISS vector database for efficient similarity search.

    Workflow:
        1. Load PDF documents from a given directory.
        2. Split text into overlapping chunks for better embedding performance.
        3. Generate embeddings using a HuggingFace transformer model.
        4. Store and persist embeddings in a FAISS vector store.

    Attributes:
        data_path (str): Directory path containing PDF files.
        db_faiss_path (str): Directory path to save FAISS vector database.
        chunk_size (int): Size of each text chunk.
        chunk_overlap (int): Overlap size between chunks.
        model_name (str): HuggingFace model name used for embeddings.
    """

    def __init__(self):
        '''
        Initialize the PDFVectorStore pipeline with configuration values.
        '''

    def load_pdfs(self, data):
        """
        Load all PDF files from the configured directory.

        Returns:
            list: A list of Document objects extracted from PDF files.
        """
        loader = DirectoryLoader(data, glob="*.pdf", loader_cls=PyPDFLoader)
        documents = loader.load()

        logging.info("Length of PDF pages: %s", len(documents))
        return documents

    def create_chunks(self, extracted_data, chunk_size, chunk_overlap):
        """
        Split extracted documents into smaller overlapping chunks.

        Returns:
            list: A list of chunked Document objects.
        """
        if not extracted_data:
            raise ValueError("No documents found. Run load_pdfs() first.")
        
        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap
        )
        text_chunks = text_splitter.split_documents(extracted_data)
        return text_chunks

    def load_embedding_model(self, model_name):
        """
        Load a HuggingFace transformer embedding model.

        Returns:
            HuggingFaceEmbeddings: The initialized embedding model.
        """
        embedding_model = HuggingFaceEmbeddings(model_name=model_name)
        return embedding_model

    def build_faiss_index(self, text_chunks, embedding_model, db_faiss_path):
        """
        Build a FAISS vector store from text chunks and embeddings.

        Returns:
            FAISS: The initialized FAISS vector store.
        """
        if not text_chunks:
            raise ValueError("No text chunks found. Run create_chunks() first.")
        if not embedding_model:
            self.load_embedding_model()

        db = FAISS.from_documents(text_chunks, embedding_model)

        if not db:
            raise ValueError("No FAISS index found. Run build_faiss_index() first.")
        db.save_local(str(db_faiss_path))
        return db
        

    # def run_pipeline(self):
    #     """
    #     Execute the full pipeline: load PDFs → chunk → embed → build & save FAISS index.

    #     Returns:
    #         FAISS: The built FAISS vector store.
    #     """
    #     self.load_pdfs()
    #     self.create_chunks()
    #     self.load_embedding_model()
    #     self.build_faiss_index()
    #     self.save_index()
    #     return db


# if __name__ == "__main__":
#     # Example usage
#     pdf_pipeline = PDFVectorStore(data_path="data/", db_faiss_path="vectorstore/db_faiss")
#     db = pdf_pipeline.run_pipeline()
#     print("✅ FAISS index created and saved at:", pdf_pipeline.db_faiss_path)
