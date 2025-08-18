from pathlib import Path
from langchain_community.document_loaders import PyPDFLoader, DirectoryLoader
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_community.vectorstores import FAISS


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

    def __init__(self,
                 data_path: str = "data/",
                 db_faiss_path: str = "vectorstore/db_faiss",
                 chunk_size: int = 500,
                 chunk_overlap: int = 50,
                 model_name: str = "sentence-transformers/all-MiniLM-L6-v2"):
        """
        Initialize the PDFVectorStore pipeline with configuration values.

        Args:
            data_path (str): Directory containing PDF files.
            db_faiss_path (str): Path where FAISS index will be stored.
            chunk_size (int): Number of characters in each text chunk.
            chunk_overlap (int): Number of characters overlapping between chunks.
            model_name (str): HuggingFace embedding model name.
        """
        self.data_path = Path(data_path)
        self.db_faiss_path = Path(db_faiss_path)
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.model_name = model_name

        self.documents = []
        self.text_chunks = []
        self.embedding_model = None
        self.db = None

    def load_pdfs(self):
        """
        Load all PDF files from the configured directory.

        Returns:
            list: A list of Document objects extracted from PDF files.
        """
        loader = DirectoryLoader(str(self.data_path),
                                 glob="*.pdf",
                                 loader_cls=PyPDFLoader)
        self.documents = loader.load()
        return self.documents

    def create_chunks(self):
        """
        Split extracted documents into smaller overlapping chunks.

        Returns:
            list: A list of chunked Document objects.
        """
        if not self.documents:
            raise ValueError("No documents found. Run load_pdfs() first.")
        
        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=self.chunk_size,
            chunk_overlap=self.chunk_overlap
        )
        self.text_chunks = text_splitter.split_documents(self.documents)
        return self.text_chunks

    def load_embedding_model(self):
        """
        Load a HuggingFace transformer embedding model.

        Returns:
            HuggingFaceEmbeddings: The initialized embedding model.
        """
        self.embedding_model = HuggingFaceEmbeddings(model_name=self.model_name)
        return self.embedding_model

    def build_faiss_index(self):
        """
        Build a FAISS vector store from text chunks and embeddings.

        Returns:
            FAISS: The initialized FAISS vector store.
        """
        if not self.text_chunks:
            raise ValueError("No text chunks found. Run create_chunks() first.")
        if not self.embedding_model:
            self.load_embedding_model()

        self.db = FAISS.from_documents(self.text_chunks, self.embedding_model)
        return self.db

    def save_index(self):
        """
        Save the FAISS vector store to the configured local path.
        """
        if not self.db:
            raise ValueError("No FAISS index found. Run build_faiss_index() first.")
        self.db.save_local(str(self.db_faiss_path))

    def run_pipeline(self):
        """
        Execute the full pipeline: load PDFs → chunk → embed → build & save FAISS index.

        Returns:
            FAISS: The built FAISS vector store.
        """
        self.load_pdfs()
        self.create_chunks()
        self.load_embedding_model()
        self.build_faiss_index()
        self.save_index()
        return self.db


if __name__ == "__main__":
    # Example usage
    pdf_pipeline = PDFVectorStore(data_path="data/", db_faiss_path="vectorstore/db_faiss")
    db = pdf_pipeline.run_pipeline()
    print("✅ FAISS index created and saved at:", pdf_pipeline.db_faiss_path)





# Step 1: Load raw PDF(s)
DATA_PATH="data/"
def load_pdf_files(data):
    loader = DirectoryLoader(data,
                             glob='*.pdf',
                             loader_cls=PyPDFLoader)
    
    documents=loader.load()
    return documents

documents=load_pdf_files(data=DATA_PATH)
#print("Length of PDF pages: ", len(documents))


# Step 2: Create Chunks
def create_chunks(extracted_data):
    text_splitter=RecursiveCharacterTextSplitter(chunk_size=500,
                                                 chunk_overlap=50)
    text_chunks=text_splitter.split_documents(extracted_data)
    return text_chunks

text_chunks=create_chunks(extracted_data=documents)
#print("Length of Text Chunks: ", len(text_chunks))

# Step 3: Create Vector Embeddings 

def get_embedding_model():
    embedding_model=HuggingFaceEmbeddings(model_name="sentence-transformers/all-MiniLM-L6-v2")
    return embedding_model

embedding_model=get_embedding_model()

# Step 4: Store embeddings in FAISS
DB_FAISS_PATH="vectorstore/db_faiss"
db=FAISS.from_documents(text_chunks, embedding_model)
db.save_local(DB_FAISS_PATH)