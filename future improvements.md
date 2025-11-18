
dougstrouth: https://duckdb.org/2024/05/03/vector-similarity-search-vss is something I want to implement

GitHub Copilot: Interesting! Vector Similarity Search in DuckDB would be great for finding similar companies based on their financial metrics, or similar filings based on text embeddings. Let me create a plan for implementing this. 

Research DuckDB VSS implementation

Read [](file:///Users/dougstrouth/github_noicloud/edgar/data_processing/edgar_data_loader.py#1-1), lines 1 to 150

Read [](file:///Users/dougstrouth/github_noicloud/edgar/data_processing/summarize_filings.py#1-1), lines 1 to 100

Read [](file:///Users/dougstrouth/github_noicloud/edgar/data_processing/feature_engineering.py)

Read [](file:///Users/dougstrouth/github_noicloud/edgar/main.py#1-1), lines 1 to 100

Read [](file:///Users/dougstrouth/github_noicloud/edgar/data_processing/edgar_data_loader.py#150-150), lines 150 to 300

Searched for regex `embedding|vector|similarity|vss`, 1 result

Read [](file:///Users/dougstrouth/github_noicloud/edgar/data_processing/summarize_filings.py#100-100), lines 100 to 156

Read [](file:///Users/dougstrouth/github_noicloud/edgar/README.md#1-1), lines 1 to 100

Read [](file:///Users/dougstrouth/github_noicloud/edgar/DATA_DICTIONARY.md#1-1), lines 1 to 150

Read [](file:///Users/dougstrouth/github_noicloud/edgar/analysis/edgar_analysis_functions.py#1-1), lines 1 to 150

Searched for regex `duckdb.*version|INSTALL|LOAD|extension`, 20 results

Read [](file:///Users/dougstrouth/github_noicloud/edgar/pyproject.toml)

Searched for regex `conn\.execute\(.*INSTALL|conn\.execute\(.*LOAD|\.install_extension|\.load_extension`, 6 results

Read [](file:///Users/dougstrouth/github_noicloud/edgar/utils/database_conn.py#1-1), lines 1 to 100

dougstrouth: fetch(https://duckdb.org/2024/05/03/vector-similarity-search-vss)

GitHub Copilot: I'll fetch that DuckDB VSS article for you to understand vector similarity search capabilities. 

Using "Fetch Web Page"

Fetched https://duckdb.org/2024/05/03/vector-similarity-search-vss

Great! Here's what DuckDB's Vector Similarity Search (VSS) extension offers and how you could use it in your EDGAR project:

## Key Features of DuckDB VSS:

**1. HNSW Index Support**
- Creates HNSW (Hierarchical Navigable Small Worlds) indexes on `FLOAT[]` array columns
- Optimized for high-dimensional vector similarity search
- Uses the `usearch` library under the hood

**2. Distance Metrics**
- `l2sq` (Euclidean distance) - default, uses `array_distance()`
- `cosine` - uses `array_cosine_distance()`
- `ip` (inner product) - uses `array_negative_inner_product()`

**3. Basic Usage**
```sql
-- Install extension
INSTALL vss;
LOAD vss;

-- Create table with embeddings
CREATE TABLE embeddings (vec FLOAT[384]);

-- Create HNSW index
CREATE INDEX idx ON embeddings USING HNSW (vec);
-- Or with custom metric:
CREATE INDEX idx ON embeddings USING HNSW (vec) WITH (metric = 'cosine');

-- Query for similarity
SELECT *
FROM embeddings
ORDER BY array_distance(vec, [1.0, 2.0, 3.0]::FLOAT[384])
LIMIT 10;
```

## Potential Use Cases for Your EDGAR Project:

**1. Semantic Search of SEC Filings**
- Generate embeddings for filing text/summaries
- Find similar companies based on business descriptions
- Search for filings with similar risk factors or MD&A sections

**2. Company Clustering**
- Embed XBRL financial metrics as vectors
- Find similar companies by financial profile
- Group companies by market behavior patterns

**3. Document Similarity**
- Compare 10-K/10-Q filings across time or companies
- Find similar business segments or industries
- Detect narrative changes in filings

## Limitations to Consider:

- **Experimental persistence**: Set `hnsw_enable_experimental_persistence = true` for disk-backed databases
- **Only FLOAT arrays**: No other data types yet
- **RAM requirement**: Index must fit in memory
- **WAL recovery issues**: Risk of corruption on unexpected shutdown (being fixed)

Would you like me to help you implement vector similarity search in your EDGAR pipeline? We could:
1. Add embedding generation for filing text
2. Create a VSS-indexed table for semantic search
3. Build similarity-based company recommendations
4. Use embeddings to enhance your prioritization scoring

dougstrouth: lets plan on how to implement those indexes throughout, refer to the data_dictionary and lets think through the best way to first implement the indexing and where

GitHub Copilot: I'll analyze your data dictionary and current schema to plan the best VSS implementation strategy for your EDGAR project. 

Read [](file:///Users/dougstrouth/github_noicloud/edgar/DATA_DICTIONARY.md#1-1), lines 1 to 200

Searched codebase for "table schema CREATE TABLE companies filings xbrl_facts text description varchar", 16 results

Now let me check for any text columns that could be embedded: 

Read [](file:///Users/dougstrouth/github_noicloud/edgar/DATA_DICTIONARY.md#200-200), lines 200 to 206

Searched for regex `summary_text|description|primary_doc_description|sic_description` (`**/*.py`), 30 results

Perfect! Now I have a clear picture. Let me create a comprehensive plan for implementing VSS indexes in your EDGAR project.

## Plan: Implementing DuckDB Vector Similarity Search (VSS) in EDGAR Project

Based on your data dictionary and current schema, here's a strategic implementation plan for VSS indexes:

### **Phase 1: Foundation - Add Embedding Infrastructure** 

**1.1 Create Embeddings Tables**
New tables to store vector embeddings:

```sql
-- Company-level embeddings
CREATE TABLE company_embeddings (
    cik VARCHAR(10) PRIMARY KEY,
    business_description_embedding FLOAT[384],  -- From sic_description, category, etc.
    last_updated TIMESTAMPTZ DEFAULT now()
);

-- Filing-level embeddings  
CREATE TABLE filing_embeddings (
    accession_number VARCHAR PRIMARY KEY,
    document_embedding FLOAT[768],  -- From primary_doc_description, summary_text
    last_updated TIMESTAMPTZ DEFAULT now()
);

-- XBRL tag embeddings (for semantic tag matching)
CREATE TABLE tag_embeddings (
    taxonomy VARCHAR,
    tag_name VARCHAR,
    semantic_embedding FLOAT[384],  -- From label + description
    PRIMARY KEY (taxonomy, tag_name)
);
```

**1.2 VSS Indexes to Create**
```sql
INSTALL vss;
LOAD vss;

-- Enable experimental persistence (with caution)
SET hnsw_enable_experimental_persistence = true;

-- Company similarity index (cosine for semantic similarity)
CREATE INDEX company_semantic_idx 
ON company_embeddings 
USING HNSW (business_description_embedding) 
WITH (metric = 'cosine');

-- Filing similarity index
CREATE INDEX filing_semantic_idx 
ON filing_embeddings 
USING HNSW (document_embedding) 
WITH (metric = 'cosine');

-- Tag semantic search index
CREATE INDEX tag_semantic_idx 
ON tag_embeddings 
USING HNSW (semantic_embedding) 
WITH (metric = 'cosine');
```

---

### **Phase 2: Best Text Sources for Embeddings**

Based on your schema, prioritize these columns:

**High Value (Implement First):**
1. **`companies.sic_description`** - Industry description (semantic company similarity)
2. **`filing_summaries.summary_text`** - Already generated summaries (perfect for embeddings!)
3. **`xbrl_tags.label` + `xbrl_tags.description`** - Semantic tag matching

**Medium Value (Implement Second):**
4. **`filings.primary_doc_description`** - Document type descriptions
5. **`companies.primary_name` + `category`** - Company name/category combos

**Lower Priority:**
6. Financial metric vectors from `xbrl_facts` (aggregate key metrics into vectors)

---

### **Phase 3: Implementation Strategy**

**Step 1: Create Embedding Generator Script**
`data_processing/generate_embeddings.py`:
- Use a lightweight model like `sentence-transformers/all-MiniLM-L6-v2` (384 dims)
- Batch process existing data
- Generate embeddings for:
  - Company descriptions (from `sic_description`)
  - Filing summaries (from `filing_summaries.summary_text`)
  - XBRL tags (from `label` + `description`)

**Step 2: Modify Data Loaders**
Update edgar_data_loader.py to:
- Create embedding tables during schema initialization
- Create VSS indexes after loading base tables

**Step 3: Create Embedding Update Pipeline**
`data_processing/update_embeddings.py`:
- Incremental updates when new filings/companies added
- Triggered after `load` step

**Step 4: Add VSS Query Utilities**
`utils/semantic_search.py`:
- Helper functions for similarity queries
- Wrapper around VSS index scans

---

### **Phase 4: High-Value Use Cases**

**Use Case 1: Similar Company Discovery**
```sql
-- Find companies similar to a given CIK
SELECT c.cik, c.primary_name, c.sic_description,
       array_cosine_distance(ce.business_description_embedding, target.business_description_embedding) as similarity
FROM company_embeddings ce
JOIN companies c ON ce.cik = c.cik
CROSS JOIN (SELECT business_description_embedding FROM company_embeddings WHERE cik = '0001108426') target
ORDER BY similarity
LIMIT 10;
```

**Use Case 2: Semantic Filing Search**
```sql
-- Find filings similar to a specific filing
SELECT f.accession_number, f.form, c.primary_name, fs.summary_text,
       array_cosine_distance(fe.document_embedding, target.document_embedding) as similarity
FROM filing_embeddings fe
JOIN filings f ON fe.accession_number = f.accession_number
JOIN companies c ON f.cik = c.cik
LEFT JOIN filing_summaries fs ON f.accession_number = fs.accession_number
CROSS JOIN (SELECT document_embedding FROM filing_embeddings WHERE accession_number = 'xxx') target
ORDER BY similarity
LIMIT 20;
```

**Use Case 3: Enhanced Prioritization**
Combine with your existing prioritization:
```python
# In prioritizer.py - add semantic similarity score
def prioritize_with_semantics(db_path, tickers, reference_cik):
    # Get companies semantically similar to high-priority reference company
    similar_companies = semantic_search(reference_cik, top_k=50)
    # Boost priority score for similar companies
    # Combine with XBRL richness, stock data needs, etc.
```

---

### **Phase 5: Integration Points**

**Main Pipeline Integration:**
```python
# In main.py, add after 'load':
("generate-embeddings", None, None),  # Generate embeddings for new data
("create-vss-indexes", None, None),   # Create/update VSS indexes
```

**Prioritizer Integration:**
- Modify prioritizer.py to include semantic similarity scoring
- Weight semantic similarity alongside XBRL richness

---

### **Recommended First Implementation** 

**Start Simple - 3 Steps:**

1. **Company Semantic Search** (Highest ROI)
   - Generate embeddings from `sic_description`
   - Create `company_embeddings` table
   - Add HNSW index
   - Use for "find similar companies" queries

2. **Filing Summary Search** (You already have summaries!)
   - Leverage existing `filing_summaries.summary_text`
   - Generate embeddings from summaries
   - Create `filing_embeddings` table
   - Use for semantic filing discovery

3. **Enhanced Prioritization** (Practical benefit)
   - Use company embeddings to prioritize data gathering
   - Focus on clusters of similar companies
   - Improve stock data prioritization

---

### **Technical Considerations**

**Embedding Model Selection:**
- **Small (384d)**: `all-MiniLM-L6-v2` - Fast, good for shorter text
- **Medium (768d)**: `all-mpnet-base-v2` - Better quality, slower
- **Finance-specific**: `FinBERT` - Domain-specific, if needed

**Performance:**
- Generate embeddings in batches (100-1000 at a time)
- Use GPU if available (`sentence-transformers` supports CUDA)
- VSS queries typically <100ms for indexes under 10M vectors

**Safety:**
- Start with in-memory database for testing
- Use `hnsw_enable_experimental_persistence = true` only after validation
- Back up database before creating large VSS indexes

Would you like me to start implementing any specific phase? I'd recommend starting with **Company Semantic Search** since you already have the `sic_description` data and it provides immediate value for understanding company relationships.

dougstrouth: fetch(https://motherduck.com/blog/search-using-duckdb-part-2/)

GitHub Copilot: I'll fetch that MotherDuck blog post about search using DuckDB to see what additional insights they have for implementing search features. 

Using "Fetch Web Page"

Fetched https://motherduck.com/blog/search-using-duckdb-part-2/

Excellent! This MotherDuck article provides a practical RAG (Retrieval Augmented Generation) implementation using DuckDB. Here are the key insights that complement the VSS extension info:

## Key Takeaways from MotherDuck RAG Tutorial:

**1. LlamaIndex Integration with DuckDB**
- Uses `llama-index-vector-stores-duckdb` for seamless integration
- Handles chunking, embedding generation, and storage automatically
- Persists knowledge base to disk for reuse

**2. Recommended Embedding Model**
- **BAAI/bge-small-en-v1.5**: 384 dimensions, 512 token limit
- Good balance of size/performance for most use cases
- Available through HuggingFace/SentenceTransformers

**3. Document Processing Workflow**
```python
# Load documents
documents = SimpleDirectoryReader("./local_documents").load_data()

# Chunk strategy
service_context = ServiceContext.from_defaults(chunk_size=512)

# Create DuckDB vector store
vector_store = DuckDBVectorStore(
    database_name="knowledge_base",
    persist_dir="./",
    embed_dim=384
)

# Build knowledge base
knowledge_base = VectorStoreIndex.from_documents(
    documents,
    storage_context=storage_context,
    embed_model=embed_model
)
```

**4. Query Engine for RAG**
```python
query_engine = knowledge_base.as_query_engine(llm=llm)
answer = query_engine.query("Your question here")
```

## Updated Implementation Plan for Your EDGAR Project:

Given this new information, I recommend a **hybrid approach** combining:
1. **DuckDB VSS extension** (for direct SQL queries and custom logic)
2. **LlamaIndex integration** (for RAG/QA workflows)

### Revised Architecture:

**Option A: Native DuckDB VSS (Full Control)**
- Direct SQL queries for similarity search
- Custom embedding generation pipeline
- Integrated with your existing data processing scripts
- Best for: Data analysis, company clustering, prioritization

**Option B: LlamaIndex Integration (RAG/QA)**
- Easy document ingestion and chunking
- Built-in RAG workflows for Q&A
- Good for: Semantic search of filings, "ask questions about company" features
- Best for: User-facing features, exploratory analysis

### Recommended Hybrid Implementation:

**Phase 1: Start with Native VSS (Better for your use case)**
Since you're building a data pipeline (not a chatbot), start with direct DuckDB VSS:

1. **Company Embeddings** - Use `sic_description` for similarity
2. **Filing Embeddings** - Use existing `filing_summaries.summary_text`  
3. **Prioritization Enhancement** - Integrate semantic similarity into stock gathering

**Phase 2: Add LlamaIndex Layer (Optional, for RAG)**
If you want Q&A capabilities later:
- Ingest full filing documents
- Enable "chat with your SEC filings" features
- Build analyst assistant tools

Would you like me to:
1. **Implement native DuckDB VSS** with sentence-transformers (full control, integrates with your prioritizer)
2. **Implement LlamaIndex RAG** for document Q&A (easier setup, less control)
3. **Build both** (hybrid approach - VSS for data pipeline, RAG for queries)

Which approach fits your needs better?

---

Future: Domain + Quality Boosters

- Reranker between VSS and LLM:
    - Model: prefer `bge-reranker-base-v1.5` (cross-encoder), alternative: `e5-mistral-7b-instruct-reranker` (hosted) if we need higher recall.
    - Flow: VSS retrieves top 50–100 by cosine → reranker scores pairs (query,candidate) → keep top 5–20 for generation.
    - Mac local: run with PyTorch MPS; batch size 8–16 for stability; measure p95 latency and Recall@10 vs weak labels (same SIC / tag).
    - Fallback: if reranker not available, return VSS results directly.

- Constrained generation (answering discipline):
    - JSON mode with a strict schema: question, answer_text, citations[{accession_number, form, cik, url}], disclaimer.
    - Guardrails: require citations for any numeric or factual claims; instruct “No speculation; cite or say ‘insufficient evidence.’”
    - Post-validate: reject non-JSON or missing citations; re-prompt with clarified constraints when invalid.

- Minimal plan to adopt later:
    1) Evaluate reranker: bake-off on 50k sample, VSS top-50 → rerank; pick the model if Recall@10 improves ≥5% with acceptable latency.
    2) Integrate reranker: small module used by MCP tools `similar_filings`/`similar_companies` (optional flag `use_reranker`).
    3) Enforce schema: define JSON schema and validator; wire into agent runtime; add tests for malformed outputs.

- Setup notes (local):
    - Dependencies: `pip install torch sentence-transformers FlagEmbedding` (or `huggingface-hub` as needed).
    - MPS tips: set `PYTORCH_MPS_HIGH_WATERMARK_RATIO=0.0`; keep batches modest to avoid OOM.
    - Target latency: VSS (<100ms) + reranker (<600ms) → total p95 < 1.0–1.5s.
