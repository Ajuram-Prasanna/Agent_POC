import os
import json
import asyncio
import httpx
import pandas as pd
import chainlit as cl
from dotenv import load_dotenv
from utils.utils import SYSTEM_PROMPT, SQL_GENERATOR_SYSTEM_PROMPT, TOOL_SCHEMAS
from typing import Dict, Optional
import requests
import time
import numpy as np
from datetime import datetime, date
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import threading
from contextlib import contextmanager
from uuid import uuid1
import psycopg2
from psycopg2 import OperationalError, InterfaceError
from matplotlib import pyplot as plt
import seaborn as sns
from matplotlib.figure import Figure
from azure.ai.projects import AIProjectClient
from azure.identity import ClientSecretCredential
import atexit
from openai import AzureOpenAI
import asyncio
import json
from concurrent.futures import ThreadPoolExecutor
import time
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
import threading
from datetime import datetime, timedelta
import hashlib
import pickle
from functools import lru_cache
import weakref

load_dotenv(override=True)

class ContentFilterError(Exception):
    """Custom exception for content filtering"""
    pass

class SQLQueryCache:
    """In-memory cache for SQL query results with TTL and size limits"""
    
    def __init__(self, max_size=100, ttl_seconds=300):  # 5 minute TTL
        self.cache = {}
        self.access_times = {}
        self.max_size = max_size
        self.ttl_seconds = ttl_seconds
        self.lock = threading.RLock()
    
    def _generate_key(self, sql_query: str) -> str:
        """Generate a consistent hash key for SQL query"""
        # Normalize the SQL query for consistent caching
        normalized = ' '.join(sql_query.lower().split())
        return hashlib.md5(normalized.encode()).hexdigest()
    
    def _cleanup_expired(self):
        """Remove expired entries"""
        current_time = time.time()
        expired_keys = [
            key for key, access_time in self.access_times.items()
            if current_time - access_time > self.ttl_seconds
        ]
        
        for key in expired_keys:
            self.cache.pop(key, None)
            self.access_times.pop(key, None)
    
    def _evict_lru(self):
        """Evict least recently used entries if cache is full"""
        if len(self.cache) >= self.max_size:
            # Remove oldest entry
            oldest_key = min(self.access_times, key=self.access_times.get)
            self.cache.pop(oldest_key, None)
            self.access_times.pop(oldest_key, None)
    
    def get(self, sql_query: str) -> Optional[pd.DataFrame]:
        """Get cached result for SQL query"""
        with self.lock:
            key = self._generate_key(sql_query)
            self._cleanup_expired()
            
            if key in self.cache:
                self.access_times[key] = time.time()
                print(f"[CACHE HIT] SQL query cache hit for key: {key[:8]}...")
                return self.cache[key].copy()  # Return copy to prevent modification
            
            return None
    
    def set(self, sql_query: str, result: pd.DataFrame):
        """Cache SQL query result"""
        with self.lock:
            if result.empty:
                return  # Don't cache empty results
            
            key = self._generate_key(sql_query)
            self._cleanup_expired()
            self._evict_lru()
            
            self.cache[key] = result.copy()
            self.access_times[key] = time.time()
            print(f"[CACHE SET] SQL query result cached for key: {key[:8]}... (rows: {len(result)})")
    
    def clear(self):
        """Clear all cache entries"""
        with self.lock:
            self.cache.clear()
            self.access_times.clear()

class AgentClientCache:
    """Singleton cache for expensive Azure AI Project clients and threads"""
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if not self._initialized:
            self.project_client = None
            self.credential = None
            self.thread_pool = {}  # Thread pool cache
            self.thread_lock = threading.RLock()
            self.client_lock = threading.RLock()
            self._initialized = True
    
    def get_project_client(self) -> AIProjectClient:
        """Get or create cached project client"""
        with self.client_lock:
            if self.project_client is None:
                print("[CACHE] Creating new Azure AI Project client...")
                
                if self.credential is None:
                    self.credential = ClientSecretCredential(
                        tenant_id=os.getenv("AI_AGENT_TENANT_ID"),
                        client_id=os.getenv("AI_AGENT_CLIENT_ID"),
                        client_secret=os.getenv("AI_AGENT_CLIENT_SECRET_VALUE")
                    )
                
                self.project_client = AIProjectClient(
                    endpoint=os.getenv("PROJECT_ENDPOINT"),
                    credential=self.credential
                )
                
                # Enable auto function calls once when client is created
                self.project_client.agents.enable_auto_function_calls([process_user_query])
                print("[CACHE] Azure AI Project client cached successfully")
            
            return self.project_client
    
    def get_or_create_thread(self, session_id: str) -> str:
        """Get existing thread ID or create new one for session"""
        with self.thread_lock:
            if session_id in self.thread_pool:
                thread_id = self.thread_pool[session_id]
                print(f"[CACHE HIT] Using cached thread: {thread_id} for session: {session_id[:8]}...")
                return thread_id
            
            try:
                print(f"[CACHE MISS] Creating new thread for session: {session_id[:8]}...")
                client = self.get_project_client()
                thread = client.agents.threads.create()
                self.thread_pool[session_id] = thread.id
                print(f"[CACHE SET] Thread {thread.id} cached for session: {session_id[:8]}...")
                return thread.id
            except Exception as e:
                print(f"[ERROR] Failed to create thread for session {session_id}: {e}")
                raise
    
    def cleanup_session(self, session_id: str):
        """Clean up cached resources for a session"""
        with self.thread_lock:
            if session_id in self.thread_pool:
                thread_id = self.thread_pool.pop(session_id)
                print(f"[CACHE CLEANUP] Removed thread {thread_id} for session {session_id[:8]}...")

# Initialize global caches
sql_cache = SQLQueryCache(max_size=50, ttl_seconds=600)  # 10 minute TTL for SQL
agent_cache = AgentClientCache()

# ====================== OPENAI CLIENT CACHING ======================

@lru_cache(maxsize=1)
def get_sql_client():
    """Cache the OpenAI client - only one instance needed"""
    print("[CACHE] Creating OpenAI SQL client...")
    return AzureOpenAI(
        api_key=os.getenv("SQL_GEN_API_KEY"),
        api_version="2025-01-01-preview", 
        azure_endpoint=os.getenv("SQL_GEN_ENDPOINT").split('/openai/')[0]
    )

# Connection management class
class SnowflakeConnectionManager:
    def __init__(self):
        self._connection = None
        self._lock = threading.Lock()
        self._last_used = None
        
    def _create_connection(self):
        """Create a new Snowflake connection"""
        conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password="&*x!Aiy(ce69",
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DB'),
            role='DEVELOPER',
            # Add connection parameters to handle timeouts better
            network_timeout=60,  # 60 seconds for network operations
            login_timeout=30,    # 30 seconds for login
            session_parameters={
                'QUERY_TAG': 'chainlit_app',
            }
        )
        
        return conn
    
    def _is_connection_valid(self):
        """Check if the current connection is still valid"""
        if self._connection is None:
            return False
            
        return True
    
    @contextmanager
    def get_connection(self):
        """Get a valid connection with automatic retry"""
        with self._lock:
            max_retries = 5
            retry_count = 0
            
            while retry_count < max_retries:
                try:
                    # Check if we need a new connection
                    if not self._is_connection_valid():
                        print("[DEBUG] Creating new Snowflake connection...")
                        if self._connection:
                            try:
                                self._connection.close()
                            except:
                                pass
                        self._connection = self._create_connection()
                        print("[DEBUG] New Snowflake connection created successfully")
                    
                    self._last_used = time.time()
                    yield self._connection
                    return
                    
                except Exception as e:
                    retry_count += 1
                    print(f"[ERROR] Connection attempt {retry_count} failed: {e}")
                    
                    if self._connection:
                        try:
                            self._connection.close()
                        except:
                            pass
                        self._connection = None
                    
                    if retry_count < max_retries:
                        print(f"[DEBUG] Retrying connection in 2 seconds...")
                        time.sleep(2)
                    else:
                        raise Exception(f"Failed to establish Snowflake connection after {max_retries} attempts: {e}")
    
    def close(self):
        """Close the connection"""
        with self._lock:
            if self._connection:
                try:
                    self._connection.close()
                except:
                    pass
                self._connection = None

# ====================== CONNECTION CACHING ======================

class EnhancedSnowflakeConnectionManager(SnowflakeConnectionManager):
    """Enhanced connection manager with connection pooling"""
    
    def __init__(self, pool_size=3):
        super().__init__()
        self.pool_size = pool_size
        self.connection_pool = []
        self.pool_lock = threading.RLock()
        self.connection_params = {
            'user': os.getenv("SNOWFLAKE_USER"),
            'password': "&*x!Aiy(ce69",
            'account': os.getenv("SNOWFLAKE_ACCOUNT"),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
            'database': os.getenv('SNOWFLAKE_DB'),
            'role': 'DEVELOPER',
            'network_timeout': 60,
            'login_timeout': 30,
            'session_parameters': {'QUERY_TAG': 'chainlit_app'},
        }
    
    def _create_pooled_connection(self):
        """Create a connection for the pool"""
        return snowflake.connector.connect(**self.connection_params)
    
    @contextmanager
    def get_connection(self):
        """Get connection from pool or create new one"""
        connection = None
        
        with self.pool_lock:
            # Try to get from pool first
            while self.connection_pool:
                connection = self.connection_pool.pop()
                try:
                    # Test if connection is still valid
                    cursor = connection.cursor()
                    cursor.execute("SELECT 1")
                    cursor.close()
                    print("[CACHE HIT] Using pooled Snowflake connection")
                    break
                except:
                    # Connection is dead, try next one
                    try:
                        connection.close()
                    except:
                        pass
                    connection = None
            
            # If no valid connection from pool, create new one
            if connection is None:
                print("[CACHE MISS] Creating new Snowflake connection")
                connection = self._create_pooled_connection()
        
        try:
            yield connection
        finally:
            # Return connection to pool if it's still valid and pool isn't full
            with self.pool_lock:
                if len(self.connection_pool) < self.pool_size:
                    try:
                        # Quick test to ensure connection is still valid
                        cursor = connection.cursor()
                        cursor.execute("SELECT 1")
                        cursor.close()
                        self.connection_pool.append(connection)
                        print("[CACHE SET] Connection returned to pool")
                    except:
                        # Connection is dead, close it
                        try:
                            connection.close()
                        except:
                            pass
                else:
                    # Pool is full, close the connection
                    try:
                        connection.close()
                    except:
                        pass

# Modified process_user_query function - only returns SQL
def process_user_query(query):
    return asyncio.run(async_process_user_query(query))

# Enhanced process_user_query function that only returns SQL
async def async_process_user_query(query):
    """
    Generate SQL from user query - NO EXECUTION
    This function should only return SQL, execution happens in execute_tool_call
    """
    start_sql_gen = time.time()
    interaction_id = cl.user_session.get("current_interaction_id")
    session_id = cl.user_session.get("session_id")

    try:
        # Generate SQL using the agent
        sql = await invoke_sql_model(query)

        # Check if agent returned an error message instead of SQL
        if sql.startswith("Failed to invoke agent:") or sql.startswith("Error:"):
            print(f"[ERROR] Agent failed: {sql}")
            return {
                "message": sql,
                "sql_generated": False,
                "success": False
            }
        
        # Clean up the SQL if it has formatting
        if sql.startswith("```sql"):
            sql = sql.replace("```sql", "").replace("```", "").strip()
        elif sql.startswith("```"):
            sql = sql.replace("```", "").strip()
            
        sql_gen_duration = time.time() - start_sql_gen
        print(f"[SQL GENERATION TIME] {sql_gen_duration:.2f} seconds")
        print(f"[GENERATED SQL] {sql}")

        log_interaction_details(
            message_id=interaction_id, 
            session_id=session_id, 
            process_name="sql_generation", 
            processor="o3-mini", 
            process_input=query, 
            process_output=sql.replace('\n', ' ')
        )

        # Return only the SQL - NO DATA RETRIEVAL HERE
        return {
            "sql": sql,
            "sql_generated": True,
            "success": True
        }
        
    except ContentFilterError:
        log_interaction_details(
            message_id=interaction_id, 
            session_id=session_id, 
            process_name="content_filter", 
            processor="azure_guardrails", 
            process_input=query, 
            process_output="BLOCKED_INAPPROPRIATE_CONTENT"
        )
        
        return {
            "message": "I'm sorry, but I cannot process this request as it may contain inappropriate content.",
            "content_filtered": True,
            "success": False
        }
        
    except Exception as e:
        print(f"[ERROR] process_user_query failed: {e}")
        
        log_interaction_details(
            message_id=interaction_id, 
            session_id=session_id, 
            process_name="sql_generation_error", 
            processor="o3-mini", 
            process_input=query, 
            process_output=f"ERROR: {str(e)}"
        )
        
        return {
            "message": f"An error occurred: {str(e)}",
            "success": False
        }

# New function to execute SQL and return DataFrame (called outside agent)
async def execute_sql_and_get_dataframe(sql):
    """Execute SQL query with caching"""
    start_time = time.time()
    interaction_id = cl.user_session.get("current_interaction_id")
    session_id = cl.user_session.get("session_id")
    
    try:
        print(f"[EXECUTING SQL] {sql}")
        
        # Check cache first
        cached_result = sql_cache.get(sql)
        if cached_result is not None:
            duration = time.time() - start_time
            print(f"[CACHE HIT] SQL execution time: {duration:.2f} seconds (cached)")
            print(f"[CACHE HIT] SQL result: {len(cached_result)} rows")
            
            log_interaction_details(
                message_id=interaction_id,
                session_id=session_id,
                process_name="sql_execution_cached",
                processor="Snowflake_Cache",
                process_input=sql.replace('\n', ' '),
                process_output=f"CACHE_HIT: {len(cached_result)} rows returned"
            )
            
            return {
                "data": cached_result,
                "sql": sql,
                "success": True,
                "error": None,
                "cached": True
            }
        
        # Execute query if not cached
        df = await asyncio.to_thread(query_to_dataframe, sql)

        duration = time.time() - start_time
        print(f"[SQL EXECUTION TIME] {duration:.2f} seconds")
        print(f"[SQL RESULT] Rows: {len(df)}")
        
        if not df.empty:
            print(f"[SQL RESULT] Columns: {list(df.columns)}")
            print(f"[SQL RESULT] Sample data:\n{df.head(2).to_string()}")
            
            # Cache the result
            sql_cache.set(sql, df)
            
        # Log successful execution
        log_interaction_details(
            message_id=interaction_id,
            session_id=session_id,
            process_name="sql_execution",
            processor="Snowflake",
            process_input=sql.replace('\n', ' '),
            process_output=f"SUCCESS: {len(df)} rows returned"
        )
        
        return {
            "data": df,
            "sql": sql,
            "success": True,
            "error": None,
            "cached": False
        }

    except Exception as e:
        print(f"[ERROR] SQL Execution failed: {e}")
        
        # Log execution error
        log_interaction_details(
            message_id=interaction_id,
            session_id=session_id,
            process_name="sql_execution_error",
            processor="Snowflake",
            process_input=sql.replace('\n', ' '),
            process_output=f"ERROR: {str(e)}"
        )
        
        return {
            "data": pd.DataFrame(),
            "sql": sql,
            "success": False,
            "error": str(e),
            "cached": False
        }

async def execute_tool_call(tool_call):
    """Execute a tool call made by the conversational agent"""
    try:
        tool_name = tool_call.function.name
        arguments = json.loads(tool_call.function.arguments)
        
        print(f"[DEBUG] ===== TOOL CALL EXECUTION =====")
        print(f"[DEBUG] Tool name: {tool_name}")
        print(f"[DEBUG] Arguments: {arguments}")
        
        if tool_name == "process_user_query":
            query = arguments.get("query", "")
            print(f"[DEBUG] Processing query through tool: '{query}'")
            
            # Step 1: Get SQL from the agent (NO execution in this step)
            sql_result = await async_process_user_query(query)
            
            print(f"[DEBUG] SQL generation result: {sql_result}")
            
            if not sql_result.get("success", False):
                # Return error if SQL generation failed
                error_message = sql_result.get("message", "Unknown error in SQL generation")
                if sql_result.get("content_filtered", False):
                    return {
                        "message": "I'm sorry, but I cannot process this request as it may contain inappropriate content. Please rephrase your question in a professional manner related to loan data queries.",
                        "sql_generated": False
                    }
                else:
                    user_friendly_message = get_user_friendly_error_message(error_message)
                    return {
                        "message": user_friendly_message,
                        "sql_generated": False
                    }
            
            sql = sql_result.get("sql", "")
            if not sql.strip():
                return {
                    "message": "No SQL query was generated. Please try rephrasing your question.",
                    "sql_generated": False
                }
            
            # Step 2: Execute SQL - THIS IS THE ONLY EXECUTION
            print(f"[DEBUG] Executing SQL (single execution): {sql}")
            data_result = await execute_sql_and_get_dataframe(sql)
            
            if not data_result.get("success", False):
                return {
                    "message": f"SQL generated successfully, but execution failed: {data_result.get('error', 'Unknown error')}",
                    "sql": sql,
                    "sql_generated": True,
                    "data_retrieved": False
                }
            
            df = data_result.get("data")
            if df.empty:
                print("[DEBUG] Query returned no rows.")
                return {
                    "message": "Query executed successfully but returned no rows.",
                    "sql": sql,
                    "sql_generated": True,
                    "data_retrieved": True,
                    "row_count": 0
                }
            
            # Store the complete result for display outside the agent
            complete_result = {
                "data": df,
                "debug": {"sql": sql, "raw_result": df.to_dict(orient="records")},
                "success": True
            }
            
            print(f"[DEBUG] Storing DataFrame result for later display...")
            print(f"[DEBUG] DataFrame shape: {df.shape}")
            
            # Generate a unique result key
            interaction_id = cl.user_session.get("current_interaction_id")
            result_key = f"{interaction_id}_{hash(query)}"
            
            # Store the result globally and in session
            _global_query_results[result_key] = complete_result
            cl.user_session.set("latest_query_result", complete_result)
            cl.user_session.set("latest_result_key", result_key)
            
            # Return a simple message to the agent
            return {
                "message": f"Successfully retrieved {len(df)} records with {len(df.columns)} columns. Data will be displayed shortly.",
                "sql": sql,
                "sql_generated": True,
                "data_retrieved": True,
                "row_count": len(df),
                "column_count": len(df.columns),
                "result_key": result_key
            }
            
        else:
            print(f"[ERROR] Unknown tool: {tool_name}")
            return {"error": f"Unknown tool: {tool_name}"}
            
    except Exception as e:
        print(f"[ERROR] ===== TOOL EXECUTION FAILED =====")
        print(f"[ERROR] Error: {e}")
        import traceback
        print(f"[ERROR] Full traceback:")
        traceback.print_exc()
        
        return {
            "message": f"Tool execution failed: {str(e)}",
            "sql_generated": False,
            "data_retrieved": False
        }

def is_content_filtered_error(error_message):
    """
    Check if the error message indicates content filtering/guardrails
    """
    if not error_message:
        return False
        
    error_message = str(error_message).lower()
    content_filter_indicators = [
        'content filter',
        'content_filter',
        'responsible ai',
        'safety',
        'inappropriate',
        'harmful',
        'content policy',
        'violated',
        'blocked'
    ]
    return any(indicator in error_message for indicator in content_filter_indicators)

def get_user_friendly_error_message(error):
    """
    Convert technical errors to user-friendly messages
    """
    error_message = str(error)
    
    if is_content_filtered_error(error_message):
        return "I'm sorry, but I cannot process this request as it may contain inappropriate content. Please rephrase your question in a professional manner related to loan data queries."
    
    elif "authentication" in error_message.lower():
        return "There's an authentication issue with the AI service. Please try again later."
    
    elif "rate limit" in error_message.lower():
        return "The service is currently experiencing high demand. Please wait a moment and try again."
    
    elif "failed" in error_message.lower() and "agent" in error_message.lower():
        return "I couldn't process your request due to a service issue. Please try rephrasing your question."
    
    else:
        return f"I encountered an unexpected error: {str(error)}. Please try again with a different question."





# Update the invoke_sql_model function to handle Azure Guardrails responses
async def invoke_sql_model(query: str) -> str:
    """Generate SQL using cached OpenAI client"""
    
    try:
        print(f"[DEBUG] Generating SQL using cached OpenAI client for query: {query}")
        
        messages = [
            {"role": "system", "content": SQL_GENERATOR_SYSTEM_PROMPT},
            {"role": "user", "content": query}
        ]
        
        def _invoke_openai_sync():
            try:
                # Use cached client
                client = get_sql_client()
                response = client.chat.completions.create(
                    model=os.getenv("SQL_GEN_DEPLOYMENT_NAME"),
                    messages=messages,
                    max_completion_tokens=1000
                )
                
                if response.choices and len(response.choices) > 0:
                    sql_content = response.choices[0].message.content
                    print(f"[DEBUG] OpenAI SQL response: {sql_content}")
                    return sql_content
                else:
                    return "Error: No response from OpenAI"
                    
            except Exception as e:
                error_message = str(e).lower()
                print(f"[ERROR] OpenAI API call failed: {e}")
                
                if is_azure_guardrails_error(e):
                    print(f"[CONTENT_FILTER] Azure Guardrails blocked inappropriate content")
                    raise ContentFilterError("Content blocked by Azure Guardrails")
                
                return f"Error: OpenAI API call failed - {str(e)}"
        
        return await asyncio.to_thread(_invoke_openai_sync)
        
    except ContentFilterError:
        raise
    except Exception as e:
        print(f"[ERROR] SQL generation failed: {e}")
        return f"Failed to generate SQL: {str(e)}"

# Update the agent_call function to handle Azure Guardrails in conversational agent
async def agent_call(query: str):
    """Call the cached Azure AI Foundry conversational agent"""
    try:
        # Use cached project client
        project_client = agent_cache.get_project_client()
        
        if project_client is None:
            raise Exception("Azure AI Project client not initialized")

        # Get cached or create thread for this session
        session_id = cl.user_session.get("session_id")
        convo_thread_id = agent_cache.get_or_create_thread(session_id)
        
        print(f"[DEBUG] === STREAMING AGENT CALL WITH CACHING ===")
        print(f"[DEBUG] Using cached thread: {convo_thread_id}")
        
        # Add user message to thread
        print(f"[DEBUG] Adding user message to thread")
        
        try:
            message = project_client.agents.messages.create(
                thread_id=convo_thread_id,
                role="user",
                content=query
            )
        except Exception as msg_error:
            raise msg_error

        # Create run
        print(f"[DEBUG] Creating run...")
        try:
            run = project_client.agents.runs.create(
                thread_id=convo_thread_id,
                agent_id=os.getenv("CONVO_AGENT_ID")
            )
        except Exception as run_error:
            raise run_error
        
        print(f"[DEBUG] Run created: {run.id}, Status: {run.status}")

        
        initial_response_sent = False
        tool_calls_executed = []
        final_response = None

        while run.status in ["queued", "in_progress", "requires_action"]:
            print(f"[DEBUG] Run status: {run.status}")
            
            if run.status == "requires_action":
                print(f"[DEBUG] Run requires action - handling tool calls")
                
                required_action = run.required_action
                if required_action and hasattr(required_action, 'submit_tool_outputs'):
                    tool_calls = required_action.submit_tool_outputs.tool_calls
                    
                    if not initial_response_sent:
                        messages = list(project_client.agents.messages.list(thread_id=convo_thread_id))
                        
                        for msg in messages:
                            if (msg.role == 'assistant' and 
                                hasattr(msg, 'run_id') and msg.run_id == run.id):
                                if msg.content and len(msg.content) > 0:
                                    initial_text = msg.content[0].text.value
                                    if initial_text and not any(keyword in initial_text.lower() 
                                                              for keyword in ['successfully retrieved', 'function', 'tool']):
                                        print(f"[DEBUG] Found initial response: {initial_text}")
                                        
                                        initial_msg = cl.Message(content=initial_text)
                                        await initial_msg.send()
                                        initial_response_sent = True
                                        break
                    
                    tool_outputs = []
                    for tool_call in tool_calls:
                        print(f"[DEBUG] Executing tool: {tool_call.function.name}")
                        
                        try:
                            result = await execute_tool_call(tool_call)
                            tool_outputs.append({
                                "tool_call_id": tool_call.id,
                                "output": json.dumps(result)
                            })
                            tool_calls_executed.append(tool_call.function.name)
                            print(f"[DEBUG] Tool executed successfully: {tool_call.function.name}")
                            
                        except Exception as tool_error:
                            print(f"[ERROR] Tool execution failed: {tool_error}")
                            tool_outputs.append({
                                "tool_call_id": tool_call.id,
                                "output": json.dumps({"error": str(tool_error)})
                            })

                    print(f"[DEBUG] Submitting {len(tool_outputs)} tool outputs")
                    try:
                        run = project_client.agents.runs.submit_tool_outputs(
                            thread_id=convo_thread_id,
                            run_id=run.id,
                            tool_outputs=tool_outputs
                        )
                    except Exception as submit_error:
                        raise submit_error
            
            await asyncio.sleep(0.2)
            
            try:
                run = project_client.agents.runs.get(
                    thread_id=convo_thread_id,
                    run_id=run.id
                )
            except Exception as get_error:
                raise get_error

        print(f"[DEBUG] Final run status: {run.status}")

        if run.status == "completed":
            messages = list(project_client.agents.messages.list(thread_id=convo_thread_id))
            
            for msg in messages:
                if (msg.role == 'assistant' and 
                    hasattr(msg, 'run_id') and msg.run_id == run.id):
                    if msg.content and len(msg.content) > 0:
                        msg_text = msg.content[0].text.value
                        
                        if initial_response_sent and msg_text and len(msg_text.strip()) > 10:
                            if not any(keyword in msg_text.lower() 
                                     for keyword in ['successfully retrieved', 'function result']):
                                final_response = msg_text
                                break
                        elif not initial_response_sent:
                            final_response = msg_text
                            break

        if not final_response:
            if tool_calls_executed:
                final_response = "I've retrieved the requested data for you."
            else:
                final_response = "I apologize, but I wasn't able to process your request properly. Please try rephrasing your question."

        return {
            "initial_response": None,
            "final_response": final_response,
            "steps": [],
            "tool_calls_executed": tool_calls_executed
        }

    except Exception as e:
        print(f"[ERROR] Streaming agent call failed: {e}")
        import traceback
        print(f"[ERROR] Full traceback: {traceback.format_exc()}")
        return {
            "initial_response": None,
            "final_response": f"I encountered an error processing your request: {str(e)}. Please try again.",
            "steps": [],
            "tool_calls_executed": []
        }
    
def is_azure_guardrails_response(response_text):
    """
    Check if a response text indicates that Azure Guardrails has filtered the content
    
    Args:
        response_text (str): The response text to check
        
    Returns:
        bool: True if the response indicates content filtering, False otherwise
    """
    if not response_text:
        return False
    
    response_lower = str(response_text).lower()
    
    # Common Azure Guardrails response patterns
    guardrails_patterns = [
        # Direct content filter messages
        "i'm sorry, but i cannot process this request",
        "i cannot process this request as it may contain inappropriate content",
        "i can't process your request as it contains inappropriate content",
        "sorry, i can't process your request",
        "i cannot assist with this request",
        "i'm not able to process this request",
        
        # Content policy violations
        "content policy",
        "content violation",
        "violates our content policy",
        "against our content guidelines",
        "inappropriate content",
        "harmful content",
        "blocked by content filter",
        "content has been filtered",
        
        # Safety and responsible AI messages
        "responsible ai",
        "safety guidelines",
        "safety policy",
        "safety filter",
        "content safety",
        
        # Professional rephrasing suggestions (common in guardrails responses)
        "please rephrase your question in a professional manner",
        "please try rephrasing your question",
        "rephrase your request",
        
        # Generic refusal patterns that might indicate filtering
        "i cannot help with that",
        "i'm unable to assist with this",
        "i cannot provide assistance with",
        
        # Azure-specific error patterns
        "content_filter",
        "content filter result",
        "filtered due to",
        "blocked content"
    ]
    
    # Check if any guardrails pattern is found
    for pattern in guardrails_patterns:
        if pattern in response_lower:
            return True
    
    # Additional check for responses that contain both refusal and content-related keywords
    refusal_keywords = ["sorry", "cannot", "unable", "can't", "won't", "refuse"]
    content_keywords = ["content", "inappropriate", "professional", "rephrase", "policy"]
    
    has_refusal = any(keyword in response_lower for keyword in refusal_keywords)
    has_content_ref = any(keyword in response_lower for keyword in content_keywords)
    
    if has_refusal and has_content_ref and len(response_text) < 500:
        # Short responses with both refusal and content references are likely guardrails responses
        return True
    
    # Check for very short generic refusals that might be filtered responses
    if len(response_text.strip()) < 100 and any(keyword in response_lower for keyword in 
                                              ["sorry", "cannot", "unable", "can't"]):
        # This might be a filtered response, but we need to be more specific
        if any(keyword in response_lower for keyword in 
               ["request", "process", "assist", "help", "content"]):
            return True
    
    return False

# New function to detect Azure Guardrails errors
def is_azure_guardrails_error(error):
    """
    Detect if an error is from Azure Guardrails content filtering
    """
    if not error:
        return False
    
    error_message = str(error).lower()
    
    # Common Azure Guardrails error indicators
    guardrails_indicators = [
        'content filter',
        'content_filter',
        'responsible ai',
        'content filtering',
        'harmful content',
        'inappropriate content',
        'content policy',
        'safety filter',
        'blocked by policy',
        'content violation',
        'filtered',
        'jailbreak',
        'prompt injection',
        'content_filter_result',
        'content_filter_status',
        'finish_reason',
        'content_filtered'
    ]
    
    # Check if any indicator is present
    for indicator in guardrails_indicators:
        if indicator in error_message:
            return True
    
    # Check for specific Azure OpenAI error codes related to content filtering
    # These are common HTTP status codes and error patterns from Azure OpenAI content filtering
    if any(code in error_message for code in ['400', '429', 'bad request']):
        if any(term in error_message for term in ['content', 'filter', 'policy', 'safety']):
            return True
    
    # Check for specific error types that might indicate content filtering
    if hasattr(error, 'code'):
        filter_codes = ['content_filter', 'responsible_ai_policy_violation', 'content_policy_violation']
        if any(code in str(error.code).lower() for code in filter_codes):
            return True
    
    # Check for response object with content filter information
    if hasattr(error, 'response'):
        try:
            response_text = str(error.response).lower()
            if any(indicator in response_text for indicator in guardrails_indicators):
                return True
        except:
            pass
    
    return False

class PostgresConnection:
    def __init__(self, host, dbname, user, password, port=5432, sslmode="require"):
        self.conn_params = {
            "host": host,
            "dbname": dbname,
            "user": user,
            "password": password,
            "port": port,
            "sslmode": sslmode
        }
        self.conn = None

    def get_connection(self):
        """
        Returns a valid PostgreSQL connection. Recreates it if closed or timed out.
        """
        if self.conn is None or self.conn.closed != 0:
            self._create_connection()
        else:
            try:
                # Ping the connection
                cur = self.conn.cursor()
                cur.execute("SELECT 1;")
                cur.close()
            except (OperationalError, InterfaceError):
                print("Reconnecting to Postgres...")
                self._create_connection()
        return self.conn

    def _create_connection(self):
        try:
            self.conn = psycopg2.connect(**self.conn_params)
            print("New Postgres connection established")
        except Exception as e:
            print("Failed to connect to Postgres:", e)
            self.conn = None



# Global connection manager
connection_manager = EnhancedSnowflakeConnectionManager(pool_size=3)

# Function to create appropriate plots based on DataFrame content and relationships
def create_plots_for_dataframe(df, max_plots=3):
    """
    Create appropriate plots based on DataFrame content and column relationships
    Returns a list of matplotlib figures
    """
    plots = []
    
    if df.empty or len(df) == 0:
        return plots
    
    # Skip plotting if DataFrame has more than 4 columns
    if len(df.columns) > 4:
        print(f"[DEBUG] Skipping plot generation: DataFrame has {len(df.columns)} columns (limit is 4)")
        return plots
    
    # Set style for better-looking plots
    plt.style.use('default')
    sns.set_palette("husl")
    
    # LIMIT DATA FOR PLOTTING - Use only first 20 rows for visualization
    plot_df = df.head(20).copy()
    
    # Get numeric and categorical columns from the plotting subset
    numeric_cols = plot_df.select_dtypes(include=['int64', 'int32', 'float64', 'float32']).columns.tolist()
    categorical_cols = plot_df.select_dtypes(include=['object', 'category']).columns.tolist()
    date_cols = plot_df.select_dtypes(include=['datetime64[ns]', 'datetime64[ns, UTC]']).columns.tolist()
    
    plot_count = 0
    
    print(f"[DEBUG] Creating plots for DataFrame with {len(df)} rows (using first {len(plot_df)} for visualization)")
    print(f"[DEBUG] Numeric columns: {numeric_cols}")
    print(f"[DEBUG] Categorical columns: {categorical_cols}")
    
    # PRIORITY 1: Categorical vs Numeric relationship (most common business case)
    # Create separate bar charts for each numeric column vs the categorical column
    if categorical_cols and numeric_cols and plot_count < max_plots:
        try:
            # Find the best categorical column (preferably names, then IDs, then types)
            cat_col = None
            for col in categorical_cols:
                if any(keyword in col.lower() for keyword in ['name', 'title', 'description']):
                    cat_col = col
                    break
            if cat_col is None:
                for col in categorical_cols:
                    if any(keyword in col.lower() for keyword in ['type', 'category', 'status']):
                        cat_col = col
                        break
            if cat_col is None:  # If no preferred column, use the first categorical
                cat_col = categorical_cols[0]
            
            unique_vals = plot_df[cat_col].nunique()
            if unique_vals <= 20:  # Reasonable number for visualization (increased from 50 since we're limiting to 20 rows)
                
                # Create separate plots for each numeric column
                for num_col in numeric_cols:
                    if plot_count >= max_plots:
                        break
                        
                    try:
                        fig, ax = plt.subplots(figsize=(max(10, unique_vals * 0.6), 6))
                        
                        # Use the actual values from the plotting dataframe
                        x_values = plot_df[cat_col].values
                        y_values = plot_df[num_col].values
                        
                        # Create bar chart
                        bars = ax.bar(range(len(x_values)), y_values, alpha=0.7)
                        
                        # Customize the plot
                        ax.set_xticks(range(len(x_values)))
                        ax.set_xticklabels(x_values, rotation=45, ha='right')
                        
                        # Update title to indicate if data is limited
                        title = f'{num_col} by {cat_col}'
                        if len(df) > 20:
                            title += f' (Top 20 of {len(df)} records)'
                        ax.set_title(title)
                        
                        ax.set_xlabel(cat_col)
                        ax.set_ylabel(num_col)
                        ax.grid(True, alpha=0.3, axis='y')
                        
                        # Format y-axis labels for large numbers
                        if len(y_values) > 0 and max(y_values) > 1000000:
                            ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x/1000000:.1f}M'))
                        elif len(y_values) > 0 and max(y_values) > 1000:
                            ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x/1000:.1f}K'))
                        
                        # Add value labels on bars
                        for i, (bar, value) in enumerate(zip(bars, y_values)):
                            # Format the label based on the size of the number
                            if value > 1000000:
                                label = f'{value/1000000:.1f}M'
                            elif value > 1000:
                                label = f'{value/1000:.1f}K'
                            else:
                                label = str(int(value)) if not pd.isna(value) else '0'
                                
                            ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(y_values) * 0.01,
                                   label, ha='center', va='bottom', fontweight='bold')
                        
                        plt.tight_layout()
                        plots.append((f'categorical_vs_{num_col.lower().replace(" ", "_")}', fig))
                        plot_count += 1
                        print(f"[DEBUG] Created categorical vs numeric plot: {cat_col} vs {num_col}")
                        
                    except Exception as e:
                        print(f"[ERROR] Creating plot for {num_col}: {e}")
                        plt.close(fig)  # Clean up if there's an error
                
        except Exception as e:
            print(f"[ERROR] Creating categorical vs numeric plots: {e}")
    
    # PRIORITY 2: Time series plot if we have date and numeric columns
    if date_cols and numeric_cols and plot_count < max_plots:
        try:
            date_col = date_cols[0]
            # Find the best numeric column for time series
            num_col = None
            for col in numeric_cols:
                if any(keyword in col.lower() for keyword in ['count', 'amount', 'value', 'total', 'sum']):
                    num_col = col
                    break
            if num_col is None:
                num_col = numeric_cols[0]
            
            # Sort by date and clean data from plotting subset
            time_plot_df = plot_df[[date_col, num_col]].sort_values(date_col).dropna()
            
            if len(time_plot_df) > 1:
                fig, ax = plt.subplots(figsize=(12, 6))
                ax.plot(time_plot_df[date_col], time_plot_df[num_col], marker='o', linewidth=2, markersize=4)
                ax.set_xlabel(date_col)
                ax.set_ylabel(num_col)
                
                # Update title to indicate if data is limited
                title = f'{num_col} over time'
                if len(df) > 20:
                    title += f' (Top 20 of {len(df)} records)'
                ax.set_title(title)
                
                ax.grid(True, alpha=0.3)
                
                # Rotate x-axis labels for better readability
                plt.xticks(rotation=45)
                plt.tight_layout()
                plots.append(('time_series', fig))
                plot_count += 1
                print(f"[DEBUG] Created time series plot: {num_col} over {date_col}")
        except Exception as e:
            print(f"[ERROR] Creating time series plot: {e}")
    
    # PRIORITY 3: Multiple numeric columns - only create scatter plots if no categorical data
    # This avoids redundant plots when we already have categorical vs numeric relationships
    if len(numeric_cols) >= 2 and not categorical_cols and plot_count < max_plots:
        try:
            # If we have exactly 2 numeric columns, create a scatter plot
            if len(numeric_cols) == 2:
                x_col, y_col = numeric_cols[0], numeric_cols[1]
                
                # Clean the data from plotting subset
                scatter_plot_df = plot_df[[x_col, y_col]].replace([float('inf'), float('-inf')], None).dropna()
                
                if len(scatter_plot_df) > 1:
                    fig, ax = plt.subplots(figsize=(10, 6))
                    ax.scatter(scatter_plot_df[x_col], scatter_plot_df[y_col], alpha=0.6, s=50)
                    ax.set_xlabel(x_col)
                    ax.set_ylabel(y_col)
                    
                    # Update title to indicate if data is limited
                    title = f'{y_col} vs {x_col}'
                    if len(df) > 20:
                        title += f' (Top 20 of {len(df)} records)'
                    ax.set_title(title)
                    
                    ax.grid(True, alpha=0.3)
                    
                    plt.tight_layout()
                    plots.append(('scatter_plot', fig))
                    plot_count += 1
                    print(f"[DEBUG] Created scatter plot: {y_col} vs {x_col}")
            
            # If we have more than 2 numeric columns, show distribution of the most relevant one
            elif len(numeric_cols) > 2 and plot_count < max_plots:
                # Choose the most interesting numeric column for distribution
                num_col = None
                for col in numeric_cols:
                    if any(keyword in col.lower() for keyword in ['amount', 'value', 'price', 'cost', 'revenue']):
                        num_col = col
                        break
                if num_col is None:
                    num_col = numeric_cols[0]
                
                clean_data = plot_df[num_col].replace([float('inf'), float('-inf')], None).dropna()
                
                if len(clean_data) > 0 and clean_data.nunique() > 1:
                    fig, ax = plt.subplots(figsize=(10, 6))
                    ax.hist(clean_data, bins=min(10, len(clean_data.unique())), alpha=0.7, edgecolor='black')
                    
                    # Update title to indicate if data is limited
                    title = f'Distribution of {num_col}'
                    if len(df) > 20:
                        title += f' (Top 20 of {len(df)} records)'
                    ax.set_title(title)
                    
                    ax.set_xlabel(num_col)
                    ax.set_ylabel('Frequency')
                    ax.grid(True, alpha=0.3)
                    
                    plt.tight_layout()
                    plots.append(('distribution', fig))
                    plot_count += 1
                    print(f"[DEBUG] Created distribution plot for: {num_col}")
                    
        except Exception as e:
            print(f"[ERROR] Creating numeric plots: {e}")
    
    # PRIORITY 4: Pure categorical analysis (only if no numeric data available)
    if not numeric_cols and categorical_cols and plot_count < max_plots:
        try:
            # Take the first categorical column with reasonable number of unique values
            for cat_col in categorical_cols[:2]:
                unique_vals = plot_df[cat_col].nunique()
                if 2 <= unique_vals <= 20:
                    fig, ax = plt.subplots(figsize=(10, 6))
                    
                    # Count plot - this is the only time we do value_counts
                    value_counts = plot_df[cat_col].value_counts()
                    ax.bar(range(len(value_counts)), value_counts.values)
                    ax.set_xticks(range(len(value_counts)))
                    ax.set_xticklabels(value_counts.index, rotation=45, ha='right')
                    
                    # Update title to indicate if data is limited
                    title = f'Distribution of {cat_col}'
                    if len(df) > 20:
                        title += f' (Top 20 of {len(df)} records)'
                    ax.set_title(title)
                    
                    ax.set_xlabel(cat_col)
                    ax.set_ylabel('Count')
                    ax.grid(True, alpha=0.3, axis='y')
                    
                    plt.tight_layout()
                    plots.append(('categorical_distribution', fig))
                    plot_count += 1
                    print(f"[DEBUG] Created categorical distribution plot for: {cat_col}")
                    break
        except Exception as e:
            print(f"[ERROR] Creating categorical distribution plot: {e}")
    
    print(f"[DEBUG] Generated {len(plots)} plots from {len(df)} total rows (plotted first {len(plot_df)})")
    return plots

async def handle_dataframe_with_plots(result, interaction_id, session_id):
    """
    Enhanced function to handle DataFrame display with optional plotting
    """
    print(f"[DEBUG] handle_dataframe_with_plots called with result type: {type(result)}")
    print(f"[DEBUG] result keys: {result.keys() if isinstance(result, dict) else 'not a dict'}")
    
    if isinstance(result["data"], pd.DataFrame) and not result["data"].empty:
        df = result["data"]
        
        try:
            print(f"[DEBUG] Original DataFrame shape: {df.shape}")
            print(f"[DEBUG] DataFrame head:\n{df.head()}")
            
            # Clean the DataFrame for display
            df_clean = clean_dataframe_for_display(df)
            
            print(f"[DEBUG] Cleaned DataFrame shape: {df_clean.shape}")
            print(f"[DEBUG] DataFrame columns: {list(df_clean.columns)}")
            print(f"[DEBUG] DataFrame dtypes:\n{df_clean.dtypes}")

            # Create elements list starting with the dataframe
            elements = []
            
            # Add the dataframe element with more detailed error handling
            try:
                print(f"[DEBUG] Creating cl.Dataframe element...")
                
                # Ensure all data is JSON serializable
                df_for_display = df_clean.copy()
                
                # Convert any remaining problematic types
                for col in df_for_display.columns:
                    if df_for_display[col].dtype == 'object':
                        df_for_display[col] = df_for_display[col].astype(str)
                    elif pd.api.types.is_numeric_dtype(df_for_display[col]):
                        # Fill any NaN values in numeric columns
                        df_for_display[col] = df_for_display[col].fillna(0)
                
                print(f"[DEBUG] DataFrame ready for display, shape: {df_for_display.shape}")
                print(f"[DEBUG] Sample data: {df_for_display.iloc[0].to_dict() if len(df_for_display) > 0 else 'empty'}")
                
                dataframe_element = cl.Dataframe(
                    name="Query Results",
                    data=df_for_display,
                    display="inline",
                )
                elements.append(dataframe_element)
                print(f"[DEBUG] Successfully created DataFrame element")
                
            except Exception as df_error:
                print(f"[ERROR] DataFrame element creation failed: {df_error}")
                print(f"[ERROR] DataFrame error traceback:", exc_info=True)
                
                # Try alternative approaches
                try:
                    # Approach 1: Convert to dict and back
                    print(f"[DEBUG] Trying dict conversion approach...")
                    df_dict = df_clean.to_dict('records')
                    df_from_dict = pd.DataFrame(df_dict)
                    
                    dataframe_element = cl.Dataframe(
                        name="Query Results",
                        data=df_from_dict,
                        display="inline",
                    )
                    elements.append(dataframe_element)
                    print(f"[DEBUG] Dict conversion approach succeeded")
                    
                except Exception as dict_error:
                    print(f"[ERROR] Dict conversion failed: {dict_error}")
                    
                    try:
                        # Approach 2: Send as text table
                        print(f"[DEBUG] Falling back to text table...")
                        text_preview = df_clean.to_string(index=False, max_rows=20)
                        fallback_msg = cl.Message(
                            content=f"**Query Results ({len(df)} rows):**\n\n```\n{text_preview}\n```"
                        )
                        await fallback_msg.send()
                        print(f"[DEBUG] Text table fallback sent")
                        return
                        
                    except Exception as text_error:
                        print(f"[ERROR] Text fallback also failed: {text_error}")
                        error_msg = cl.Message(
                            content=f"Data retrieved successfully ({len(df)} rows, {len(df.columns)} columns), but display failed. Please check the logs."
                        )
                        await error_msg.send()
                        return
            
            # Generate plots if the dataset is suitable for visualization
            if len(df_clean) > 1:  # Need at least 2 rows for meaningful plots
                print("[DEBUG] Generating plots...")
                try:
                    plots = create_plots_for_dataframe(df_clean)
                    
                    # Add plot elements
                    for plot_type, fig in plots:
                        try:
                            plot_element = cl.Pyplot(
                                name=f"{plot_type}",
                                figure=fig,
                                display="inline"
                            )
                            elements.append(plot_element)
                            print(f"[DEBUG] Added {plot_type} to elements")
                        except Exception as plot_error:
                            print(f"[ERROR] Failed to create plot element {plot_type}: {plot_error}")
                            plt.close(fig)  # Clean up the figure
                            
                except Exception as plots_error:
                    print(f"[ERROR] Plot generation failed: {plots_error}")
            
            # Send message with all elements (dataframe + plots)
            try:
                content_text = f"Found {len(df)} records."
                if len(elements) > 1:  # More than just the dataframe
                    content_text += f" Generated {len(elements)-1} visualization(s) based on the data."
                
                print(f"[DEBUG] Sending message with {len(elements)} elements")
                preview_msg = cl.Message(
                    content=content_text,
                    elements=elements,
                )
                await preview_msg.send()
                print(f"[DEBUG] Message with DataFrame sent successfully")
                
            except Exception as send_error:
                print(f"[ERROR] Failed to send message with elements: {send_error}")
                
                # Final fallback - send just the text
                fallback_content = f"Query completed successfully. Found {len(df)} records, but unable to display the table. Please check the application logs."
                fallback_msg = cl.Message(content=fallback_content)
                await fallback_msg.send()
                
        except Exception as e:
            print(f"[ERROR] DataFrame processing failed: {e}")
            import traceback
            traceback.print_exc()
            
            # Send error message with basic info
            error_msg = cl.Message(
                content=f"Data retrieved successfully ({len(df)} rows, {len(df.columns)} columns), but display failed. Error: {str(e)}"
            )
            await error_msg.send()
    
    else:
        print(f"[DEBUG] No DataFrame to display or DataFrame is empty")
        if isinstance(result.get("data"), str):
            # If data is a string message, send it
            msg = cl.Message(content=result["data"])
            await msg.send()


def query_to_dataframe(sql_query):
    """Execute SQL query and return DataFrame with connection management"""
    try:
        with connection_manager.get_connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(sql_query)
                # Fetch all rows and get column names
                rows = cur.fetchall()
                cols = [col[0] for col in cur.description]
                # Convert to DataFrame
                df = pd.DataFrame(rows, columns=cols)
                return df
            except Exception as e:
                print(f"[ERROR] SQL execution failed: {e}")
                return pd.DataFrame()
            finally:
                cur.close()
    except Exception as e:
        print(f"[ERROR] Connection failed: {e}")
        return pd.DataFrame()

def clean_dataframe_for_display(df):
    """Clean DataFrame to ensure it displays properly in Chainlit"""
    if df.empty:
        return df
    
    # Create a copy to avoid modifying the original
    df_clean = df.copy()
    
    # Handle different data types that might cause issues
    for col in df_clean.columns:
        # Convert problematic data types to strings
        if df_clean[col].dtype == 'object':
            # Handle None, NaN, and complex objects
            df_clean[col] = df_clean[col].apply(lambda x: 
                '' if pd.isna(x) or x is None 
                else str(x) if not isinstance(x, (dict, list)) 
                else json.dumps(x) if isinstance(x, (dict, list))
                else str(x)
            )
        elif df_clean[col].dtype in ['datetime64[ns]', 'datetime64[ns, UTC]']:
            # Convert datetime to string
            df_clean[col] = df_clean[col].dt.strftime('%Y-%m-%d %H:%M:%S').fillna('')
        elif df_clean[col].dtype in ['int64', 'int32', 'float64', 'float32']:
            # Handle NaN in numeric columns
            df_clean[col] = df_clean[col].fillna(0)
        elif df_clean[col].dtype == 'bool':
            # Convert boolean to string
            df_clean[col] = df_clean[col].astype(str)
    
    # Remove any remaining completely empty rows
    df_clean = df_clean.dropna(how='all')
    
    # Ensure column names are strings and don't contain special characters
    df_clean.columns = [str(col).replace(',', '_').replace('.', '_') for col in df_clean.columns]
    
    # Limit the number of rows for display performance
    if len(df_clean) > 500:
        df_clean = df_clean.head(500)
    
    return df_clean


# Global variable to track the current feedback message
current_feedback_msg = None
db = PostgresConnection(
    host=os.getenv('PGHOST'), 
    dbname=os.getenv('PGDATABASE'), 
    user=os.getenv('PGUSER'), 
    password=os.getenv('PGPASSWORD')
)
postgres_conn = db

PROJECT_ENDPOINT = os.getenv("PROJECT_ENDPOINT")



SQL_GEN_DEPLOYMENT = os.getenv("SQL_GEN_DEPLOYMENT_NAME")

CONVO_AGENT_MODEL = os.getenv("CONVO_AGENT_MODEL")
CONVO_AGENT_ID = os.getenv("CONVO_AGENT_ID")

credential = ClientSecretCredential(
    tenant_id=os.getenv("AI_AGENT_TENANT_ID"),
    client_id=os.getenv("AI_AGENT_CLIENT_ID"),
    client_secret=os.getenv("AI_AGENT_CLIENT_SECRET_VALUE")
)



_global_query_results = {}

print("[INFO] Azure AI Project client initialized successfully")

def log_interaction_details(session_id, message_id, process_name, processor, process_input, process_output):
    """
    Insert interaction details into loan_agent_logs table.
    Timestamp is handled automatically by the DB using CURRENT_TIMESTAMP.
    """
    global postgres_conn
    
    try:
        conn = postgres_conn.get_connection()
        cursor = conn.cursor()
        log_id = str(uuid1())
        query = """
            INSERT INTO loan_agent_logs (
                id, session_id, message_id, process_name, processor, process_input, process_output, _ts
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        """
        cursor.execute(query, (log_id, session_id, message_id, process_name, processor, process_input, process_output))
        conn.commit()
        cursor.close()
        print(f"[LOGGED] Log {log_id} added successfully.")
    except Exception as e:
        print(f"[FAILED] Log {log_id}: {e}")


tool_functions = {
    "process_user_query": lambda query: process_user_query(query)
}

# Add cleanup on application shutdown
atexit.register(connection_manager.close)

# Modification 1: Update the feedback action callbacks to remove buttons after clicking

@cl.action_callback("thumbs_up")
async def on_thumbs_up(action):
    """Handle thumbs up feedback"""
    
    # Get the interaction ID from session and update feedback
    interaction_id = cl.user_session.get("current_interaction_id")
    session_id = cl.user_session.get("session_id")
    
    log_interaction_details(message_id = interaction_id, session_id = session_id, process_name = "user_feedback", processor = None, process_input = "POSITIVE", process_output = "Thank you for your positive feedback!")
    
    print(f"[FEEDBACK] 👍 User LIKED this reply")
    
    feedback_msg = cl.Message(content="Thank you for your positive feedback! 👍")

    # Remove current feedback message after clicking
    global current_feedback_msg
    if current_feedback_msg is not None:
        try:
            await current_feedback_msg.remove()
        except:
            pass  # Ignore if message is already removed or doesn't exist
        current_feedback_msg = None
    
    await feedback_msg.send()

@cl.action_callback("thumbs_down")
async def on_thumbs_down(action):
    """Handle thumbs down feedback"""

    # Get the interaction ID from session and update feedback
    interaction_id = cl.user_session.get("current_interaction_id")
    session_id = cl.user_session.get("session_id")

    log_interaction_details(message_id = interaction_id, session_id = session_id, process_name = "user_feedback", processor = None, process_input = "NEGATIVE", process_output = "Thank you for your feedback! We'll work to improve our responses.")
    
    print(f"[FEEDBACK] 👎 User DISLIKED this reply")
    
    feedback_msg = cl.Message(content="Thank you for your feedback! We'll work to improve our responses. 👎")
    
    # Remove current feedback message after clicking
    global current_feedback_msg
    if current_feedback_msg is not None:
        try:
            await current_feedback_msg.remove()
        except:
            pass  # Ignore if message is already removed or doesn't exist
        current_feedback_msg = None
    
    await feedback_msg.send()

def create_feedback_actions(user_input: str, response_content: str):
    """Create thumbs up/down action buttons with correct payload format"""
    feedback_data = {
        "query": user_input,
        "response": response_content[:500],  # Limit length to avoid issues
        "timestamp": datetime.now().isoformat()
    }
    
    return [
        cl.Action(
            name="thumbs_up",
            payload=feedback_data,
            label="👍 Helpful"
        ),
        cl.Action(
            name="thumbs_down", 
            payload=feedback_data,
            label="👎 Not Helpful"
        )
    ]

@cl.set_starters
async def starters():
    return [
        cl.Starter(
            label="💰 Show all loans",
            message="Show me all loans",
        ),
        cl.Starter(
            label="👥 Count loans by agent",
            message="Count loans by agent",
        ),
        cl.Starter(
            label="✅ Show all approved loans",
            message="Show me approved loans",
        ),
        cl.Starter(
            label="🏢 Show business loans",
            message="Show me business loans",
        ),
    ]

# Modified startup function - remove SQL agent thread creation
@cl.on_chat_start
async def start():
    """Enhanced startup with caching"""
    print("=== CACHED AGENT INITIALIZATION ===")
    print(f"Convo Agent ID: {os.getenv('CONVO_AGENT_ID')}")
    print(f"SQL Generation: OpenAI {os.getenv('SQL_GEN_DEPLOYMENT_NAME')}")
    
    # Initialize cached clients
    try:
        # This will cache the project client and create a thread
        session_id = str(uuid1())
        cl.user_session.set("session_id", session_id)
        
        # Pre-warm the agent cache
        agent_cache.get_project_client()
        print("[CACHE] Azure AI Project client pre-warmed")
        
        # Pre-warm SQL client cache
        get_sql_client()
        print("[CACHE] OpenAI SQL client pre-warmed")
        
    except Exception as e:
        print(f"❌ Agent cache setup error: {e}")
    
    cl.user_session.set("message_history", [])
    print("[DEBUG] ===== Cached chat session initialized =====")

@cl.on_chat_end
async def end_chat():
    """Clean up cached resources when chat ends"""
    print("[DEBUG] Chat session ending, cleaning up caches...")
    
    session_id = cl.user_session.get("session_id")
    if session_id:
        # Clean up cached thread for this session
        agent_cache.cleanup_session(session_id)
        
        # Clean up any stored results for this session
        keys_to_remove = [key for key in _global_query_results.keys() if session_id in key]
        for key in keys_to_remove:
            del _global_query_results[key]
        print(f"[DEBUG] Cleaned up {len(keys_to_remove)} stored results")
    
    # Clear session data
    cl.user_session.set("message_history", [])
    cl.user_session.set("latest_query_result", None)
    cl.user_session.set("latest_result_key", None)
    
    print("[DEBUG] Cached chat cleanup completed")

# Add cache statistics endpoint (optional for monitoring)
def get_cache_stats():
    """Get cache statistics for monitoring"""
    return {
        "sql_cache_size": len(sql_cache.cache),
        "sql_cache_hits": getattr(sql_cache, 'hits', 0),
        "agent_thread_pool_size": len(agent_cache.thread_pool),
        "connection_pool_size": len(connection_manager.connection_pool)
    }

# Update the handle_message function to properly handle content filtering
@cl.on_message
async def handle_message(msg):
    total_start_time = time.time()
    query = msg.content
    hist = cl.user_session.get("message_history") or []

    if not hist:
        hist.append({"role": "system", "content": SYSTEM_PROMPT})

    hist.append({"role": "user", "content": query})

    interaction_id = str(uuid1())
    cl.user_session.set("current_interaction_id", interaction_id)
    session_id = cl.user_session.get("session_id")

    # Remove previous feedback buttons when new query arrives
    global current_feedback_msg
    if current_feedback_msg is not None:
        try:
            await current_feedback_msg.remove()
            print("[DEBUG] Removed previous feedback buttons")
        except:
            pass
        current_feedback_msg = None

    # Clear any previous results
    cl.user_session.set("latest_query_result", None)
    cl.user_session.set("latest_result_key", None)

    # Log the interaction input
    log_interaction_details(
        message_id=interaction_id,
        session_id=session_id,
        process_name="user_input",
        processor=None,
        process_input=query,
        process_output=None
    )

    print(f"[DEBUG] ===== Starting new streaming message processing =====")
    print(f"[DEBUG] User query: {query}")
    print(f"[DEBUG] Interaction ID: {interaction_id}")

    try:
        agent_start = time.time()
        
        # Call the streaming agent - initial response is sent within this call
        result = await agent_call(query)
        
        agent_latency = time.time() - agent_start
        print(f"[DEBUG] Streaming agent call completed in {agent_latency:.2f}s")

        # Check if content was filtered by the agent
        if result.get("content_filtered", False):
            print(f"[CONTENT_FILTER] Content was filtered by agent, sending filtered response")
            
            # Send the filtered response
            filtered_msg = cl.Message(content=result["final_response"])
            await filtered_msg.send()
            
            # Log the filtered response
            log_interaction_details(
                message_id=interaction_id,
                session_id=session_id,
                process_name="filtered_response",
                processor="azure_guardrails",
                process_input=query,
                process_output=result["final_response"]
            )
            
            # Don't continue with normal processing
            total_duration = time.time() - total_start_time
            print(f"[TOTAL TIME FOR FILTERED QUESTION] {total_duration:.2f} seconds")
            return

        # Check if the final response itself indicates content filtering
        final_response = result.get("final_response", "")
        if final_response and is_azure_guardrails_response(final_response):
            print(f"[CONTENT_FILTER] Final response indicates content filtering")
            print(f"[CONTENT_FILTER] Response: {final_response}")
            
            
            
            # Send a consistent filtered response
            filtered_response = "I'm sorry, but I cannot process this request as it may contain inappropriate content. Please rephrase your question in a professional manner related to loan data queries."

            # Log this as filtered content
            log_interaction_details(
                message_id=interaction_id,
                session_id=session_id,
                process_name="content_filter",
                processor="Azure Guardrails",
                process_input=query,
                process_output=filtered_response
            )

            filtered_msg = cl.Message(content=filtered_response)
            await filtered_msg.send()
            
            
            total_duration = time.time() - total_start_time
            print(f"[TOTAL TIME FOR FILTERED QUESTION] {total_duration:.2f} seconds")
            return

        # Continue with normal processing for non-filtered content
        # Check for stored results from tool execution
        stored_result = None
        result_key = None
        
        # Try multiple approaches to get the stored result
        try:
            result_key = cl.user_session.get("latest_result_key")
            if result_key and result_key in _global_query_results:
                stored_result = _global_query_results[result_key]
                print(f"[DEBUG] Found result using session key: {result_key}")
        except:
            pass
            
        if not stored_result:
            try:
                stored_result = cl.user_session.get("latest_query_result")
                if stored_result:
                    print(f"[DEBUG] Found result in session storage")
            except:
                pass
        
        if not stored_result:
            query_hash = hash(query)
            for key, value in _global_query_results.items():
                if key.startswith(interaction_id) or key.endswith(str(query_hash)):
                    stored_result = value
                    result_key = key
                    print(f"[DEBUG] Found result by searching global storage: {key}")
                    break

        # Display the DataFrame if we found results
        if stored_result and stored_result.get("success"):
            print(f"[DEBUG] Displaying stored result...")
            await handle_dataframe_with_plots(stored_result, interaction_id, session_id)
            
            # Clean up the result
            if result_key and result_key in _global_query_results:
                del _global_query_results[result_key]
                print(f"[DEBUG] Cleaned up global result: {result_key}")

        # Send final response only if we have one and tools were executed
        if final_response and result.get("tool_calls_executed"):
            final_msg = cl.Message(content=final_response)
            await final_msg.send()

            hist.append({"role": "assistant", "content": final_response})
            cl.user_session.set("message_history", hist)

            # Log the final response
            log_interaction_details(
                message_id=interaction_id,
                session_id=session_id,
                process_name="text_generation",
                processor="Convo Agent",
                process_input=query,
                process_output=final_response.replace('\n', ' ')
            )

            # Add feedback buttons
            feedback_actions = create_feedback_actions(query, final_response)
            feedback_msg = cl.Message(
                content="Please rate this response:",
                actions=feedback_actions
            )
            await feedback_msg.send()
            current_feedback_msg = feedback_msg

        elif final_response and not result.get("tool_calls_executed"):
            # If no tools were executed, send the final response as a regular conversation
            final_msg = cl.Message(content=final_response)
            await final_msg.send()
            
            # ADD THIS LOGGING HERE - this is what's missing!
            hist.append({"role": "assistant", "content": final_response})
            cl.user_session.set("message_history", hist)

            # Log the normal conversation response (THIS IS THE MISSING PART)
            log_interaction_details(
                message_id=interaction_id,
                session_id=session_id,
                process_name="text_generation",
                processor="Convo Agent",
                process_input=query,
                process_output=final_response.replace('\n', ' ')
            )

            # Add feedback buttons for normal responses too
            feedback_actions = create_feedback_actions(query, final_response)
            feedback_msg = cl.Message(
                content="Please rate this response:",
                actions=feedback_actions
            )
            await feedback_msg.send()
            current_feedback_msg = feedback_msg

        elif final_response and not result.get("tool_calls_executed"):
            # If no tools were executed, send the final response as a regular conversation
            final_msg = cl.Message(content=final_response)
            await final_msg.send()

        total_duration = time.time() - total_start_time
        print(f"[TOTAL TIME FOR QUESTION] {total_duration:.2f} seconds")

    except Exception as e:
        print(f"[ERROR] handle_message failed: {e}")
        import traceback
        print(f"[ERROR] Full traceback: {traceback.format_exc()}")

        user_friendly_message = get_user_friendly_error_message(e)
        error_msg = cl.Message(content=user_friendly_message)
        await error_msg.send()

        log_interaction_details(
            message_id=interaction_id,
            session_id=session_id,
            process_name="conversation_error",
            processor="system",
            process_input=query,
            process_output=f"ERROR: {str(e)}"
        )