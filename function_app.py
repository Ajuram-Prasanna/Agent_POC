import httpx
import logging
import os
import json
import asyncio
import time
import pandas as pd
import snowflake.connector
import threading
from contextlib import contextmanager
from dotenv import load_dotenv
import azure.functions as func
from azure.ai.projects import AIProjectClient
from azure.identity import ClientSecretCredential
from openai import AzureOpenAI
from azure.cosmos import CosmosClient
import requests

load_dotenv()

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


connection_manager = EnhancedSnowflakeConnectionManager(pool_size=3)

sql_client = AzureOpenAI(
    api_key=os.getenv("SQL_GEN_API_KEY"),
    api_version="2025-01-01-preview", 
    azure_endpoint=os.getenv("SQL_GEN_ENDPOINT").split('/openai/')[0]
)

credential = ClientSecretCredential(
    tenant_id=os.getenv("AI_AGENT_TENANT_ID"),
    client_id=os.getenv("AI_AGENT_CLIENT_ID"),
    client_secret=os.getenv("AI_AGENT_CLIENT_SECRET_VALUE")
)

project_client = AIProjectClient(
    endpoint=os.getenv("PROJECT_ENDPOINT"),
    credential=credential
)

COSMOS_DB_NAME = os.getenv('COSMOS_DB_NAME')
COSMOS_ENDPOINT = os.getenv('COSMOS_ENDPOINT')
COSMOS_KEY = os.getenv('COSMOS_KEY')

cosmos_client = CosmosClient(COSMOS_ENDPOINT, COSMOS_KEY)

database = cosmos_client.get_database_client(COSMOS_DB_NAME)

COSMOS_CONTAINERS = ['configs', 'utils']

container_clients = dict()

for container_name in COSMOS_CONTAINERS:
    container_clients[container_name] = database.get_container_client(container_name)

func_app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

def get_from_cosmos(container_name = '', query = ''):
    return list(container_clients[container_name].query_items(
        query=query,
        enable_cross_partition_query=True
    ))

def format_sse_event(event_type: str, data: dict) -> str:
    """Format data as Server-Sent Event"""
    return f"event: {event_type}\ndata: {json.dumps(data)}\n\n"

@func_app.route(route="get_sql_generator_prompt", methods=["GET"], auth_level=func.AuthLevel.FUNCTION)
async def get_sql_generator_prompt(req: func.HttpRequest) -> func.HttpResponse:
    SQL_GENERATOR_SYSTEM_PROMPT = get_from_cosmos('utils', 'SELECT VALUE c.util_content FROM c WHERE c.util_type = "sql_generator_prompt"')[0]
    return func.HttpResponse(
        json.dumps({"prompt": SQL_GENERATOR_SYSTEM_PROMPT}),
        status_code=200,
    )

@func_app.route(route="invoke_convo_agent", methods=["POST"], auth_level=func.AuthLevel.FUNCTION)
async def invoke_convo_agent(req: func.HttpRequest) -> func.HttpResponse:
    """Stream agent responses using Server-Sent Events"""
    
    try:
        req_body = req.get_json()
        message = req_body.get("message")
        convo_thread_id = req_body.get("convo_thread_id")
        logging.warning(f"Received message: {message} with thread ID: {convo_thread_id}")

        if not convo_thread_id:
            convo_thread_id = project_client.agents.threads.create().id
        
        # Collect all events in a list
        events = []
        
        # Send initial event
        events.append(format_sse_event("status", {"message": "Processing your request..."}))
        
        # Add user message to thread
        try:
            project_client.agents.messages.create(
                thread_id=convo_thread_id,
                role="user",
                content=message
            )
            events.append(format_sse_event("status", {"message": "Message added to thread"}))
        except Exception as msg_error:
            events.append(format_sse_event("error", {"message": f"Failed to add message: {str(msg_error)}"}))
            response_body = "".join(events)
            return func.HttpResponse(
                response_body,
                mimetype="text/plain",
                headers={
                    "Content-Type": "text/event-stream",
                    "Cache-Control": "no-cache",
                    "Connection": "keep-alive",
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Headers": "Content-Type"
                }
            )

        # Create run
        try:
            run = project_client.agents.runs.create(
                thread_id=convo_thread_id,
                agent_id=os.getenv("CONVO_AGENT_ID")
            )
            events.append(format_sse_event("status", {"message": f"Run created: {run.id}"}))
        except Exception as run_error:
            events.append(format_sse_event("error", {"message": f"Failed to create run: {str(run_error)}"}))
            response_body = "".join(events)
            return func.HttpResponse(
                response_body,
                mimetype="text/plain",
                headers={
                    "Content-Type": "text/event-stream",
                    "Cache-Control": "no-cache",
                    "Connection": "keep-alive",
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Headers": "Content-Type"
                }
            )
        
        initial_response_sent = False
        tool_calls_executed = []

        while run.status in ["queued", "in_progress", "requires_action"]:
            events.append(format_sse_event("status", {"message": f"Run status: {run.status}"}))
            
            if run.status == "requires_action":
                required_action = run.required_action
                if required_action and hasattr(required_action, 'submit_tool_outputs'):
                    tool_calls = required_action.submit_tool_outputs.tool_calls
                    
                    # Send initial response if available
                    if not initial_response_sent:
                        messages = list(project_client.agents.messages.list(thread_id=convo_thread_id))
                        
                        for msg in messages:
                            if (msg.role == 'assistant' and 
                                hasattr(msg, 'run_id') and msg.run_id == run.id):
                                if msg.content and len(msg.content) > 0:
                                    initial_text = msg.content[0].text.value
                                    if initial_text and not any(keyword in initial_text.lower() 
                                                              for keyword in ['successfully retrieved', 'function', 'tool']):
                                        events.append(format_sse_event("initial_response", {"content": initial_text}))
                                        initial_response_sent = True
                                        break
                    
                    # Execute tools
                    tool_outputs = []
                    for tool_call in tool_calls:
                        events.append(format_sse_event("tool_execution", {
                            "tool_name": tool_call.function.name,
                            "status": "executing"
                        }))
                        
                        try:
                            # Handle both sync and async tool calls
                            result = execute_tool_call(tool_call)
                            
                            # Check if result is a coroutine (async function result)
                            if hasattr(result, '__await__'):
                                result = await result
                            
                            # Ensure result is a dictionary
                            if not isinstance(result, dict):
                                result = {"output": str(result)}
                            
                            tool_outputs.append({
                                "tool_call_id": tool_call.id,
                                "output": json.dumps(result)
                            })
                            tool_calls_executed.append(tool_call.function.name)
                            
                            events.append(format_sse_event("tool_execution", {
                                "tool_name": tool_call.function.name,
                                "status": "completed",
                                "output": str(result.get("sql", str(result)))
                            }))
                            
                        except Exception as tool_error:
                            logging.error(f"[ERROR] Tool execution error: {tool_error}")
                            logging.error(f"[ERROR] Tool call object: {tool_call}")
                            
                            tool_outputs.append({
                                "tool_call_id": tool_call.id,
                                "output": json.dumps({"error": str(tool_error)})
                            })
                            
                            events.append(format_sse_event("tool_execution", {
                                "tool_name": tool_call.function.name,
                                "status": "error",
                                "error": str(tool_error)
                            }))

                    # Submit tool outputs
                    try:
                        run = project_client.agents.runs.submit_tool_outputs(
                            thread_id=convo_thread_id,
                            run_id=run.id,
                            tool_outputs=tool_outputs
                        )
                        events.append(format_sse_event("status", {"message": "Tool outputs submitted"}))
                    except Exception as submit_error:
                        events.append(format_sse_event("error", {"message": f"Failed to submit tool outputs: {str(submit_error)}"}))
                        response_body = "".join(events)
                        return func.HttpResponse(
                            response_body,
                            mimetype="text/plain",
                            headers={
                                "Content-Type": "text/event-stream",
                                "Cache-Control": "no-cache",
                                "Connection": "keep-alive",
                                "Access-Control-Allow-Origin": "*",
                                "Access-Control-Allow-Headers": "Content-Type"
                            }
                        )
            
            await asyncio.sleep(0.2)
            
            try:
                run = project_client.agents.runs.get(
                    thread_id=convo_thread_id,
                    run_id=run.id
                )
            except Exception as get_error:
                events.append(format_sse_event("error", {"message": f"Failed to get run status: {str(get_error)}"}))
                response_body = "".join(events)
                return func.HttpResponse(
                    response_body,
                    mimetype="text/plain",
                    headers={
                        "Content-Type": "text/event-stream",
                        "Cache-Control": "no-cache",
                        "Connection": "keep-alive",
                        "Access-Control-Allow-Origin": "*",
                        "Access-Control-Allow-Headers": "Content-Type"
                    }
                )

        # Send final response
        if run.status == "completed":
            messages = list(project_client.agents.messages.list(thread_id=convo_thread_id))
            
            final_response = None
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
                final_response = "I've retrieved the requested data for you." if tool_calls_executed else "I apologize, but I wasn't able to process your request properly."

            events.append(format_sse_event("final_response", {"content": final_response}))
            events.append(format_sse_event("completed", {
                "tool_calls_executed": tool_calls_executed,
                "status": "success"
            }))
        else:
            events.append(format_sse_event("error", {"message": f"Run ended with status: {run.status}"}))

        # Join all events into a single response
        response_body = "".join(events)

        logging.warning("RIGHT HERE")
        logging.error('-' * 50)
        logging.warning(events)
        
        return func.HttpResponse(
            response_body,
            mimetype="text/plain",
            headers={
                "Content-Type": "text/event-stream",
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type"
            }
        )

    except Exception as e:
        error_event = format_sse_event("error", {"message": f"Streaming agent call failed: {str(e)}"})
        return func.HttpResponse(
            error_event,
            mimetype="text/plain",
            headers={
                "Content-Type": "text/event-stream",
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type"
            }
        )



async def execute_tool_call(tool_call):
    """Execute a tool call made by the conversational agent"""
    try:
        tool_name = tool_call.function.name
        arguments = json.loads(tool_call.function.arguments)
        
        logging.warning(f"[DEBUG] ===== TOOL CALL EXECUTION =====")
        logging.warning(f"[DEBUG] Tool name: {tool_name}")
        logging.warning(f"[DEBUG] Arguments: {arguments}")
        
        if tool_name == "process_user_query":
            query = arguments.get("query", "")
            logging.warning(f"[DEBUG] Processing query through tool: '{query}'")
            
            # Step 1: Get SQL from the agent (NO execution in this step)
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    url="http://localhost:7071/api/invoke_sql_model",
                    json={"query": query},
                    timeout=30  # 30 second timeout
                )

            sql = response.json().get("sql", "")

            # Check if agent returned an error message instead of SQL
            if sql.startswith("Failed to invoke agent:") or sql.startswith("Error:") or sql == "":
                print(f"[ERROR] Agent failed: {sql}")
                sql_result = {
                    "message": sql,
                    "sql_generated": False,
                    "success": False
                }
            
            else:
                sql_result = {
                    "sql_generated": True,
                    "success": True
                }
            
            # Clean up the SQL if it has formatting
            if sql.startswith("```sql"):
                sql = sql.replace("```sql", "").replace("```", "").strip()
            elif sql.startswith("```"):
                sql = sql.replace("```", "").strip()
            
            sql_result["message"] = sql
            
            logging.warning(f"[DEBUG] SQL generation result: {sql_result}")
            
            if not sql_result.get("success", False):
                # Return error if SQL generation failed
                error_message = sql_result.get("message", "Unknown error in SQL generation")
                if sql_result.get("content_filtered", False):
                    return {
                        "message": "I'm sorry, but I cannot process this request as it may contain inappropriate content. Please rephrase your question in a professional manner related to loan data queries.",
                        "sql_generated": False
                    }
                else:
                    return {
                        "message": error_message,
                        "sql_generated": False
                    }
            
            sql = sql_result.get("message", "")
            if not sql.strip():
                return {
                    "message": "No SQL query was generated. Please try rephrasing your question.",
                    "sql_generated": False
                }
            
            # Return only SQL to the agent
            return {
                "message": f"Successfully generated SQL for user input.",
                "sql": sql
            }
            
        else:
            logging.error(f"[ERROR] Unknown tool: {tool_name}")
            return {"error": f"Unknown tool: {tool_name}"}
            
    except Exception as e:
        logging.error(f"[ERROR] ===== TOOL EXECUTION FAILED =====")
        logging.error(f"[ERROR] Error: {e}")
        import traceback
        logging.error(f"[ERROR] Full traceback:")
        traceback.print_exc()
        
        return {
            "message": f"Tool execution failed: {str(e)}",
            "sql_generated": False,
            "data_retrieved": False
        }

# Enhanced process_user_query function that only returns SQL
async def async_process_user_query(query):
    start_sql_gen = time.time()

    try:
        # Generate SQL using the agent
        response = requests.post(url="http://localhost:7071/api/invoke_sql_model", json={
            "query": query
        })

        sql = response.json().get("sql", "")

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

        # Return only the SQL - NO DATA RETRIEVAL HERE
        return {
            "sql": sql,
            "sql_generated": True,
            "success": True
        }
        
        
    except Exception as e:
        print(f"[ERROR] process_user_query failed: {e}")
        
        return {
            "message": f"An error occurred: {str(e)}",
            "success": False
        }




@func_app.route(route="invoke_sql_model", methods=["POST"], auth_level=func.AuthLevel.FUNCTION)
def invoke_sql_model(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP endpoint to generate SQL from natural language query"""
    
    try:
        # Get request body
        req_body = req.get_json()
        if not req_body:
            return func.HttpResponse(
                json.dumps({"error": "Request body is required"}),
                status_code=400,
                mimetype="application/json"
            )
        
        # Extract query parameter
        query = req_body.get("query")
        if not query:
            return func.HttpResponse(
                json.dumps({"error": "Query parameter is required"}),
                status_code=400,
                mimetype="application/json"
            )
        
        # Generate SQL
        SQL_GENERATOR_SYSTEM_PROMPT = requests.get("http://localhost:7071/api/get_sql_generator_prompt").json().get("prompt", "")
        
        messages = [
            {"role": "system", "content": SQL_GENERATOR_SYSTEM_PROMPT},
            {"role": "user", "content": query}
        ]

        try:
            response = sql_client.chat.completions.create(
                model=os.getenv("SQL_GEN_DEPLOYMENT_NAME"),
                messages=messages,
                max_completion_tokens=1000
            )
            
            if response.choices and len(response.choices) > 0:
                sql = response.choices[0].message.content
                logging.warning(f"[DEBUG] Generated SQL: {sql}")
                
                # Return successful response
                return func.HttpResponse(
                    json.dumps({
                        "sql": sql,
                        "query": query,
                        "status": "success"
                    }),
                    status_code=200,
                    mimetype="application/json"
                )
            else:
                logging.error("Error: No response from OpenAI")
                return func.HttpResponse(
                    json.dumps({"error": "No response from OpenAI"}),
                    status_code=500,
                    mimetype="application/json"
                )
                
        except Exception as e:
            logging.error(f"[ERROR] OpenAI API call failed: {e}")
            return func.HttpResponse(
                json.dumps({"error": f"OpenAI API call failed: {str(e)}"}),
                status_code=500,
                mimetype="application/json"
            )
        
    except Exception as e:
        logging.error(f"[ERROR] HTTP function failed: {e}")
        return func.HttpResponse(
            json.dumps({"error": f"Request processing failed: {str(e)}"}),
            status_code=500,
            mimetype="application/json"
        )

@func_app.route(route="get_snowflake_data", methods=["GET"], auth_level=func.AuthLevel.FUNCTION)
def get_snowflake_data(req: func.HttpRequest) -> func.HttpResponse:
    try:
        req_body = req.get_json()
        if not req_body:
            return func.HttpResponse(
                json.dumps({"error": "Request body is required"}),
                status_code=400,
                mimetype="application/json"
            )
            
        sql = req_body.get("sql")
        if not sql:
            return func.HttpResponse(
                json.dumps({"error": "SQL parameter is required"}),
                status_code=400,
                mimetype="application/json"
            )

        global connection_manager

        with connection_manager.get_connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(sql)
                # Fetch all rows and get column names
                rows = cur.fetchall()
                cols = [col[0] for col in cur.description]
                # Convert to DataFrame
                df = pd.DataFrame(rows, columns=cols)

                df_json = df.to_dict(orient="records")

                return func.HttpResponse(
                    json.dumps({"df": df_json}),
                    status_code=200,
                    mimetype="application/json"
                )
            except Exception as e:
                print(f"[ERROR] SQL execution failed: {e}")
                return func.HttpResponse(
                    json.dumps({"error": f"SQL execution failed: {str(e)}"}),
                    status_code=500,
                    mimetype="application/json"
                )
            finally:
                cur.close()
                
    except Exception as e:
        print(f"[ERROR] Connection or general error: {e}")
        return func.HttpResponse(
            json.dumps({"error": f"Connection failed: {str(e)}"}),
            status_code=500,
            mimetype="application/json"
        )