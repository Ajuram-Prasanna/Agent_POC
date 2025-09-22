import os
from dotenv import load_dotenv
import time
import snowflake.connector
import threading
from contextlib import contextmanager
import time
import threading
import yaml
from azure.cosmos import CosmosClient
from jinja2 import Template
from uuid import uuid1

load_dotenv()

COSMOS_DB_NAME = os.getenv('COSMOS_DB_NAME')
COSMOS_ENDPOINT = os.getenv('COSMOS_ENDPOINT')
COSMOS_KEY = os.getenv('COSMOS_KEY')

cosmos_client = CosmosClient(COSMOS_ENDPOINT, COSMOS_KEY)

cosmos_database = cosmos_client.get_database_client(COSMOS_DB_NAME)

COSMOS_CONTAINERS = ['configs', 'utils']

container_clients = dict()

for container_name in COSMOS_CONTAINERS:
    container_clients[container_name] = cosmos_database.get_container_client(container_name)

def get_from_cosmos(container_name = '', query = ''):
    return list(container_clients[container_name].query_items(
        query=query,
        enable_cross_partition_query=True
    ))

def upload_to_cosmos(container_name, item): 
    container = container_clients[container_name]
    
    saved_item = container.upsert_item(item)
    return saved_item

def update_cosmos(container_name = '', query = '', item = {}):

    existing_items = get_from_cosmos(container_name, query)
    if not existing_items:
        print(f"No items found in Cosmos DB for query: {query}")
        return
    
    for existing_item in existing_items:
    # Update the existing item with the new data
        for key, value in item.items():
            existing_item[key] = value

        # Upsert the updated item back to Cosmos DB
        upload_to_cosmos(container_name, existing_item)

DATABASE_NAME = get_from_cosmos('configs', f'SELECT VALUE c.config_content FROM c WHERE c.config_type = "database"')[0]
SCHEMA_NAME = get_from_cosmos('configs', f'SELECT VALUE c.config_content FROM c WHERE c.config_type = "schema"')[0]
TABLE_NAME = get_from_cosmos('configs', f'SELECT VALUE c.config_content FROM c WHERE c.config_type = "table"')[0]

config = dict()

config['database'] = dict()
config['database']['name'] = DATABASE_NAME
config['database']['schema'] = SCHEMA_NAME
config['database']['table'] = TABLE_NAME
config['general_instructions'] = get_from_cosmos('configs', 'SELECT VALUE c.config_content FROM c WHERE c.config_type = "general_instruction"')
config['important_instructions'] = get_from_cosmos('configs', 'SELECT VALUE c.config_content FROM c WHERE c.config_type = "important_instruction"')
config['rules'] = get_from_cosmos('configs', 'SELECT VALUE c.config_content FROM c WHERE c.config_type = "rule"')
config['restrictions'] = get_from_cosmos('configs', 'SELECT VALUE c.config_content FROM c WHERE c.config_type = "restriction"')
config['examples'] = get_from_cosmos('configs', 'SELECT VALUE c.config_content FROM c WHERE c.config_type = "example"')

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
            database=DATABASE_NAME,
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



def map_snowflake_type_to_generic(snowflake_type: str) -> str:
    t = snowflake_type.upper()
    if t in ("TEXT", "VARCHAR", "CHAR", "STRING"):
        return "string"
    elif t in ("NUMBER", "INT", "INTEGER", "BIGINT", "DECIMAL", "NUMERIC", "FLOAT", "DOUBLE"):
        return "number"
    elif t == "DATE":
        return "date"
    elif t in ("TIMESTAMP", "TIMESTAMP_LTZ", "TIMESTAMP_NTZ", "TIMESTAMP_TZ", "DATETIME"):
        return "datetime"
    elif t == "BOOLEAN":
        return "boolean"
    else:
        return "string"  # fallback


def generate_query_patterns(fields):
    """
    Auto-generate aggregation SQL examples from fields metadata.
    Only include queries if suitable fields exist.
    """
    numeric_fields = []
    categorical_fields = []

    for f in fields:
        # numeric check: all examples are numbers
        if all(str(x).replace(".", "", 1).isdigit() for x in f.get("examples", [])):
            numeric_fields.append(f["name"])
        else:
            # categorical if few unique values (good for GROUP BY / filtering)
            if len(f.get("examples", [])) <= 10:
                categorical_fields.append(f["name"])

    query_patterns = {}

    # always valid
    query_patterns["count_all"] = "SELECT COUNT(*) FROM {FULL_TABLE}"

    # count group requires a categorical field
    if categorical_fields:
        cat_field = categorical_fields[0]
        query_patterns["count_group"] = (
            f"SELECT {cat_field}, COUNT(*) as {cat_field}_COUNT\n"
            f"FROM {{FULL_TABLE}}\nGROUP BY {cat_field}"
        )

    # numeric-based queries
    if numeric_fields:
        num_field = numeric_fields[0]

        # group by sum requires both
        if categorical_fields:
            cat_field = categorical_fields[0]
            query_patterns["sum_group"] = (
                f"SELECT {cat_field}, SUM({num_field}) as TOTAL_AMOUNT\n"
                f"FROM {{FULL_TABLE}}\nGROUP BY {cat_field}"
            )

            # filter value from examples
            filter_val = next(
                (ex for f in fields if f["name"] == cat_field for ex in f.get("examples", [])),
                None
            )
            if filter_val:
                query_patterns["simple_sum"] = (
                    f"SELECT SUM({num_field}) as TOTAL_AMOUNT\n"
                    f"FROM {{FULL_TABLE}}\nWHERE {cat_field} = '{filter_val}'"
                )
                query_patterns["average"] = (
                    f"SELECT AVG({num_field}) as AVERAGE_AMOUNT\n"
                    f"FROM {{FULL_TABLE}}\nWHERE {cat_field} = '{filter_val}'"
                )

        # always valid with numeric field
        query_patterns["max"] = f"SELECT MAX({num_field}) as MAX_AMOUNT FROM {{FULL_TABLE}}"
        query_patterns["min"] = f"SELECT MIN({num_field}) as MIN_AMOUNT FROM {{FULL_TABLE}}"

    return query_patterns




def construct_sql_generator_prompt(config):

    # Build full table reference
    config["FULL_TABLE"] = f"{config['database']['name']}.{config['database']['schema']}.{config['database']['table']}"

    # Replace placeholders in examples/query_patterns with FULL_TABLE
    for ex in config.get("examples", []):
        ex["query"] = ex["query"].replace("{FULL_TABLE}", config["FULL_TABLE"])

    for k, v in config.get("query_patterns", {}).items():
        config["query_patterns"][k] = v.replace("{FULL_TABLE}", config["FULL_TABLE"])

    for i, r in enumerate(config.get("restrictions", [])):
        config["restrictions"][i] = r.replace("{FULL_TABLE}", config["FULL_TABLE"])

    for i, r in enumerate(config.get("rules", [])):
        config["rules"][i] = r.replace("{FULL_TABLE}", config["FULL_TABLE"])

    # Handle fields (schemas + examples + unique values)
    for field in config.get("fields", []):
        # Replace placeholder if you want table reference in descriptions/examples
        if "examples" in field and isinstance(field["examples"], list):
            field["examples"] = [ex.replace("{FULL_TABLE}", config["FULL_TABLE"]) if isinstance(ex, str) else ex for ex in field["examples"]]

        if "allowed_values" in field and isinstance(field["allowed_values"], list):
            field["allowed_values"] = [val.replace("{FULL_TABLE}", config["FULL_TABLE"]) if isinstance(val, str) else val for val in field["allowed_values"]]

    # Load template
    
    template = Template(get_from_cosmos('utils', 'SELECT VALUE c.util_content FROM c WHERE c.util_type = "sql_generator_prompt_template"')[0])
    

    # Render final prompt
    SQL_GENERATOR_SYSTEM_PROMPT = template.render(**config)

    return SQL_GENERATOR_SYSTEM_PROMPT











connection_manager = EnhancedSnowflakeConnectionManager(pool_size = 3)

with connection_manager.get_connection() as conn:
    cursor = conn.cursor()

cursor.execute(f"""
    SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = '{SCHEMA_NAME}'
      AND TABLE_NAME = '{TABLE_NAME}';
""")

field_records = cursor.fetchall()


fields = []
for field_name, snowflake_type, *_ in field_records:
    generic_type = map_snowflake_type_to_generic(snowflake_type)


    # Get count of unique values
    cursor.execute(f"""
        SELECT COUNT(DISTINCT "{field_name}") 
        FROM {SCHEMA_NAME}.{TABLE_NAME}
    """)
    distinct_count = cursor.fetchone()[0]

    examples = []
    if distinct_count <= 10:
        # Fetch all unique values
        cursor.execute(f"""
            SELECT DISTINCT "{field_name}" 
            FROM {SCHEMA_NAME}.{TABLE_NAME}
            WHERE "{field_name}" IS NOT NULL
            LIMIT 10
        """)
        examples = [row[0] for row in cursor.fetchall()]
    else:
        # Fetch a few sample values
        cursor.execute(f"""
            SELECT DISTINCT "{field_name}" 
            FROM {SCHEMA_NAME}.{TABLE_NAME}
            WHERE "{field_name}" IS NOT NULL
            LIMIT 5
        """)
        examples = [row[0] for row in cursor.fetchall()]

    field = {"name": field_name, "type": generic_type, "examples": examples}
    fields.append(field)

config['fields'] = fields

query_patterns = generate_query_patterns(fields)
config["query_patterns"] = query_patterns

# with open("utils/config.yaml", "w") as f:
#     yaml.dump(config, f, sort_keys=False, indent=2)

SQL_GENERATOR_SYSTEM_PROMPT = construct_sql_generator_prompt(config = config)


if get_from_cosmos('utils', 'SELECT * FROM c WHERE c.util_type = "sql_generator_prompt"'):
    update_cosmos('utils', 'SELECT * FROM c WHERE c.util_type = "sql_generator_prompt"', {
        'util_content': SQL_GENERATOR_SYSTEM_PROMPT
    })
else:
    upload_to_cosmos('utils', {
        'id': str(uuid1()),
        'util_type': 'sql_generator_prompt',
        'util_content': SQL_GENERATOR_SYSTEM_PROMPT
    })
