import os
from dotenv import load_dotenv
from azure.cosmos import CosmosClient

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


SQL_GENERATOR_SYSTEM_PROMPT = get_from_cosmos('utils', 'SELECT VALUE c.util_content FROM c WHERE c.util_type = "sql_generator_prompt"')

if not SQL_GENERATOR_SYSTEM_PROMPT:
    import config_and_utils_generator

SQL_GENERATOR_SYSTEM_PROMPT = get_from_cosmos('utils', 'SELECT VALUE c.util_content FROM c WHERE c.util_type = "sql_generator_prompt"')

SQL_GENERATOR_SYSTEM_PROMPT = SQL_GENERATOR_SYSTEM_PROMPT[0]

# with open("final_prompt.txt", 'w') as f:
#     f.write(SQL_GENERATOR_SYSTEM_PROMPT)

# Updated System Prompt for Loan Data Analysis
SYSTEM_PROMPT = get_from_cosmos('utils', 'SELECT VALUE c.util_content FROM c WHERE c.util_type = "convo_agent_prompt"')[0]


# Tool Schema Definition
TOOL_SCHEMAS = [
    {
        "type": "function",
        "function": {
            "name": "process_user_query",
            "description": "Generate Snowflake SQL from a natural-language request about loan data, execute it, and return the result. Use this tool whenever the user asks about loans, agents, loan amounts, dates, statistics, or any data-related questions.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The user's natural language query about loan and agent data that needs to be processed into a database query.",
                    }
                },
                "required": ["query"],
            },
        },
    }
]